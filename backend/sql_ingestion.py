import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import (
    Facility, Space, Tenant, AlternateContact, Lease,
    FinancialBalance, InsuranceCoverage, Lien,
    Promotion, Discount, MilitaryDetail,
)

logger = logging.getLogger("sql_ingestion")

HERO_TO_FACILITY = {
    "owner": "Owner",
    "name": "Name",
    "building": "Building",
}

HERO_TO_SPACE = {
    "space": "Space",
    "width": "Width",
    "length": "Length",
    "height": "Height",
    "rate": "Rate",
    "web_rate": "Web Rate",
    "space_size": "Space Size",
    "space_category": "Space Category",
    "space_type": "Space Type",
    "door_width": "Door Width",
    "door_height": "Door Height",
    "amenities": "Amenities",
    "sq_ft": "Sq. Ft.",
    "floor": "Floor",
    "alarm_enabled": "Alarm Enabled",
    "access_24_hour": "24-hour access",
    "offline": "Offline",
    "offline_reason": "OfflineReason",
    "catch_flag": "Catch Flag",
}

HERO_TO_TENANT = {
    "first_name": "First Name",
    "last_name": "Last Name",
    "middle_name": "Middle Name",
    "account_code": "Account Code",
    "address": "Address",
    "city": "City",
    "state": "State",
    "zip": "ZIP",
    "country": "Country",
    "email": "Email",
    "cell_phone": "Cell Phone",
    "home_phone": "Home Phone",
    "work_phone": "Work Phone",
    "access_code": "Access Code",
    "dob": "DOB",
    "gender": "Gender",
    "active_military": "Active Military",
    "dl_id": "DL Id",
    "dl_state": "DL State",
    "dl_city": "DL City",
    "dl_exp_date": "DL Exp Date",
}

HERO_TO_ALT_CONTACT = {
    "first_name": "Alt First Name",
    "last_name": "Alt Last Name",
    "middle_name": "Alt Middle Name",
    "address": "Alt Address",
    "city": "Alt City",
    "state": "Alt State",
    "zip": "Alt ZIP",
    "email": "Alt Email",
    "home_phone": "Alt Home Phone",
    "work_phone": "Alt Work Phone",
    "cell_phone": "Alt Cell Phone",
}

HERO_TO_LEASE = {
    "rent": "Rent",
    "move_in_date": "Move In Date",
    "move_out_date": "Move Out Date",
    "start_date": "start_date",
    "end_date": "end_date",
    "last_rent_change_date": "Last Rent Change Date",
    "paid_date": "Paid Date",
    "paid_through_date": "Paid Through Date",
    "pay_by_date": "pay_by_date",
    "bill_day": "Bill Day",
    "payment_cycle": "payment_cycle",
    "is_business_lease": "IsBusinessLease",
    "paperless_billing": "PaperlessBilling",
}

HERO_TO_FINANCIAL = {
    "security_deposit": "Security Deposit",
    "security_deposit_balance": "Security Deposit Balance",
    "rent_balance": "Rent Balance",
    "fees_balance": "Fees Balance",
    "protection_insurance_balance": "Protection/Insurance Balance",
    "merchandise_balance": "Merchandise Balance",
    "late_fees_balance": "Late Fees Balance",
    "lien_fees_balance": "Lien Fees Balance",
    "tax_balance": "Tax Balance",
    "prepaid_rent": "Prepaid Rent",
    "prepaid_additional_rent_premium": "Prepaid Additional Rent/Premium",
    "prepaid_tax": "Prepaid Tax",
}

HERO_TO_INSURANCE = {
    "provider": "Protection/Insurance Provider",
    "coverage": "Protection/Insurance Coverage",
    "additional_rent_premium": "Additional Rent/Premium",
}

HERO_TO_LIEN = {
    "delinquency_status": "Delinquency Status",
    "lien_status": "Lien Status",
    "lien_posted_date": "Lien Posted Date",
    "holder_first_name": "Lien Holder First Name",
    "holder_last_name": "Lien Holder Last name",
    "holder_email": "Lien Holder Email",
    "holder_phone": "Lien Holder Phone",
    "holder_address_1": "Lien Holder Address 1",
    "holder_address_2": "Lien Holder Address 2",
    "holder_city": "Lien Holder City",
    "holder_state": "Lien Holder State",
    "holder_zipcode": "Lien Holder Zipcode",
}

HERO_TO_PROMOTION = {
    "promotion": "Promotion",
    "promotion_type": "Promotion Type",
    "promotion_value": "Promotion Value",
    "promotion_start": "Promotion Start",
    "promotion_length": "Promotion Length",
}

HERO_TO_DISCOUNT = {
    "discount": "Discount",
    "discount_type": "Discount Type",
    "discount_value": "Discount Value",
}

HERO_TO_MILITARY = {
    "commanding_officer_first_name": "Commanding Officer First Name",
    "commanding_officer_last_name": "Commanding Officer Last Name",
    "commanding_officer_phone": "Commanding Officer Phone",
    "commanding_officer_email": "Commanding Officer Email",
    "rank": "Rank",
    "military_serial_number": "Military Serial Number",
    "military_email": "Military Email",
    "service_member_dob": "Service Member DOB",
    "expiration_term_of_service": "Expiration Term of Service",
    "military_branch": "Military Branch",
    "unit_name": "Military Unit Name",
    "unit_phone": "Military Unit Phone",
    "unit_address_1": "Military Unit Address 1",
    "unit_address_2": "Military Unit Address 2",
    "city": "Military City",
    "unit_state": "Military Unit State",
    "unit_zipcode": "Military Unit Zipcode",
}

DATE_FIELDS = {
    "dob", "dl_exp_date",
    "move_in_date", "move_out_date", "start_date", "end_date",
    "last_rent_change_date", "paid_date", "paid_through_date", "pay_by_date",
    "lien_posted_date", "promotion_start",
    "service_member_dob", "expiration_term_of_service",
}

BOOLEAN_FIELDS = {
    "alarm_enabled", "access_24_hour", "offline",
    "active_military", "is_business_lease", "paperless_billing",
}


def _safe_val(row: pd.Series, hero_col: str):
    """Extract a value from the row, returning None for NaN/empty."""
    if hero_col not in row.index:
        return None
    val = row.get(hero_col)
    if pd.isna(val):
        return None
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def _parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _parse_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def _parse_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_int(val):
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _extract_fields(row: pd.Series, mapping: dict, coerce_types: bool = True) -> dict:
    """Pull Hero Format columns from a row using a db_field->hero_col mapping."""
    result = {}
    for db_field, hero_col in mapping.items():
        val = _safe_val(row, hero_col)
        if coerce_types:
            if db_field in DATE_FIELDS:
                val = _parse_date(val)
            elif db_field in BOOLEAN_FIELDS:
                val = _parse_bool(val)
        result[db_field] = val
    return result


def _has_any_value(data: dict) -> bool:
    """Return True if at least one value is non-None."""
    return any(v is not None for v in data.values())


def _clear_job_data(db: Session, job_id: str):
    """Remove all records for a job_id (idempotent re-run)."""
    lease_ids = [lid for (lid,) in db.query(Lease.id).filter(Lease.job_id == job_id).all()]
    if lease_ids:
        db.query(FinancialBalance).filter(FinancialBalance.lease_id.in_(lease_ids)).delete(synchronize_session=False)
        db.query(InsuranceCoverage).filter(InsuranceCoverage.lease_id.in_(lease_ids)).delete(synchronize_session=False)
        db.query(Lien).filter(Lien.lease_id.in_(lease_ids)).delete(synchronize_session=False)
        db.query(Promotion).filter(Promotion.lease_id.in_(lease_ids)).delete(synchronize_session=False)
        db.query(Discount).filter(Discount.lease_id.in_(lease_ids)).delete(synchronize_session=False)

    tenant_ids = [tid for (tid,) in db.query(Tenant.id).filter(Tenant.job_id == job_id).all()]
    if tenant_ids:
        db.query(AlternateContact).filter(AlternateContact.tenant_id.in_(tenant_ids)).delete(synchronize_session=False)
        db.query(MilitaryDetail).filter(MilitaryDetail.tenant_id.in_(tenant_ids)).delete(synchronize_session=False)

    db.query(Lease).filter(Lease.job_id == job_id).delete(synchronize_session=False)
    db.query(Tenant).filter(Tenant.job_id == job_id).delete(synchronize_session=False)
    db.query(Space).filter(Space.job_id == job_id).delete(synchronize_session=False)
    db.query(Facility).filter(Facility.job_id == job_id).delete(synchronize_session=False)
    db.commit()


def _facility_key(data: dict) -> tuple:
    return (data.get("owner") or "", data.get("name") or "", data.get("building") or "")


def _space_key(data: dict):
    return data.get("space")


def _tenant_key(data: dict) -> tuple:
    ac = data.get("account_code")
    if ac:
        return ("ac", ac)
    return ("name", data.get("first_name") or "", data.get("last_name") or "", data.get("email") or "")


def ingest_hero_to_sql(job_id: str, staged_df: pd.DataFrame) -> dict:
    """
    Parse a staged Hero Format DataFrame into normalized SQL tables.
    Returns a summary dict with counts per table.
    """
    db: Session = SessionLocal()
    try:
        _clear_job_data(db, job_id)

        facility_cache: dict[tuple, Facility] = {}
        space_cache: dict[str, Space] = {}
        tenant_cache: dict[tuple, Tenant] = {}

        counts = {
            "facilities": 0,
            "spaces": 0,
            "tenants": 0,
            "alternate_contacts": 0,
            "leases": 0,
            "financial_balances": 0,
            "insurance_coverages": 0,
            "liens": 0,
            "promotions": 0,
            "discounts": 0,
            "military_details": 0,
        }

        for idx, row in staged_df.iterrows():
            # --- Facility ---
            fac_data = _extract_fields(row, HERO_TO_FACILITY)
            fac_key = _facility_key(fac_data)
            if fac_key not in facility_cache:
                facility = Facility(job_id=job_id, **fac_data)
                db.add(facility)
                db.flush()
                facility_cache[fac_key] = facility
                counts["facilities"] += 1
            else:
                facility = facility_cache[fac_key]

            # --- Space ---
            sp_data = _extract_fields(row, HERO_TO_SPACE)
            for f in ("width", "length", "height", "rate", "web_rate", "door_width",
                       "door_height", "sq_ft", "floor"):
                sp_data[f] = _parse_float(sp_data.get(f))

            sp_key = _space_key(sp_data)
            if sp_key and sp_key not in space_cache:
                space = Space(job_id=job_id, facility_id=facility.id, **sp_data)
                db.add(space)
                db.flush()
                space_cache[sp_key] = space
                counts["spaces"] += 1
            elif sp_key:
                space = space_cache[sp_key]
            else:
                space = Space(job_id=job_id, facility_id=facility.id, **sp_data)
                db.add(space)
                db.flush()
                counts["spaces"] += 1

            # --- Tenant ---
            tn_data = _extract_fields(row, HERO_TO_TENANT)
            tn_key = _tenant_key(tn_data)
            if tn_key not in tenant_cache:
                tenant = Tenant(job_id=job_id, **tn_data)
                db.add(tenant)
                db.flush()
                tenant_cache[tn_key] = tenant
                counts["tenants"] += 1
            else:
                tenant = tenant_cache[tn_key]

            # --- Alternate Contact (1:1 with tenant, only if data exists) ---
            alt_data = _extract_fields(row, HERO_TO_ALT_CONTACT, coerce_types=False)
            if _has_any_value(alt_data) and not tenant.alternate_contact:
                alt = AlternateContact(tenant_id=tenant.id, **alt_data)
                db.add(alt)
                counts["alternate_contacts"] += 1

            # --- Military Detail (1:1 with tenant, only if data exists) ---
            mil_data = _extract_fields(row, HERO_TO_MILITARY)
            for f in ("service_member_dob", "expiration_term_of_service"):
                mil_data[f] = _parse_date(mil_data.get(f))
            if _has_any_value(mil_data) and not tenant.military_detail:
                mil = MilitaryDetail(tenant_id=tenant.id, **mil_data)
                db.add(mil)
                counts["military_details"] += 1

            # --- Lease ---
            ls_data = _extract_fields(row, HERO_TO_LEASE)
            ls_data["rent"] = _parse_float(ls_data.get("rent"))
            ls_data["bill_day"] = _parse_int(ls_data.get("bill_day"))

            lease = Lease(
                job_id=job_id,
                tenant_id=tenant.id,
                space_id=space.id,
                **ls_data,
            )
            db.add(lease)
            db.flush()
            counts["leases"] += 1

            # --- Financial Balance (1:1 with lease) ---
            fin_data = _extract_fields(row, HERO_TO_FINANCIAL, coerce_types=False)
            for k in fin_data:
                fin_data[k] = _parse_float(fin_data[k])
            if _has_any_value(fin_data):
                fb = FinancialBalance(lease_id=lease.id, **fin_data)
                db.add(fb)
                counts["financial_balances"] += 1

            # --- Insurance Coverage (1:1 with lease) ---
            ins_data = _extract_fields(row, HERO_TO_INSURANCE, coerce_types=False)
            ins_data["coverage"] = _parse_float(ins_data.get("coverage"))
            ins_data["additional_rent_premium"] = _parse_float(ins_data.get("additional_rent_premium"))
            if _has_any_value(ins_data):
                ic = InsuranceCoverage(lease_id=lease.id, **ins_data)
                db.add(ic)
                counts["insurance_coverages"] += 1

            # --- Lien (1:1 with lease) ---
            lien_data = _extract_fields(row, HERO_TO_LIEN)
            if _has_any_value(lien_data):
                ln = Lien(lease_id=lease.id, **lien_data)
                db.add(ln)
                counts["liens"] += 1

            # --- Promotion (1:N with lease) ---
            promo_data = _extract_fields(row, HERO_TO_PROMOTION)
            promo_data["promotion_value"] = _parse_float(promo_data.get("promotion_value"))
            promo_data["promotion_length"] = _parse_int(promo_data.get("promotion_length"))
            if _has_any_value(promo_data):
                pr = Promotion(lease_id=lease.id, **promo_data)
                db.add(pr)
                counts["promotions"] += 1

            # --- Discount (1:N with lease) ---
            disc_data = _extract_fields(row, HERO_TO_DISCOUNT)
            disc_data["discount_value"] = _parse_float(disc_data.get("discount_value"))
            if _has_any_value(disc_data):
                dc = Discount(lease_id=lease.id, **disc_data)
                db.add(dc)
                counts["discounts"] += 1

        db.commit()
        logger.info(f"Job {job_id}: ingested {counts}")
        return counts

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
