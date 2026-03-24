import re
import logging
from datetime import datetime

import pandas as pd
from pandas.errors import OutOfBoundsDatetime

logger = logging.getLogger("tagger")

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
PHONE_REGEX = re.compile(r"^[\d\s\(\)\-\+\.]{7,20}$")
ZIP_REGEX = re.compile(r"^\d{5}(-\d{4})?$")
STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

# Columns where a far-future end date means "open-ended / no end" (PMS exports).
_OPEN_ENDED_DATE_COLS = frozenset({"end_date", "Move Out Date"})


def _normalize_zip_for_validation(v):
    """
    Excel/CSV often loads ZIP as float (92308.0). Normalize to a digit string
    suitable for ZIP_REGEX / length checks.
    """
    if pd.isna(v):
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        f = float(v)
        if not f.is_integer():
            return str(v).strip()
        n = int(abs(f))
        s = str(n)
        if len(s) <= 5:
            s = s.zfill(5)
        return s
    s = str(v).strip()
    m = re.match(r"^(\d+)\.0$", s)
    if m:
        return m.group(1).zfill(5)
    return s


def _is_open_ended_date_value(v, col):
    """True if v is a common 'no end date' sentinel (valid, not an error)."""
    if col not in _OPEN_ENDED_DATE_COLS:
        return False
    if hasattr(v, "year") and getattr(v, "year", None) == 9999:
        return True
    s = str(v).strip()
    return "9999-12-31" in s


def _parse_date_for_tagging(v, col):
    """
    Parse a cell for date checks. Returns pd.Timestamp, or None if the value is
    a valid open-ended sentinel (skip year-range checks). Raises ValueError if unparseable.
    """
    if _is_open_ended_date_value(v, col):
        return None
    try:
        return pd.to_datetime(v)
    except OutOfBoundsDatetime:
        if _is_open_ended_date_value(v, col):
            return None
        raise ValueError from None
    except Exception:
        s = str(v).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            for sl in (s, s[:19]):
                try:
                    return pd.Timestamp(datetime.strptime(sl, fmt))
                except Exception:
                    continue
        raise ValueError from None


def detect(df):
    """
    Anomaly detection on staged data.
    Returns list of dicts with row, field, value, category, and reason.
    """
    anomalies = []

    def _add(row, field, value, category, reason):
        anomalies.append({
            "row": int(row),
            "field": field,
            "value": str(value) if pd.notna(value) else None,
            "category": category,
            "reason": reason,
        })

    # --- RENT / RATE CHECKS ---
    for col in ["Rent", "Rate", "Web Rate"]:
        if col not in df.columns:
            continue
        for i, v in df[col].items():
            if pd.isna(v):
                continue
            try:
                num = float(v)
                if num < 0:
                    _add(i, col, v, "negative_value", f"{col} is negative (${num:.2f})")
                elif col == "Rent" and num > 10000:
                    _add(i, col, v, "outlier", f"{col} unusually high (${num:.2f}), verify if correct")
            except (ValueError, TypeError):
                _add(i, col, v, "invalid_type", f"{col} is not a valid number: '{v}'")

    # --- EMAIL CHECKS ---
    if "Email" in df.columns:
        for i, v in df["Email"].items():
            if pd.isna(v) or str(v).strip() == "":
                continue
            email = str(v).strip()
            if not EMAIL_REGEX.match(email):
                _add(i, "Email", v, "invalid_email", f"Invalid email format: '{email}'")

    # --- PHONE CHECKS ---
    for col in ["Cell Phone", "Home Phone", "Work Phone"]:
        if col not in df.columns:
            continue
        for i, v in df[col].items():
            if pd.isna(v) or str(v).strip() == "":
                continue
            phone = str(v).strip()
            digits = re.sub(r"\D", "", phone)
            if len(digits) < 7:
                _add(i, col, v, "invalid_phone", f"{col} has too few digits ({len(digits)}): '{phone}'")
            elif len(digits) > 15:
                _add(i, col, v, "invalid_phone", f"{col} has too many digits ({len(digits)}): '{phone}'")

    # --- ZIP CODE CHECK ---
    if "ZIP" in df.columns:
        for i, v in df["ZIP"].items():
            if pd.isna(v) or str(v).strip() == "":
                continue
            z = _normalize_zip_for_validation(v)
            if z is None:
                continue
            if not ZIP_REGEX.match(z):
                digits = re.sub(r"\D", "", z)
                if len(digits) not in (5, 9):
                    _add(i, "ZIP", v, "invalid_zip", f"Invalid ZIP code format: '{z}'")

    # --- STATE CODE CHECK ---
    if "State" in df.columns:
        for i, v in df["State"].items():
            if pd.isna(v) or str(v).strip() == "":
                continue
            s = str(v).strip().upper()
            if len(s) == 2 and s not in STATE_CODES:
                _add(i, "State", v, "invalid_state", f"Unknown state code: '{v}'")

    # --- DATE CHECKS ---
    for col in ["Move In Date", "Move Out Date", "DOB", "DL Exp Date",
                 "Paid Through Date", "Last Rent Change Date", "start_date", "end_date"]:
        if col not in df.columns:
            continue
        for i, v in df[col].items():
            if pd.isna(v) or str(v).strip() == "":
                continue
            try:
                parsed = _parse_date_for_tagging(v, col)
            except (ValueError, TypeError):
                _add(i, col, v, "invalid_date", f"Cannot parse as date: '{v}'")
                continue
            if parsed is None:
                continue
            if col == "DOB" and parsed.year > 2020:
                _add(i, col, v, "suspicious_date", f"Date of birth is in the future or too recent: {v}")
            if col == "DOB" and parsed.year < 1900:
                _add(i, col, v, "suspicious_date", f"Date of birth before 1900: {v}")
            if col == "Move In Date" and parsed.year < 2000:
                _add(i, col, v, "suspicious_date", f"Move-in date before year 2000: {v}")

    # --- NEGATIVE BALANCE CHECKS ---
    for col in ["Rent Balance", "Fees Balance", "Tax Balance", "Late Fees Balance",
                 "Security Deposit", "Security Deposit Balance"]:
        if col not in df.columns:
            continue
        for i, v in df[col].items():
            if pd.isna(v):
                continue
            try:
                num = float(v)
                if num < 0:
                    _add(i, col, v, "negative_value", f"{col} is negative (${num:.2f})")
            except (ValueError, TypeError):
                pass

    # --- DIMENSION CHECKS ---
    for col in ["Width", "Length", "Height", "Sq. Ft."]:
        if col not in df.columns:
            continue
        for i, v in df[col].items():
            if pd.isna(v):
                continue
            try:
                num = float(v)
                if num <= 0:
                    _add(i, col, v, "invalid_dimension", f"{col} must be positive, got {num}")
                elif col == "Sq. Ft." and num > 5000:
                    _add(i, col, v, "outlier", f"{col} unusually large ({num} sq ft), verify if correct")
            except (ValueError, TypeError):
                pass

    # --- ACCESS CODE CHECK ---
    if "Access Code" in df.columns:
        for i, v in df["Access Code"].items():
            if pd.isna(v) or str(v).strip() == "":
                continue
            code = str(v).strip()
            if len(code) < 3:
                _add(i, "Access Code", v, "suspicious_value", f"Access code too short ({len(code)} chars): '{code}'")

    # --- CONFIDENCE CHECK (for duplicate detection) ---
    if "Account Code" in df.columns:
        acct = df["Account Code"].dropna()
        dupes = acct[acct.duplicated(keep=False)]
        if not dupes.empty and "Space" in df.columns:
            groups = df.loc[dupes.index].groupby("Account Code")
            for acct_code, group in groups:
                if len(group) > 1:
                    spaces = group["Space"].dropna().tolist()
                    for idx in group.index:
                        _add(idx, "Account Code", acct_code, "info",
                             f"Account '{acct_code}' has {len(group)} units ({', '.join(str(s) for s in spaces)}) — same tenant, multiple leases")

    logger.info(f"Tagging complete: {len(anomalies)} anomalies found")
    return anomalies
