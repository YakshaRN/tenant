from datetime import datetime

from sqlalchemy import (
    Column, Integer, Float, String, Boolean, Date, DateTime,
    Text, ForeignKey, UniqueConstraint,
)
from sqlalchemy.orm import relationship

from backend.database import Base


class Facility(Base):
    __tablename__ = "facilities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    owner = Column(String)
    name = Column(String)
    building = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    spaces = relationship("Space", back_populates="facility", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("job_id", "owner", "name", "building", name="uq_facility_natural"),
    )


class Space(Base):
    __tablename__ = "spaces"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True)
    space = Column(String)
    width = Column(Float)
    length = Column(Float)
    height = Column(Float)
    rate = Column(Float)
    web_rate = Column(Float)
    space_size = Column(String)
    space_category = Column(String)
    space_type = Column(String)
    door_width = Column(Float)
    door_height = Column(Float)
    amenities = Column(Text)
    sq_ft = Column(Float)
    floor = Column(Float)
    alarm_enabled = Column(Boolean)
    access_24_hour = Column(Boolean)
    offline = Column(Boolean)
    offline_reason = Column(String)
    catch_flag = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    facility = relationship("Facility", back_populates="spaces")
    leases = relationship("Lease", back_populates="space", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("job_id", "space", name="uq_space_natural"),
    )


class Tenant(Base):
    __tablename__ = "tenants"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    account_code = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    middle_name = Column(String)
    address = Column(Text)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    country = Column(String)
    email = Column(String)
    cell_phone = Column(String)
    home_phone = Column(String)
    work_phone = Column(String)
    access_code = Column(String)
    dob = Column(Date)
    gender = Column(String)
    active_military = Column(Boolean)
    dl_id = Column(String)
    dl_state = Column(String)
    dl_city = Column(String)
    dl_exp_date = Column(Date)
    created_at = Column(DateTime, default=datetime.utcnow)

    alternate_contact = relationship(
        "AlternateContact", back_populates="tenant", uselist=False, cascade="all, delete-orphan"
    )
    military_detail = relationship(
        "MilitaryDetail", back_populates="tenant", uselist=False, cascade="all, delete-orphan"
    )
    leases = relationship("Lease", back_populates="tenant", cascade="all, delete-orphan")


class AlternateContact(Base):
    __tablename__ = "alternate_contacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, unique=True)
    first_name = Column(String)
    last_name = Column(String)
    middle_name = Column(String)
    address = Column(Text)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    email = Column(String)
    home_phone = Column(String)
    work_phone = Column(String)
    cell_phone = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="alternate_contact")


class Lease(Base):
    __tablename__ = "leases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=True)
    space_id = Column(Integer, ForeignKey("spaces.id"), nullable=True)
    rent = Column(Float)
    move_in_date = Column(Date)
    move_out_date = Column(Date)
    start_date = Column(Date)
    end_date = Column(Date)
    last_rent_change_date = Column(Date)
    paid_date = Column(Date)
    paid_through_date = Column(Date)
    pay_by_date = Column(Date)
    bill_day = Column(Integer)
    payment_cycle = Column(String)
    is_business_lease = Column(Boolean)
    paperless_billing = Column(Boolean)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="leases")
    space = relationship("Space", back_populates="leases")
    financial_balance = relationship(
        "FinancialBalance", back_populates="lease", uselist=False, cascade="all, delete-orphan"
    )
    insurance_coverage = relationship(
        "InsuranceCoverage", back_populates="lease", uselist=False, cascade="all, delete-orphan"
    )
    lien = relationship(
        "Lien", back_populates="lease", uselist=False, cascade="all, delete-orphan"
    )
    promotions = relationship("Promotion", back_populates="lease", cascade="all, delete-orphan")
    discounts = relationship("Discount", back_populates="lease", cascade="all, delete-orphan")


class FinancialBalance(Base):
    __tablename__ = "financial_balances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lease_id = Column(Integer, ForeignKey("leases.id"), nullable=False, unique=True)
    security_deposit = Column(Float)
    security_deposit_balance = Column(Float)
    rent_balance = Column(Float)
    fees_balance = Column(Float)
    protection_insurance_balance = Column(Float)
    merchandise_balance = Column(Float)
    late_fees_balance = Column(Float)
    lien_fees_balance = Column(Float)
    tax_balance = Column(Float)
    prepaid_rent = Column(Float)
    prepaid_additional_rent_premium = Column(Float)
    prepaid_tax = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    lease = relationship("Lease", back_populates="financial_balance")


class InsuranceCoverage(Base):
    __tablename__ = "insurance_coverages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lease_id = Column(Integer, ForeignKey("leases.id"), nullable=False, unique=True)
    provider = Column(String)
    coverage = Column(Float)
    additional_rent_premium = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    lease = relationship("Lease", back_populates="insurance_coverage")


class Lien(Base):
    __tablename__ = "liens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lease_id = Column(Integer, ForeignKey("leases.id"), nullable=False, unique=True)
    delinquency_status = Column(String)
    lien_status = Column(String)
    lien_posted_date = Column(Date)
    holder_first_name = Column(String)
    holder_last_name = Column(String)
    holder_email = Column(String)
    holder_phone = Column(String)
    holder_address_1 = Column(Text)
    holder_address_2 = Column(Text)
    holder_city = Column(String)
    holder_state = Column(String)
    holder_zipcode = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    lease = relationship("Lease", back_populates="lien")


class Promotion(Base):
    __tablename__ = "promotions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lease_id = Column(Integer, ForeignKey("leases.id"), nullable=False)
    promotion = Column(String)
    promotion_type = Column(String)
    promotion_value = Column(Float)
    promotion_start = Column(Date)
    promotion_length = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    lease = relationship("Lease", back_populates="promotions")


class Discount(Base):
    __tablename__ = "discounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lease_id = Column(Integer, ForeignKey("leases.id"), nullable=False)
    discount = Column(String)
    discount_type = Column(String)
    discount_value = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    lease = relationship("Lease", back_populates="discounts")


class MilitaryDetail(Base):
    __tablename__ = "military_details"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, unique=True)
    commanding_officer_first_name = Column(String)
    commanding_officer_last_name = Column(String)
    commanding_officer_phone = Column(String)
    commanding_officer_email = Column(String)
    rank = Column(String)
    military_serial_number = Column(String)
    military_email = Column(String)
    service_member_dob = Column(Date)
    expiration_term_of_service = Column(Date)
    military_branch = Column(String)
    unit_name = Column(String)
    unit_phone = Column(String)
    unit_address_1 = Column(Text)
    unit_address_2 = Column(Text)
    city = Column(String)
    unit_state = Column(String)
    unit_zipcode = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="military_detail")
