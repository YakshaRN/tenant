import os
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

logger = logging.getLogger("database")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "onboarding.db")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from backend.models import (  # noqa: F401 – ensure models are registered
        Facility, Space, Tenant, AlternateContact, Lease,
        FinancialBalance, InsuranceCoverage, Lien,
        Promotion, Discount, MilitaryDetail,
    )
    Base.metadata.create_all(bind=engine)
    logger.info(f"Database initialized at {DATABASE_URL}")
