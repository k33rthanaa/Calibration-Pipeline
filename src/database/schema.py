"""SQLAlchemy ORM models and DB init helpers."""

import logging
from datetime import datetime

from sqlalchemy import (
    Column, DateTime, Float, ForeignKey, Integer, String, Text,
    create_engine, event
)
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class CalibrationRun(Base):
    """Metadata for each calibration run."""
    __tablename__ = "calibration_runs"

    run_id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    operator = Column(String(100))
    equipment_id = Column(String(100), nullable=False)
    source_file = Column(String(500))
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    measurements = relationship("CalibrationMeasurement", back_populates="run", cascade="all, delete-orphan")
    validation_results = relationship("ValidationResult", back_populates="run", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<CalibrationRun(run_id={self.run_id}, equipment={self.equipment_id}, ts={self.timestamp})>"


class CalibrationMeasurement(Base):
    """Individual position measurements within a calibration run."""
    __tablename__ = "calibration_measurements"

    measurement_id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("calibration_runs.run_id"), nullable=False)
    position = Column(String(100), nullable=False)
    actual_value = Column(Float, nullable=False)
    expected_value = Column(Float, nullable=False)
    deviation = Column(Float, nullable=False)
    timestamp = Column(DateTime)
    axis = Column(String(10))        # X, Y, Z, or combined
    unit = Column(String(20))        # mm, um, nm, etc.

    run = relationship("CalibrationRun", back_populates="measurements")

    def __repr__(self):
        return f"<Measurement(pos={self.position}, dev={self.deviation:.4f})>"


class FileImport(Base):
    """Audit trail of all imported files."""
    __tablename__ = "file_imports"

    file_id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(500), nullable=False)
    file_hash = Column(String(64))           # SHA-256 for dedup detection
    import_timestamp = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), nullable=False)   # success, failed, skipped
    rows_imported = Column(Integer, default=0)
    error_message = Column(Text)


class ValidationResult(Base):
    """Outcomes of validation checks per run."""
    __tablename__ = "validation_results"

    validation_id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("calibration_runs.run_id"), nullable=False)
    check_type = Column(String(100), nullable=False)   # timestamp, duplicate, outlier, etc.
    status = Column(String(20), nullable=False)         # pass, warn, fail
    details = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    run = relationship("CalibrationRun", back_populates="validation_results")


def get_engine(db_path: str):
    """Create SQLAlchemy engine with WAL mode for better concurrency."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def init_db(db_path: str):
    """Initialize the database, creating tables if they don't exist."""
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    logger.info("Database initialized at %s", db_path)
    return engine


def get_session(engine):
    """Return a new SQLAlchemy session."""
    Session = sessionmaker(bind=engine)
    return Session()
