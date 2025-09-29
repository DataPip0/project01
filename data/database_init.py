# db_bootstrap.py
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional
import os

from sqlalchemy import (
    create_engine, String, Integer, Text, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.sql import func

# ---------- ORM models (4 tables) ----------

class Base(DeclarativeBase):
    pass

# AHMED: Go through and ensure all the tables are correctly defined in line with the specif

class Journey(Base):
    __tablename__ = "journey"
    journey_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_ref: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    product: Mapped[str] = mapped_column(String(50))
    channel: Mapped[str] = mapped_column(String(30))
    status: Mapped[str] = mapped_column(String(30), index=True)           # e.g., Open/In-Progress/Completed
    start_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    end_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # relationships
    events: Mapped[list["Event"]] = relationship(back_populates="journey", cascade="all, delete-orphan")

class SubProcess(Base):
    __tablename__ = "sub_process"
    sub_process_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(30), index=True)           # e.g., Open/In-Progress/Completed
    start_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    end_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    steps: Mapped[list["Steps"]] = relationship(back_populates="sub_process", cascade="all, delete-orphan")

class Steps(Base):
    __tablename__ = "steps"
    step_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sub_process_id: Mapped[int] = mapped_column(ForeignKey("sub_process.sub_process_id", ondelete="RESTRICT"), index=True)
    journey_id: Mapped[int] = mapped_column(ForeignKey("journey.jorney_id", ondelete="RESTRICT"), index=True)
    name: Mapped[str] = mapped_column(String(60))
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(30), index=True)           # e.g., Open/In-Progress/Completed
    start_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    end_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    sub_process: Mapped[SubProcess] = relationship(back_populates="steps")

    __table_args__ = (
        Index("uq_stage_per_subprocess", "sub_process_id", "name", unique=True),
    )

class Event(Base):
    __tablename__ = "event"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(ForeignKey("application.id", ondelete="CASCADE"), index=True)
    stage_id: Mapped[Optional[int]] = mapped_column(ForeignKey("stage.id", ondelete="SET NULL"), nullable=True)
    event_type: Mapped[str] = mapped_column(String(30))                    # RECEIVED/STARTED/COMPLETED/REJECTED/etc.
    detail: Mapped[Optional[str]] = mapped_column(Text)
    event_ts: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    status: Mapped[str] = mapped_column(String(30), index=True)           # e.g., Open/In-Progress/Completed
    start_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    end_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    application: Mapped[Journey] = relationship(back_populates="events")

# ---------- DB manager ----------

@dataclass
class _DBContext:
    engine: any
    SessionLocal: sessionmaker

_ctx: Optional[_DBContext] = None  # module-level singleton

def init_db(url: str = "sqlite:///data/app.db", echo: bool = False) -> None:
    """
    Initialize the database (create file/tables if missing) and cache
    a global Session factory for the rest of the app to use.
    Call this ONCE at app start / workflow init.
    """
    global _ctx

    # Ensure directory for SQLite
    if url.startswith("sqlite:///"):
        db_path = url.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    engine = create_engine(url, echo=echo, future=True)

    # Create tables idempotently
    Base.metadata.create_all(engine)

    # PRAGMA foreign_keys for SQLite
    if url.startswith("sqlite"):
        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA foreign_keys=ON;")

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    _ctx = _DBContext(engine=engine, SessionLocal=SessionLocal)

def get_engine():
    assert _ctx is not None, "init_db() must be called first"
    return _ctx.engine

def get_session() -> sessionmaker:
    """Return the session factory so callers can do `with session() as s:`."""
    assert _ctx is not None, "init_db() must be called first"
    return _ctx.SessionLocal

@contextmanager
def session_scope() -> Generator:
    """
    Handy context manager:
        with session_scope() as s:
            ...
    """
    SessionLocal = get_session()
    s = SessionLocal()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()
