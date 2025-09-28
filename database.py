from typing import Optional, Dict
from enum import IntEnum

from db_tool import Base, ConflictAction, upsert_with_policy

from sqlalchemy import (
    create_engine,
    Text,
    LargeBinary,
    JSON,
    DateTime,
    func,
    UniqueConstraint,
)

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker
from datetime import datetime

def get_session(db_url: str):
    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)
    return Session()

class ReprMixin:
    # Seznam dvojic (datový typ, formátovací funkce)
    __repr_formatters__ = [
        (datetime, lambda dt: dt.strftime("%Y-%m-%d %H:%M")),  # bez sekund
        (bytes, lambda b: b.hex())  # hexadecimální výpis
    ]

    def __repr__(self):
        values = []
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            for data_type, formatter in self.__repr_formatters__:
                if isinstance(value, data_type):
                    value = formatter(value)
                    break
            values.append(f"{column.name}={value!r}")
        return f"<{self.__class__.__name__}({', '.join(values)})>"


class FileRecord(ReprMixin, Base):
    __tablename__ = "file_records"
    __table_args__ = (UniqueConstraint("local_path"),)

    ark_base_name: Mapped[str] = mapped_column(Text, primary_key=True)
    local_path: Mapped[str] = mapped_column(Text, nullable=False)
    digest: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    metadata_data: Mapped[dict] = mapped_column(JSON, nullable=False)
    linkdata: Mapped[dict] = mapped_column(JSON, nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    def insert(self, session):
        upsert_with_policy(session, self,
                           policy={"created": ConflictAction.IGNORE, "updated": ConflictAction.UPDATE},
                           default_policy=ConflictAction.WARNING)

    @staticmethod
    def initialize_database(db_url: str) -> sessionmaker:
        engine = create_engine(db_url, echo=False, future=True)
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine, autoflush=False, autocommit=False)


