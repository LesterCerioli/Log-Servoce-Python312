from datetime import datetime
from uuid import UUID, uuid4
from enum import Enum
from sqlalchemy import String, Integer, BigInteger, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PGUUID


class Base(DeclarativeBase):
    pass


class LogLevel(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"


class Log(Base):
    __tablename__ = "logs"
    
    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4
    )
    service_name: Mapped[str] = mapped_column(String(100))
    start_date: Mapped[datetime] = mapped_column(DateTime)
    start_times: Mapped[int] = mapped_column(Integer)  # start_times_running in Go
    duration_ms: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[LogLevel] = mapped_column(String(20))
    log_description: Mapped[str] = mapped_column(String(500))
    error_details: Mapped[str] = mapped_column(String(1000), nullable=True)
    
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Log(id={self.id}, service='{self.service_name}', status='{self.status.value}')>"