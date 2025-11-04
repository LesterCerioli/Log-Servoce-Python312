from enum import Enum
from pydantic import BaseModel, EmailStr, Field, validator
from typing import Dict, Optional, List
from datetime import datetime, date
from uuid import UUID



# ==================================================
#                  LOG SCHEMAS 
# ==================================================

class LogStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"

class LogBase(BaseModel):
    service_name: str
    start_date: Optional[datetime] = None
    start_times: int = Field(default=0, ge=0)
    duration_ms: int = Field(default=0, ge=0)
    status: LogStatus
    log_description: Optional[str] = None
    error_details: Optional[str] = None

class LogCreate(LogBase):
    token: str
    id: Optional[UUID] = None

class LogUpdate(BaseModel):
    token: str
    service_name: Optional[str] = None
    start_date: Optional[datetime] = None
    start_times: Optional[int] = Field(None, ge=0)
    duration_ms: Optional[int] = Field(None, ge=0)
    status: Optional[LogStatus] = None
    log_description: Optional[str] = None
    error_details: Optional[str] = None

class LogResponse(BaseModel):
    id: UUID
    service_name: str
    start_date: datetime
    start_times: int
    duration_ms: int
    status: LogStatus
    log_description: Optional[str]
    error_details: Optional[str]

    class Config:
        from_attributes = True

class LogListResponse(BaseModel):
    logs: List[LogResponse]
    total_count: int

class DateRangeRequest(BaseModel):
    token: str
    start_date: datetime
    end_date: datetime

class ServiceStatsResponse(BaseModel):
    total_logs: int
    success_count: int
    error_count: int
    pending_count: int
    avg_duration_ms: Optional[float]
    last_log_date: Optional[datetime]

class HighDurationRequest(BaseModel):
    token: str
    threshold_ms: int = Field(..., ge=0)

class CleanupRequest(BaseModel):
    token: str
    older_than_days: int = Field(..., ge=1)

class CleanupResponse(BaseModel):
    deleted_count: int
    message: str
