from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field, validator


class LogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class LogBaseDTO(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=255)
    log_description: Optional[str] = Field(None, max_length=10000)
    level: LogLevel = Field(LogLevel.INFO)
    duration_ms: int = Field(0, ge=0, le=864000000)
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    tags: Optional[List[str]] = Field(default_factory=list)
    correlation_id: Optional[str] = Field(None, max_length=100)


class LogCreateDTO(LogBaseDTO):
    id: Optional[UUID] = None
    start_date: Optional[datetime] = None
    start_times: int = Field(0, ge=0)
    error_details: Optional[str] = Field(None, max_length=10000)
    organization_id: Optional[UUID] = None
    organization_name: Optional[str] = Field(None, max_length=255)


class LogUpdateDTO(BaseModel):
    service_name: Optional[str] = Field(None, min_length=1, max_length=255)
    log_description: Optional[str] = Field(None, max_length=10000)
    level: Optional[LogLevel] = None
    duration_ms: Optional[int] = Field(None, ge=0, le=864000000)
    error_details: Optional[str] = Field(None, max_length=10000)
    start_times: Optional[int] = Field(None, ge=0)
    metadata: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    correlation_id: Optional[str] = Field(None, max_length=100)
    organization_id: Optional[UUID] = None
    organization_name: Optional[str] = Field(None, max_length=255)


class LogDTO(LogBaseDTO):
    id: UUID
    start_date: datetime
    start_times: int
    error_details: Optional[str]
    organization_id: Optional[UUID]
    organization_name: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }


class LogResponseDTO(LogDTO):
    performance_category: Optional[str] = None
    is_recent: Optional[bool] = None


class LogFilterDTO(BaseModel):
    service_name: Optional[str] = None
    level: Optional[LogLevel] = None
    organization_id: Optional[UUID] = None
    organization_name: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    min_duration: Optional[int] = Field(None, ge=0)
    max_duration: Optional[int] = Field(None, ge=0)
    tags: Optional[List[str]] = None
    correlation_id: Optional[str] = None
    has_errors: Optional[bool] = None
    search_text: Optional[str] = None
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)


class LogListDTO(BaseModel):
    data: List[LogResponseDTO]
    pagination: Dict[str, Any]
    filters: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None