from uuid import UUID, uuid4
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging
import re


class LogCRUD:
    """
    Enhanced Log CRUD implementation with improved security,
    API optimization, and data processing capabilities.
    """
    
    
    MAX_DESCRIPTION_LENGTH = 10000
    MAX_SERVICE_NAME_LENGTH = 255
    VALID_STATUSES = {"success", "error", "pending"}
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.logger = logging.getLogger(__name__)

    def _validate_service_name(self, service_name: str) -> str:
        """Validate and sanitize service name"""
        if not service_name or not service_name.strip():
            raise ValueError("Service name cannot be empty")
        
        service_name = service_name.strip()
        if len(service_name) > self.MAX_SERVICE_NAME_LENGTH:
            raise ValueError(f"Service name too long (max {self.MAX_SERVICE_NAME_LENGTH} characters)")
        
        
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', service_name):
            raise ValueError("Service name contains invalid characters")
            
        return service_name

    def _validate_text_field(self, field_name: str, value: str, max_length: int) -> Optional[str]:
        """Validate and sanitize text fields"""
        if value is None:
            return None
            
        value = str(value).strip()
        if len(value) > max_length:
            raise ValueError(f"{field_name} too long (max {max_length} characters)")
        
        
        value = re.sub(r'[\x00-\x1f\x7f]', '', value)  # Remove control characters
        return value if value else None

    def _validate_status(self, status: str) -> str:
        """Validate status value"""
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}. Must be one of: {self.VALID_STATUSES}")
        return status

    def _validate_duration(self, duration_ms: int) -> int:
        """Validate duration value"""
        if duration_ms < 0:
            raise ValueError("Duration cannot be negative")
        if duration_ms > 864000000:  # 10 days in milliseconds (reasonable upper limit)
            raise ValueError("Duration too large")
        return duration_ms

    async def create_log(self, log_data: dict) -> dict:
        """
        Create a new log entry with comprehensive validation
        """
        try:
            # Validate required fields
            service_name = self._validate_service_name(log_data.get('service_name', ''))
            status = self._validate_status(log_data.get('status', 'pending'))
            
            
            log_description = self._validate_text_field(
                'Log description', 
                log_data.get('log_description'), 
                self.MAX_DESCRIPTION_LENGTH
            )
            error_details = self._validate_text_field(
                'Error details',
                log_data.get('error_details'),
                self.MAX_DESCRIPTION_LENGTH
            )
            
            
            duration_ms = self._validate_duration(log_data.get('duration_ms', 0))
            
            
            log_id = log_data.get('id') or uuid4()
            start_date = log_data.get('start_date') or datetime.utcnow()
            start_times = max(0, log_data.get('start_times', 0))
            
            
            insert_query = """
            INSERT INTO public.logs (
                id, service_name, start_date, start_times, duration_ms,
                status, log_description, error_details, created_at
            ) VALUES (
                :id, :service_name, :start_date, :start_times, :duration_ms,
                :status, :log_description, :error_details, NOW()
            )
            RETURNING *
            """
            
            params = {
                "id": str(log_id),
                "service_name": service_name,
                "start_date": start_date,
                "start_times": start_times,
                "duration_ms": duration_ms,
                "status": status,
                "log_description": log_description,
                "error_details": error_details
            }
            
            result = await self.db.execute(text(insert_query), params)
            created_log = result.mappings().first()
            
            if not created_log:
                raise Exception("Failed to create log - no rows returned")
            
            self.logger.info(f"Log created successfully: {log_id}")
            return dict(created_log)
            
        except Exception as e:
            self.logger.error(f"Error creating log: {str(e)}")
            raise

    async def get_logs_by_service(self, service_name: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """
        Get logs by service name with pagination
        """
        service_name = self._validate_service_name(service_name)
        
        query = """
        SELECT * FROM public.logs 
        WHERE service_name = :service_name 
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        result = await self.db.execute(
            text(query),
            {
                "service_name": service_name,
                "limit": min(limit, 1000),  # Prevent excessive limits
                "offset": max(0, offset)
            }
        )
        logs = result.mappings().all()
        
        return [dict(log) for log in logs]

    async def get_logs_by_status(self, status: str, limit: int = 100, offset: int = 0) -> List[dict]:
        """
        Get logs by status with pagination
        """
        status = self._validate_status(status)
        
        query = """
        SELECT * FROM public.logs 
        WHERE status = :status 
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        result = await self.db.execute(
            text(query),
            {
                "status": status,
                "limit": min(limit, 1000),
                "offset": max(0, offset)
            }
        )
        logs = result.mappings().all()
        
        return [dict(log) for log in logs]

    async def get_log_by_id(self, log_id: UUID) -> dict:
        """
        Get log by ID with enhanced error handling
        """
        try:
            UUID(str(log_id))  # Validate UUID format
        except ValueError:
            raise ValueError(f"Invalid log ID format: {log_id}")
        
        query = """
        SELECT * FROM public.logs 
        WHERE id = :log_id
        """
        
        result = await self.db.execute(
            text(query),
            {"log_id": str(log_id)}
        )
        log = result.mappings().first()
        
        if not log:
            raise ValueError(f"Log not found with ID: {log_id}")
            
        return dict(log)

    async def update_log(self, log_id: UUID, log_data: dict) -> dict:
        """
        Update an existing log with comprehensive validation
        """
        
        await self.get_log_by_id(log_id)
        
        update_fields = []
        params = {"log_id": str(log_id)}
        
        
        allowed_fields = {
            'service_name', 'status', 'log_description', 
            'error_details', 'duration_ms', 'start_times'
        }
        
        for field, value in log_data.items():
            if field not in allowed_fields:
                continue
                
            if value is not None:
                if field == 'service_name':
                    value = self._validate_service_name(value)
                elif field == 'status':
                    value = self._validate_status(value)
                elif field == 'log_description':
                    value = self._validate_text_field('Log description', value, self.MAX_DESCRIPTION_LENGTH)
                elif field == 'error_details':
                    value = self._validate_text_field('Error details', value, self.MAX_DESCRIPTION_LENGTH)
                elif field == 'duration_ms':
                    value = self._validate_duration(value)
                elif field == 'start_times':
                    value = max(0, value)
                
                update_fields.append(f"{field} = :{field}")
                params[field] = value
        
        if not update_fields:
            return await self.get_log_by_id(log_id)
        
        
        update_fields.append("updated_at = NOW()")
        
        update_query = f"""
        UPDATE public.logs 
        SET {', '.join(update_fields)}
        WHERE id = :log_id
        RETURNING *
        """
        
        result = await self.db.execute(text(update_query), params)
        updated_log = result.mappings().first()
        
        if not updated_log:
            raise ValueError(f"Log not found with ID: {log_id}")
        
        self.logger.info(f"Log updated successfully: {log_id}")
        return dict(updated_log)

    async def delete_log_by_id(self, log_id: UUID) -> bool:
        """
        Delete log by ID with confirmation
        """
        await self.get_log_by_id(log_id)
        
        delete_query = """
        DELETE FROM public.logs 
        WHERE id = :log_id
        """
        
        result = await self.db.execute(
            text(delete_query),
            {"log_id": str(log_id)}
        )
        
        self.logger.info(f"Log deleted successfully: {log_id}")
        return True

    async def get_logs_by_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime,
        limit: int = 100,
        offset: int = 0
    ) -> List[dict]:
        """
        Get logs within a date range with pagination
        """
        if start_date > end_date:
            raise ValueError("Start date cannot be after end date")
        
        
        max_range_days = 365
        if (end_date - start_date).days > max_range_days:
            raise ValueError(f"Date range cannot exceed {max_range_days} days")

        query = """
        SELECT * FROM public.logs 
        WHERE start_date BETWEEN :start_date AND :end_date 
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        result = await self.db.execute(
            text(query),
            {
                "start_date": start_date,
                "end_date": end_date,
                "limit": min(limit, 1000),
                "offset": max(0, offset)
            }
        )
        logs = result.mappings().all()
        
        return [dict(log) for log in logs]

    async def get_logs_by_service_and_status(
        self, 
        service_name: str, 
        status: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[dict]:
        """
        Get logs by service name and status with pagination
        """
        service_name = self._validate_service_name(service_name)
        status = self._validate_status(status)

        query = """
        SELECT * FROM public.logs 
        WHERE service_name = :service_name AND status = :status 
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        result = await self.db.execute(
            text(query),
            {
                "service_name": service_name,
                "status": status,
                "limit": min(limit, 1000),
                "offset": max(0, offset)
            }
        )
        logs = result.mappings().all()
        
        return [dict(log) for log in logs]

    async def get_error_logs(self, limit: Optional[int] = 100) -> List[dict]:
        """
        Get error logs with safe limit handling
        """
        safe_limit = min(limit or 100, 1000)
        
        query = """
        SELECT * FROM public.logs 
        WHERE status = 'error' 
        ORDER BY start_date DESC
        LIMIT :limit
        """
        
        result = await self.db.execute(
            text(query),
            {"limit": safe_limit}
        )
        logs = result.mappings().all()
        
        return [dict(log) for log in logs]

    async def get_service_statistics(self, service_name: str) -> dict:
        """
        Get comprehensive statistics for a service
        """
        service_name = self._validate_service_name(service_name)

        query = """
        SELECT 
            COUNT(*) as total_logs,
            COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
            COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count,
            AVG(duration_ms) as avg_duration_ms,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as median_duration_ms,
            MAX(start_date) as last_log_date,
            MIN(start_date) as first_log_date
        FROM public.logs 
        WHERE service_name = :service_name
        """
        
        result = await self.db.execute(
            text(query),
            {"service_name": service_name}
        )
        stats = result.mappings().first()
        
        return self._process_statistics(dict(stats)) if stats else {}

    def _process_statistics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Process and format statistics data"""
        
        if stats.get('avg_duration_ms'):
            stats['avg_duration_ms'] = float(stats['avg_duration_ms'])
        if stats.get('median_duration_ms'):
            stats['median_duration_ms'] = float(stats['median_duration_ms'])
        
        
        total = stats.get('total_logs', 0)
        if total > 0:
            stats['success_rate'] = round((stats.get('success_count', 0) / total) * 100, 2)
            stats['error_rate'] = round((stats.get('error_count', 0) / total) * 100, 2)
        
        return stats

    async def get_high_duration_logs(
        self, 
        threshold_ms: int, 
        limit: int = 100,
        offset: int = 0
    ) -> List[dict]:
        """
        Get logs with duration above threshold with pagination
        """
        threshold_ms = self._validate_duration(threshold_ms)
        
        query = """
        SELECT * FROM public.logs 
        WHERE duration_ms > :threshold_ms 
        ORDER BY duration_ms DESC
        LIMIT :limit OFFSET :offset
        """
        
        result = await self.db.execute(
            text(query),
            {
                "threshold_ms": threshold_ms,
                "limit": min(limit, 1000),
                "offset": max(0, offset)
            }
        )
        logs = result.mappings().all()
        
        return [dict(log) for log in logs]

    async def cleanup_old_logs(self, older_than_days: int) -> Dict[str, Any]:
        """
        Clean up logs older than specified days with comprehensive reporting
        """
        if older_than_days < 1:
            raise ValueError("Days parameter must be at least 1")
        
        
        count_query = """
        SELECT COUNT(*) as count FROM public.logs 
        WHERE start_date < NOW() - INTERVAL ':days days'
        """
        
        count_result = await self.db.execute(
            text(count_query),
            {"days": older_than_days}
        )
        count_before = count_result.scalar() or 0
        
        if count_before == 0:
            return {
                "deleted_count": 0,
                "message": "No logs found to delete"
            }
        
        
        delete_query = """
        DELETE FROM public.logs 
        WHERE start_date < NOW() - INTERVAL ':days days'
        """
        
        result = await self.db.execute(
            text(delete_query),
            {"days": older_than_days}
        )
        
        deleted_count = result.rowcount
        
        self.logger.info(f"Cleaned up {deleted_count} logs older than {older_than_days} days")
        
        return {
            "deleted_count": deleted_count,
            "estimated_before": count_before,
            "older_than_days": older_than_days,
            "cleanup_date": datetime.utcnow().isoformat()
        }

    async def search_logs(
        self,
        service_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        min_duration: Optional[int] = None,
        max_duration: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Advanced search with multiple filters and pagination metadata
        """
        
        conditions = []
        params = {}
        
        if service_name:
            service_name = self._validate_service_name(service_name)
            conditions.append("service_name = :service_name")
            params["service_name"] = service_name
            
        if status:
            status = self._validate_status(status)
            conditions.append("status = :status")
            params["status"] = status
            
        if start_date and end_date:
            if start_date > end_date:
                raise ValueError("Start date cannot be after end date")
            conditions.append("start_date BETWEEN :start_date AND :end_date")
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif start_date:
            conditions.append("start_date >= :start_date")
            params["start_date"] = start_date
        elif end_date:
            conditions.append("start_date <= :end_date")
            params["end_date"] = end_date
            
        if min_duration is not None:
            min_duration = self._validate_duration(min_duration)
            conditions.append("duration_ms >= :min_duration")
            params["min_duration"] = min_duration
            
        if max_duration is not None:
            max_duration = self._validate_duration(max_duration)
            conditions.append("duration_ms <= :max_duration")
            params["max_duration"] = max_duration
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        
        count_query = f"SELECT COUNT(*) as total FROM public.logs WHERE {where_clause}"
        count_result = await self.db.execute(text(count_query), params)
        total_count = count_result.scalar() or 0
                
        data_query = f"""
        SELECT * FROM public.logs 
        WHERE {where_clause}
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        params["limit"] = min(limit, 1000)
        params["offset"] = max(0, offset)
        
        result = await self.db.execute(text(data_query), params)
        logs = [dict(log) for log in result.mappings().all()]
        
        return {
            "data": logs,
            "pagination": {
                "total": total_count,
                "limit": params["limit"],
                "offset": params["offset"],
                "has_more": (params["offset"] + len(logs)) < total_count
            }
        }

    async def get_recent_services(self, limit: int = 10) -> List[str]:
        """
        Get list of recently active services
        """
        query = """
        SELECT DISTINCT service_name 
        FROM public.logs 
        WHERE start_date > NOW() - INTERVAL '7 days'
        ORDER BY service_name
        LIMIT :limit
        """
        
        result = await self.db.execute(
            text(query),
            {"limit": min(limit, 50)}
        )
        
        services = [row[0] for row in result.all()]
        return services