from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID
from typing import List, Optional
from log_dto import (
    LogDTO,
    LogCreateDTO,
    LogUpdateDTO,
    LogResponseDTO,
    LogFilterDTO,
    LogListDTO,
    LogLevel
)


class LogServiceContract(ABC):
    """Contract for log management and monitoring services"""
    
    @abstractmethod
    async def create_log(self, log: LogCreateDTO) -> LogResponseDTO:
        """
        Create a new log entry
        
        Args:
            log: Log data for creation
            
        Returns:
            LogResponseDTO: Created log entry with generated ID
            
        Raises:
            ValidationError: If log data is invalid
        """
        pass
    
    @abstractmethod
    async def get_log_by_id(self, log_id: UUID) -> LogResponseDTO:
        """
        Get log by ID
        
        Args:
            log_id: UUID of the log entry
            
        Returns:
            LogResponseDTO: Found log entry
            
        Raises:
            NotFoundError: If log entry not found
        """
        pass
    
    @abstractmethod
    async def get_logs_by_service(self, service_name: str, filter_dto: Optional[LogFilterDTO] = None) -> LogListDTO:
        """
        Get logs by service name
        
        Args:
            service_name: Name of the service to filter by
            filter_dto: Optional additional filters
            
        Returns:
            LogListDTO: Paginated list of logs for the specified service
        """
        pass
    
    @abstractmethod
    async def get_logs_by_service_name(self, service_name: str, filter_dto: Optional[LogFilterDTO] = None) -> LogListDTO:
        """
        Get logs by service name (alias for get_logs_by_service)
        
        Args:
            service_name: Name of the service to filter by
            filter_dto: Optional additional filters
            
        Returns:
            LogListDTO: Paginated list of logs for the specified service
        """
        pass
    
    @abstractmethod
    async def get_logs_by_status(self, status: LogLevel, filter_dto: Optional[LogFilterDTO] = None) -> LogListDTO:
        """
        Get logs by status level
        
        Args:
            status: Log level to filter by (success, failure, etc.)
            filter_dto: Optional additional filters
            
        Returns:
            LogListDTO: Paginated list of logs with the specified status
        """
        pass
    
    @abstractmethod
    async def get_all_logs(self, filter_dto: Optional[LogFilterDTO] = None) -> LogListDTO:
        """
        Get all logs with optional filtering
        
        Args:
            filter_dto: Optional filters for the query
            
        Returns:
            LogListDTO: Paginated list of logs
        """
        pass
    
    @abstractmethod
    async def update_log(self, log_id: UUID, log: LogUpdateDTO) -> LogResponseDTO:
        """
        Update an existing log entry
        
        Args:
            log_id: UUID of the log entry to update
            log: Updated log data
            
        Returns:
            LogResponseDTO: Updated log entry
            
        Raises:
            NotFoundError: If log entry not found
            ValidationError: If update data is invalid
        """
        pass
    
    @abstractmethod
    async def delete_log_by_id(self, log_id: UUID) -> None:
        """
        Delete a log entry by ID
        
        Args:
            log_id: UUID of the log entry to delete
            
        Raises:
            NotFoundError: If log entry not found
        """
        pass
    
    @abstractmethod
    async def get_logs_by_date_range(self, start_date: datetime, end_date: datetime, filter_dto: Optional[LogFilterDTO] = None) -> LogListDTO:
        """
        Get logs within a date range
        
        Args:
            start_date: Start of the date range
            end_date: End of the date range
            filter_dto: Optional additional filters
            
        Returns:
            LogListDTO: Paginated list of logs in the date range
            
        Raises:
            ValidationError: If date range is invalid
        """
        pass
    
    @abstractmethod
    async def get_logs_by_duration_threshold(self, min_duration_ms: int, max_duration_ms: Optional[int] = None) -> LogListDTO:
        """
        Get logs by execution duration threshold
        
        Args:
            min_duration_ms: Minimum duration in milliseconds
            max_duration_ms: Optional maximum duration in milliseconds
            
        Returns:
            LogListDTO: Paginated list of logs within duration range
            
        Raises:
            ValidationError: If duration thresholds are invalid
        """
        pass
    
    @abstractmethod
    async def search_logs(self, query: str, filter_dto: Optional[LogFilterDTO] = None) -> LogListDTO:
        """
        Search logs by text in log description or error details
        
        Args:
            query: Search term
            filter_dto: Optional additional filters
            
        Returns:
            LogListDTO: Paginated list of matching logs
        """
        pass
    
    @abstractmethod
    async def get_service_statistics(self, service_name: str, start_date: datetime, end_date: datetime) -> dict:
        """
        Get statistics for a specific service
        
        Args:
            service_name: Name of the service
            start_date: Start of the statistics period
            end_date: End of the statistics period
            
        Returns:
            dict: Statistics including total logs, success rate, average duration, etc.
            
        Raises:
            ValidationError: If date range is invalid
        """
        pass
    
    @abstractmethod
    async def get_system_health_report(self, start_date: datetime, end_date: datetime) -> dict:
        """
        Get system health report based on log analysis
        
        Args:
            start_date: Start of the report period
            end_date: End of the report period
            
        Returns:
            dict: Health report with service status, error rates, performance metrics
            
        Raises:
            ValidationError: If date range is invalid
        """
        pass
    
    @abstractmethod
    async def cleanup_old_logs(self, older_than_days: int) -> int:
        """
        Clean up logs older than specified days
        
        Args:
            older_than_days: Delete logs older than this many days
            
        Returns:
            int: Number of logs deleted
            
        Raises:
            ValidationError: If days threshold is invalid
        """
        pass
    
    @abstractmethod
    async def export_logs(self, filter_dto: Optional[LogFilterDTO] = None, format: str = "json") -> str:
        """
        Export logs in specified format
        
        Args:
            filter_dto: Optional filters for the export
            format: Export format (json, csv, xml)
            
        Returns:
            str: Exported logs data
            
        Raises:
            ValidationError: If export format is invalid
        """
        pass
    
    @abstractmethod
    async def get_error_trends(self, service_name: Optional[str] = None, days: int = 30) -> dict:
        """
        Get error trends over time
        
        Args:
            service_name: Optional service name to filter by
            days: Number of days to analyze
            
        Returns:
            dict: Error trends with daily counts and patterns
            
        Raises:
            ValidationError: If days parameter is invalid
        """
        pass
    
    @abstractmethod
    async def log_service_execution(self, service_name: str, start_time: datetime, duration_ms: int, status: LogLevel, description: str, error_details: Optional[str] = None) -> LogResponseDTO:
        """
        Convenience method to log service execution with automatic timing
        
        Args:
            service_name: Name of the service
            start_time: When the service started execution
            duration_ms: Execution duration in milliseconds
            status: Execution status (success/failure)
            description: Log description
            error_details: Optional error details for failures
            
        Returns:
            LogResponseDTO: Created log entry
        """
        pass
    
    @abstractmethod
    async def get_performance_metrics(self, service_name: str, time_window_hours: int = 24) -> dict:
        """
        Get performance metrics for a service
        
        Args:
            service_name: Name of the service
            time_window_hours: Time window for metrics in hours
            
        Returns:
            dict: Performance metrics including avg duration, p95, error rate, etc.
            
        Raises:
            ValidationError: If time window is invalid
        """
        pass
    
    @abstractmethod
    async def bulk_create_logs(self, logs: List[LogCreateDTO]) -> dict:
        """
        Bulk create multiple log entries
        
        Args:
            logs: List of log data to create
            
        Returns:
            dict: Bulk operation results with success/failure counts
            
        Raises:
            ValidationError: If any log data is invalid
        """
        pass