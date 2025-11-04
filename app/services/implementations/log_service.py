from uuid import UUID, uuid4
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging
import re
import json


class LogService:
    """
    Enhanced Log Service implementation with organization_id support
    and organization_name conversion capabilities.
    """
        
    MAX_DESCRIPTION_LENGTH = 10000
    MAX_SERVICE_NAME_LENGTH = 255
    MAX_ORGANIZATION_NAME_LENGTH = 255
    VALID_STATUSES = {"success", "error", "pending"}
    MAX_BATCH_INSERT_SIZE = 1000
    DEFAULT_PAGINATION_LIMIT = 100
    MAX_PAGINATION_LIMIT = 1000
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.logger = logging.getLogger(__name__)

        
    def _validate_organization_id(self, organization_id: UUID) -> str:
        """Validate organization_id format"""
        if not organization_id:
            raise ValueError("Organization ID cannot be empty")
        
        try:
            return str(UUID(str(organization_id)))
        except ValueError:
            raise ValueError(f"Invalid organization ID format: {organization_id}")

    def _validate_organization_name(self, organization_name: str) -> str:
        """Validate and sanitize organization name"""
        if not organization_name or not organization_name.strip():
            raise ValueError("Organization name cannot be empty")
        
        organization_name = organization_name.strip()
        if len(organization_name) > self.MAX_ORGANIZATION_NAME_LENGTH:
            raise ValueError(f"Organization name too long (max {self.MAX_ORGANIZATION_NAME_LENGTH} characters)")
                
        if not re.match(r'^[a-zA-Z0-9_\-\.\s]+$', organization_name):
            raise ValueError("Organization name contains invalid characters")
            
        return organization_name
    

    async def _get_organization_name_by_id(self, organization_id: UUID) -> Optional[str]:
        """
        Convert organization_id to organization_name by querying organizations table
        """
        try:
            query = """
            SELECT organization_name FROM public.organizations 
            WHERE id = :organization_id
            """
            
            result = await self.db.execute(
                text(query),
                {"organization_id": str(organization_id)}
            )
            organization = result.mappings().first()
            
            return organization['organization_name'] if organization else None
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch organization name for ID {organization_id}: {str(e)}")
            return None

    async def _get_organization_id_by_name(self, organization_name: str) -> Optional[UUID]:
        """
        Convert organization_name to organization_id by querying organizations table
        """
        try:
            query = """
            SELECT id FROM public.organizations 
            WHERE organization_name = :organization_name
            """
            
            result = await self.db.execute(
                text(query),
                {"organization_name": organization_name}
            )
            organization = result.mappings().first()
            
            return UUID(organization['id']) if organization else None
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch organization ID for name {organization_name}: {str(e)}")
            return None

    async def _enrich_with_organization_name(self, log_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich log data with organization_name if organization_id is present
        """
        if log_data.get('organization_id'):
            organization_name = await self._get_organization_name_by_id(
                UUID(log_data['organization_id'])
            )
            if organization_name:
                log_data['organization_name'] = organization_name
            else:
                log_data['organization_name'] = None
                self.logger.warning(f"Organization not found for ID: {log_data['organization_id']}")
        
        return log_data

    async def _enrich_list_with_organization_names(self, logs_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich a list of log records with organization names
        """
       
        organization_ids = set()
        for log in logs_list:
            if log.get('organization_id'):
                organization_ids.add(log['organization_id'])
        
       
        organization_names_map = {}
        if organization_ids:
            try:
                query = """
                SELECT id, organization_name FROM public.organizations 
                WHERE id IN :organization_ids
                """
                
                # Convert set to tuple for SQL IN clause
                org_ids_tuple = tuple(organization_ids)
                result = await self.db.execute(
                    text(query),
                    {"organization_ids": org_ids_tuple}
                )
                organizations = result.mappings().all()
                
                organization_names_map = {
                    str(org['id']): org['organization_name'] 
                    for org in organizations
                }
                
            except Exception as e:
                self.logger.error(f"Failed to batch fetch organization names: {str(e)}")
        
        
        enriched_logs = []
        for log in logs_list:
            enriched_log = log.copy()
            org_id = log.get('organization_id')
            if org_id and org_id in organization_names_map:
                enriched_log['organization_name'] = organization_names_map[org_id]
            elif org_id:
                enriched_log['organization_name'] = None
            enriched_logs.append(enriched_log)
        
        return enriched_logs

    

    async def create_log(self, log_data: dict) -> dict:
        """
        Create a new log entry with organization_id support
        """
        try:
            
            service_name = self._validate_service_name(log_data.get('service_name', ''))
            status = self._validate_status(log_data.get('status', 'pending'))
                        
            organization_id = None
            if log_data.get('organization_id'):
                organization_id = self._validate_organization_id(log_data['organization_id'])
                        
            organization_name = log_data.get('organization_name')
            if organization_name and not organization_id:
                organization_id = await self._get_organization_id_by_name(organization_name)
                if not organization_id:
                    raise ValueError(f"Organization not found with name: {organization_name}")
                        
            if log_data.get('error_details') and status == 'success':
                status = 'error'
                self.logger.warning(f"Auto-corrected status to 'error' for service {service_name} with error details")
            
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
                status, log_description, error_details, created_at,
                metadata, tags, correlation_id, organization_id
            ) VALUES (
                :id, :service_name, :start_date, :start_times, :duration_ms,
                :status, :log_description, :error_details, NOW(),
                :metadata::jsonb, :tags, :correlation_id, :organization_id
            )
            RETURNING 
                id, service_name, start_date, start_times, duration_ms,
                status, log_description, error_details, created_at,
                updated_at, metadata, tags, correlation_id, organization_id
            """
            
            params = {
                "id": str(log_id),
                "service_name": service_name,
                "start_date": start_date,
                "start_times": start_times,
                "duration_ms": duration_ms,
                "status": status,
                "log_description": log_description,
                "error_details": error_details,
                "metadata": json.dumps(log_data.get('metadata', {})),
                "tags": log_data.get('tags', []),
                "correlation_id": log_data.get('correlation_id'),
                "organization_id": organization_id
            }
            
            result = await self.db.execute(text(insert_query), params)
            created_log = result.mappings().first()
            
            if not created_log:
                raise Exception("Failed to create log - no rows returned")
            
            result_dict = dict(created_log)
                        
            result_dict = await self._enrich_with_organization_name(result_dict)
                        
            if result_dict.get('created_at'):
                result_dict['created_at'] = result_dict['created_at'].isoformat()
            if result_dict.get('updated_at'):
                result_dict['updated_at'] = result_dict['updated_at'].isoformat()
            if result_dict.get('start_date'):
                result_dict['start_date'] = result_dict['start_date'].isoformat()
            
            self.logger.info(f"Log created successfully: {log_id} for service: {service_name}, organization: {result_dict.get('organization_name')}")
            return result_dict
            
        except Exception as e:
            self.logger.error(f"Error creating log: {str(e)}", exc_info=True)
            raise

    async def get_logs_by_organization(self, organization_id: UUID, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Get logs by organization_id with organization_name enrichment
        """
        org_id = self._validate_organization_id(organization_id)
        safe_limit, safe_offset = self._validate_pagination_params(limit, offset)
                
        organization_name = await self._get_organization_name_by_id(organization_id)
                
        count_query = """
        SELECT COUNT(*) as total FROM public.logs 
        WHERE organization_id = :organization_id
        """
        
        count_result = await self.db.execute(
            text(count_query),
            {"organization_id": org_id}
        )
        total_count = count_result.scalar() or 0
                
        query = """
        SELECT * FROM public.logs 
        WHERE organization_id = :organization_id 
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        result = await self.db.execute(
            text(query),
            {
                "organization_id": org_id,
                "limit": safe_limit,
                "offset": safe_offset
            }
        )
        logs = result.mappings().all()
                
        processed_logs = []
        for log in logs:
            log_dict = dict(log)
            # Convert datetime objects to ISO format strings
            for date_field in ['start_date', 'created_at', 'updated_at']:
                if log_dict.get(date_field) and hasattr(log_dict[date_field], 'isoformat'):
                    log_dict[date_field] = log_dict[date_field].isoformat()
            processed_logs.append(log_dict)
                
        enriched_logs = await self._enrich_list_with_organization_names(processed_logs)
        
        return {
            "data": enriched_logs,
            "pagination": {
                "total": total_count,
                "limit": safe_limit,
                "offset": safe_offset,
                "has_more": (safe_offset + len(enriched_logs)) < total_count
            },
            "organization": {
                "id": org_id,
                "name": organization_name
            }
        }

    async def get_logs_by_organization_name(self, organization_name: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Get logs by organization_name (converts to organization_id internally)
        """
        org_name = self._validate_organization_name(organization_name)
                
        organization_id = await self._get_organization_id_by_name(org_name)
        if not organization_id:
            raise ValueError(f"Organization not found with name: {org_name}")
        
        
        return await self.get_logs_by_organization(organization_id, limit, offset)

    async def get_log_by_id(self, log_id: UUID) -> dict:
        """
        Get log by ID with organization_name enrichment
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
        
        log_dict = dict(log)
        
        
        log_dict = await self._enrich_with_organization_name(log_dict)
                
        for date_field in ['start_date', 'created_at', 'updated_at']:
            if log_dict.get(date_field) and hasattr(log_dict[date_field], 'isoformat'):
                log_dict[date_field] = log_dict[date_field].isoformat()
        
        
        if log_dict.get('metadata'):
            try:
                log_dict['metadata'] = json.loads(log_dict['metadata']) if isinstance(log_dict['metadata'], str) else log_dict['metadata']
            except json.JSONDecodeError:
                log_dict['metadata'] = {}
        
        return log_dict

    async def update_log(self, log_id: UUID, log_data: dict) -> dict:
        """
        Update an existing log with organization_id/organization_name support
        """
        
        existing_log = await self.get_log_by_id(log_id)
        
        update_fields = []
        params = {"log_id": str(log_id)}
                
        allowed_fields = {
            'service_name', 'status', 'log_description', 
            'error_details', 'duration_ms', 'start_times',
            'metadata', 'tags', 'correlation_id', 'organization_id'
        }
                
        if 'organization_name' in log_data and log_data['organization_name']:
            organization_id = await self._get_organization_id_by_name(log_data['organization_name'])
            if not organization_id:
                raise ValueError(f"Organization not found with name: {log_data['organization_name']}")
            log_data['organization_id'] = organization_id
            
            log_data.pop('organization_name')
        
        for field, value in log_data.items():
            if field not in allowed_fields:
                self.logger.warning(f"Attempted to update disallowed field: {field}")
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
                    
                    if value and log_data.get('status') != 'error':
                        update_fields.append("status = 'error'")
                elif field == 'duration_ms':
                    value = self._validate_duration(value)
                elif field == 'start_times':
                    value = max(0, value)
                elif field == 'metadata':
                    value = json.dumps(value) if value else None
                elif field == 'organization_id':
                    value = self._validate_organization_id(value)
                
                update_fields.append(f"{field} = :{field}")
                params[field] = value
        
        if not update_fields:
            return existing_log
        
        
        update_fields.append("updated_at = NOW()")
        
        
        update_query = f"""
        WITH updated_log AS (
            UPDATE public.logs 
            SET {', '.join(update_fields)}
            WHERE id = :log_id
            RETURNING *
        )
        SELECT * FROM updated_log
        """
        
        result = await self.db.execute(text(update_query), params)
        updated_log = result.mappings().first()
        
        if not updated_log:
            raise ValueError(f"Log not found with ID: {log_id}")
        
        updated_log_dict = dict(updated_log)
                
        updated_log_dict = await self._enrich_with_organization_name(updated_log_dict)
                
        for date_field in ['start_date', 'created_at', 'updated_at']:
            if updated_log_dict.get(date_field) and hasattr(updated_log_dict[date_field], 'isoformat'):
                updated_log_dict[date_field] = updated_log_dict[date_field].isoformat()
        
        self.logger.info(f"Log updated successfully: {log_id}")
        return updated_log_dict

    # ========== ENHANCED SEARCH WITH ORGANIZATION SUPPORT ==========

    async def search_logs(
        self,
        service_name: Optional[str] = None,
        status: Optional[str] = None,
        organization_id: Optional[UUID] = None,
        organization_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        min_duration: Optional[int] = None,
        max_duration: Optional[int] = None,
        tags: Optional[List[str]] = None,
        correlation_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Advanced search with organization_id and organization_name support
        """
        conditions = []
        params = {}
                
        if organization_name and not organization_id:
            organization_id = await self._get_organization_id_by_name(organization_name)
            if not organization_id:
                raise ValueError(f"Organization not found with name: {organization_name}")
        
        
        if service_name:
            service_name = self._validate_service_name(service_name)
            conditions.append("service_name = :service_name")
            params["service_name"] = service_name
            
        if status:
            status = self._validate_status(status)
            conditions.append("status = :status")
            params["status"] = status
            
        if organization_id:
            org_id = self._validate_organization_id(organization_id)
            conditions.append("organization_id = :organization_id")
            params["organization_id"] = org_id
            
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
            
        if tags:
            conditions.append("tags && :tags")
            params["tags"] = tags
            
        if correlation_id:
            conditions.append("correlation_id = :correlation_id")
            params["correlation_id"] = correlation_id
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        safe_limit, safe_offset = self._validate_pagination_params(limit, offset)
                
        count_query = f"SELECT COUNT(*) as total FROM public.logs WHERE {where_clause}"
        count_result = await self.db.execute(text(count_query), params)
        total_count = count_result.scalar() or 0
                
        data_query = f"""
        SELECT 
            id, service_name, start_date, start_times, duration_ms,
            status, log_description, error_details, created_at,
            updated_at, metadata, tags, correlation_id, organization_id,
            -- Business metrics
            CASE 
                WHEN duration_ms > 10000 THEN 'HIGH'
                WHEN duration_ms > 1000 THEN 'MEDIUM' 
                ELSE 'LOW'
            END as performance_category
        FROM public.logs 
        WHERE {where_clause}
        ORDER BY start_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        params.update({
            "limit": safe_limit,
            "offset": safe_offset
        })
        
        result = await self.db.execute(text(data_query), params)
        logs = result.mappings().all()
                
        processed_logs = []
        for log in logs:
            log_dict = dict(log)
            
            for date_field in ['start_date', 'created_at', 'updated_at']:
                if log_dict.get(date_field) and hasattr(log_dict[date_field], 'isoformat'):
                    log_dict[date_field] = log_dict[date_field].isoformat()
            
            if log_dict.get('metadata'):
                try:
                    log_dict['metadata'] = json.loads(log_dict['metadata']) if isinstance(log_dict['metadata'], str) else log_dict['metadata']
                except json.JSONDecodeError:
                    log_dict['metadata'] = {}
            processed_logs.append(log_dict)
        
        
        enriched_logs = await self._enrich_list_with_organization_names(processed_logs)
                
        organization_info = None
        if organization_id:
            org_name = await self._get_organization_name_by_id(organization_id)
            organization_info = {
                "id": str(organization_id),
                "name": org_name
            }
        
        return {
            "data": enriched_logs,
            "pagination": {
                "total": total_count,
                "limit": safe_limit,
                "offset": safe_offset,
                "has_more": (safe_offset + len(enriched_logs)) < total_count
            },
            "organization": organization_info,
            "search_metrics": {
                "results_count": len(enriched_logs),
                "performance_breakdown": self._calculate_performance_breakdown(enriched_logs)
            }
        }

    # ========== ORGANIZATION ANALYTICS METHODS ==========

    async def get_organization_statistics(self, organization_id: UUID) -> Dict[str, Any]:
        """
        Get comprehensive statistics for an organization
        """
        org_id = self._validate_organization_id(organization_id)
        organization_name = await self._get_organization_name_by_id(organization_id)

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
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
            MAX(start_date) as last_log_date,
            MIN(start_date) as first_log_date,
            COUNT(DISTINCT service_name) as unique_services,
            COUNT(DISTINCT correlation_id) as unique_operations
        FROM public.logs 
        WHERE organization_id = :organization_id
        """
        
        result = await self.db.execute(
            text(query),
            {"organization_id": org_id}
        )
        stats = result.mappings().first()
        
        stats_dict = dict(stats) if stats else {}
                
        stats_dict['organization'] = {
            "id": org_id,
            "name": organization_name
        }
        
        return self._process_statistics(stats_dict)

    async def get_organizations_overview(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get overview of all organizations with their log statistics
        """
        safe_limit = min(limit, 100)
        
        query = """
        WITH org_stats AS (
            SELECT 
                l.organization_id,
                o.organization_name,
                COUNT(*) as total_logs,
                COUNT(CASE WHEN l.status = 'success' THEN 1 END) as success_count,
                COUNT(CASE WHEN l.status = 'error' THEN 1 END) as error_count,
                AVG(l.duration_ms) as avg_duration_ms,
                MAX(l.start_date) as last_activity
            FROM public.logs l
            LEFT JOIN public.organizations o ON l.organization_id = o.id
            WHERE l.organization_id IS NOT NULL
            GROUP BY l.organization_id, o.organization_name
            ORDER BY total_logs DESC
            LIMIT :limit
        )
        SELECT * FROM org_stats
        """
        
        result = await self.db.execute(
            text(query),
            {"limit": safe_limit}
        )
        organizations_stats = result.mappings().all()
                
        processed_orgs = []
        for org in organizations_stats:
            org_dict = dict(org)
                        
            total = org_dict.get('total_logs', 0)
            if total > 0:
                org_dict['success_rate'] = round((org_dict.get('success_count', 0) / total) * 100, 2)
                org_dict['error_rate'] = round((org_dict.get('error_count', 0) / total) * 100, 2)
                org_dict['health_score'] = 100 - org_dict['error_rate']
            
            
            if org_dict.get('last_activity') and hasattr(org_dict['last_activity'], 'isoformat'):
                org_dict['last_activity'] = org_dict['last_activity'].isoformat()
            
            processed_orgs.append(org_dict)
        
        return processed_orgs

    async def get_organization_services(self, organization_id: UUID) -> List[Dict[str, Any]]:
        """
        Get all services for an organization with their statistics
        """
        org_id = self._validate_organization_id(organization_id)
        
        query = """
        SELECT 
            service_name,
            COUNT(*) as total_requests,
            COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
            COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
            AVG(duration_ms) as avg_duration_ms,
            MAX(start_date) as last_request
        FROM public.logs 
        WHERE organization_id = :organization_id
        GROUP BY service_name
        ORDER BY total_requests DESC
        """
        
        result = await self.db.execute(
            text(query),
            {"organization_id": org_id}
        )
        services_stats = result.mappings().all()
        
        processed_services = []
        for service in services_stats:
            service_dict = dict(service)
                        
            total = service_dict.get('total_requests', 0)
            if total > 0:
                service_dict['success_rate'] = round((service_dict.get('success_count', 0) / total) * 100, 2)
                service_dict['error_rate'] = round((service_dict.get('error_count', 0) / total) * 100, 2)
                        
            if service_dict.get('last_request') and hasattr(service_dict['last_request'], 'isoformat'):
                service_dict['last_request'] = service_dict['last_request'].isoformat()
            
            processed_services.append(service_dict)
        
        return processed_services

    def _process_statistics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Process and format statistics data"""
        
        if stats.get('avg_duration_ms'):
            stats['avg_duration_ms'] = float(stats['avg_duration_ms'])
        if stats.get('median_duration_ms'):
            stats['median_duration_ms'] = float(stats['median_duration_ms'])
        if stats.get('p95_duration_ms'):
            stats['p95_duration_ms'] = float(stats['p95_duration_ms'])
                
        total = stats.get('total_logs', 0)
        if total > 0:
            stats['success_rate'] = round((stats.get('success_count', 0) / total) * 100, 2)
            stats['error_rate'] = round((stats.get('error_count', 0) / total) * 100, 2)
            stats['availability'] = 100 - stats['error_rate']
        
        return stats

    def _calculate_performance_breakdown(self, logs: List[dict]) -> Dict[str, int]:
        """Calculate performance breakdown for search results"""
        breakdown = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for log in logs:
            category = log.get('performance_category', 'LOW')
            breakdown[category] = breakdown.get(category, 0) + 1
        return breakdown