import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any, List, Tuple
from app.config import config
import contextlib
import uuid
from datetime import date, datetime, timedelta
from uuid import UUID

class Database:
    def __init__(self):
        self.connection_string = config.DATABASE_URL
    
    @contextlib.contextmanager
    def get_connection(self):
        """Context manager to handle database connections"""
        conn = psycopg2.connect(
            self.connection_string,
            cursor_factory=RealDictCursor
        )
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        """Initializes the database tables"""
        print("INFO: Database tables already exist, skipping table creation")
    
    def organization_exists(self, organization_name: str) -> bool:
        """Checks if an organization exists by name (case-insensitive)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.organizations WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))) AS exists",
                        (organization_name,)
                    )
                    result = cursor.fetchone()
                    return result['exists']
        except Exception as e:
            print(f"Error checking organization: {e}")
            return False
    
    def get_organization_id(self, organization_name: str) -> Optional[str]:
        """Gets the organization ID by name (case-insensitive with debug)"""
        try:
            print(f"DEBUG: Searching for organization: '{organization_name}'")
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id, name FROM public.organizations WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))",
                        (organization_name,)
                    )
                    result = cursor.fetchone()
                    
                    if result:
                        print(f"DEBUG: Organization found - ID: {result['id']}, Name: '{result['name']}'")
                        return result['id']
                    else:
                        cursor.execute("SELECT id, name FROM public.organizations")
                        all_orgs = cursor.fetchall()
                        print(f"DEBUG: Available organizations: {[dict(org) for org in all_orgs]}")
                        return None
        except Exception as e:
            print(f"Error fetching organization: {e}")
            return None

    
    def create_appointment(self, appointment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new appointment"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        INSERT INTO public.appointments (
                            id, organization_id, patient_id, doctor_id, user_id,
                            specialization, date_time, notes, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    cursor.execute(query, (
                        appointment_data['id'],
                        appointment_data['organization_id'],
                        appointment_data['patient_id'],
                        appointment_data['doctor_id'],
                        appointment_data['user_id'],
                        appointment_data['specialization'],
                        appointment_data['date_time'],
                        appointment_data.get('notes', ''),
                        appointment_data.get('status', 'scheduled'),
                        appointment_data.get('created_at', datetime.utcnow()),
                        appointment_data.get('updated_at', datetime.utcnow())
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating appointment: {e}")
            raise

    def get_appointment_by_id(self, appointment_id: UUID) -> Optional[Dict[str, Any]]:
        """Get appointment by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM appointments WHERE id = %s"
                    cursor.execute(query, (appointment_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching appointment: {e}")
            return None

    def update_appointment(self, appointment_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing appointment"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(appointment_id)
                    
                    query = f"""
                        UPDATE public.appointments 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating appointment: {e}")
            raise

    def delete_appointment(self, appointment_id: UUID) -> bool:
        """Delete an appointment"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "DELETE FROM appointments WHERE id = %s"
                    cursor.execute(query, (appointment_id,))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting appointment: {e}")
            return False

    def get_appointments_by_datetime(self, date_time: datetime) -> List[Dict[str, Any]]:
        """Find appointments by date and time"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.appointments WHERE date_time = %s"
                    cursor.execute(query, (date_time,))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching appointments by datetime: {e}")
            return []

    def get_appointments_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Find appointments within a date range"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.appointments 
                        WHERE date_time >= %s AND date_time <= %s
                        ORDER BY date_time
                    """
                    cursor.execute(query, (start_date, end_date))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching appointments by date range: {e}")
            return []

    def get_all_appointments(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all appointments with optional filtering and pagination"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    base_query = "SELECT * FROM public.appointments WHERE 1=1"
                    count_query = "SELECT COUNT(*) FROM public.appointments WHERE 1=1"
                    params = []
                    
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('patient_id'):
                            base_query += " AND patient_id = %s"
                            count_query += " AND patient_id = %s"
                            params.append(filters['patient_id'])
                        
                        if filters.get('doctor_id'):
                            base_query += " AND doctor_id = %s"
                            count_query += " AND doctor_id = %s"
                            params.append(filters['doctor_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                        
                        if filters.get('start_date'):
                            base_query += " AND date_time >= %s"
                            count_query += " AND date_time >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND date_time <= %s"
                            count_query += " AND date_time <= %s"
                            params.append(filters['end_date'])
                    
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY date_time LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all appointments: {e}")
            return [], 0

    def check_scheduling_conflict(self, doctor_id: UUID, date_time: datetime, exclude_appointment_id: Optional[UUID] = None) -> bool:
        """Check if doctor is already booked at the specified time"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT EXISTS (
                            SELECT 1 FROM public.appointments 
                            WHERE doctor_id = %s 
                            AND date_time = %s 
                            AND status IN ('scheduled', 'confirmed')
                        ) AS exists_conflict
                    """
                    params = [doctor_id, date_time]
                    
                    if exclude_appointment_id:
                        query = query.replace("WHERE", "WHERE id != %s AND")
                        params.insert(0, exclude_appointment_id)
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return result['exists_conflict']
        except Exception as e:
            print(f"Error checking scheduling conflict: {e}")
            return True  

    def validate_entities_exist(self, patient_id: UUID, doctor_id: UUID, organization_id: UUID) -> bool:
        """Validate that patient, doctor and organization exist"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM patients WHERE id = %s AND organization_id = %s) AS patient_exists",
                        (patient_id, organization_id)
                    )
                    patient_exists = cursor.fetchone()['patient_exists']
                    
                    
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM doctors WHERE id = %s AND organization_id = %s) AS doctor_exists",
                        (doctor_id, organization_id)
                    )
                    doctor_exists = cursor.fetchone()['doctor_exists']
                    
                    return patient_exists and doctor_exists
        except Exception as e:
            print(f"Error validating entities: {e}")
            return False

    def resolve_patient_id_by_name(self, name: str, organization_id: UUID) -> Optional[UUID]:
        """Resolve patient ID by name within an organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT id FROM public.patients 
                        WHERE name ILIKE %s AND organization_id = %s 
                        LIMIT 1
                    """
                    cursor.execute(query, (f"%{name}%", organization_id))
                    result = cursor.fetchone()
                    return result['id'] if result else None
        except Exception as e:
            print(f"Error resolving patient ID: {e}")
            return None

    def resolve_doctor_id_by_name(self, name: str, organization_id: UUID) -> Optional[UUID]:
        """Resolve doctor ID by name within an organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT id FROM public.doctors 
                        WHERE full_name ILIKE %s AND organization_id = %s 
                        LIMIT 1
                    """
                    cursor.execute(query, (f"%{name}%", organization_id))
                    result = cursor.fetchone()
                    return result['id'] if result else None
        except Exception as e:
            print(f"Error resolving doctor ID: {e}")
            return None

    def cancel_appointment(self, appointment_id: UUID, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Cancel an appointment"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.appointments 
                        SET status = 'cancelled', 
                            notes = CASE 
                                WHEN %s IS NOT NULL THEN CONCAT(COALESCE(notes, ''), '\nCancelled: ', %s)
                                ELSE notes
                            END,
                            updated_at = %s
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, (reason, reason, datetime.utcnow(), appointment_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error cancelling appointment: {e}")
            raise

    def confirm_appointment(self, appointment_id: UUID) -> Optional[Dict[str, Any]]:
        """Confirm an appointment"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.appointments 
                        SET status = 'confirmed', updated_at = %s
                        WHERE id = %s AND status = 'scheduled'
                        RETURNING *
                    """
                    cursor.execute(query, (datetime.utcnow(), appointment_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error confirming appointment: {e}")
            raise

    
    def get_patient_by_id(self, patient_id: UUID) -> Optional[Dict[str, Any]]:
        """Get patient by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM patients WHERE id = %s"
                    cursor.execute(query, (patient_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching patient: {e}")
            return None

    
    def get_doctor_by_id(self, doctor_id: UUID) -> Optional[Dict[str, Any]]:
        """Get doctor by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM doctors WHERE id = %s"
                    cursor.execute(query, (doctor_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching doctor: {e}")
            return None
        
        
    
    def create_charge(self, charge_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new charge"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        INSERT INTO public.charges (
                            id, amount, currency, description, payment_method,
                            status, stripe_id, organization_id, customer_id,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    cursor.execute(query, (
                        charge_data['id'],
                        charge_data['amount'],
                        charge_data['currency'],
                        charge_data.get('description', ''),
                        charge_data['payment_method'],
                        charge_data.get('status', 'pending'),
                        charge_data.get('stripe_id'),
                        charge_data['organization_id'],
                        charge_data['customer_id'],
                        charge_data.get('created_at', datetime.utcnow()),
                        charge_data.get('updated_at', datetime.utcnow())
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating charge: {e}")
            raise

    def get_charge_by_id(self, charge_id: UUID) -> Optional[Dict[str, Any]]:
        """Get charge by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.charges WHERE id = %s"
                    cursor.execute(query, (charge_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching charge: {e}")
            return None

    def update_charge(self, charge_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing charge"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(charge_id)
                    
                    query = f"""
                        UPDATE public.charges 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating charge: {e}")
            raise

    def get_charges_by_status(self, status: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get charges by status with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.charges WHERE status = %s"
                    count_query = "SELECT COUNT(*) FROM public.charges WHERE status = %s"
                    params = [status]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('customer_id'):
                            base_query += " AND customer_id = %s"
                            count_query += " AND customer_id = %s"
                            params.append(filters['customer_id'])
                        
                        if filters.get('payment_method'):
                            base_query += " AND payment_method = %s"
                            count_query += " AND payment_method = %s"
                            params.append(filters['payment_method'])
                        
                        if filters.get('currency'):
                            base_query += " AND currency = %s"
                            count_query += " AND currency = %s"
                            params.append(filters['currency'])
                        
                        if filters.get('start_date'):
                            base_query += " AND created_at >= %s"
                            count_query += " AND created_at >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND created_at <= %s"
                            count_query += " AND created_at <= %s"
                            params.append(filters['end_date'])
                        
                        if filters.get('min_amount'):
                            base_query += " AND amount >= %s"
                            count_query += " AND amount >= %s"
                            params.append(filters['min_amount'])
                        
                        if filters.get('max_amount'):
                            base_query += " AND amount <= %s"
                            count_query += " AND amount <= %s"
                            params.append(filters['max_amount'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching charges by status: {e}")
            return [], 0

    def process_charge_payment(self, charge_id: UUID, payment_method: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Process payment for a charge"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    query = """
                        UPDATE public.charges 
                        SET status = 'processing', 
                            payment_method = COALESCE(%s, payment_method),
                            updated_at = %s
                        WHERE id = %s AND status = 'pending'
                        RETURNING *
                    """
                    cursor.execute(query, (payment_method, datetime.utcnow(), charge_id))
                    result = cursor.fetchone()
                    
                    if result:
                        
                        import time
                        time.sleep(1)
                        
                        
                        query = """
                            UPDATE public.charges 
                            SET status = 'succeeded', updated_at = %s
                            WHERE id = %s
                            RETURNING *
                        """
                        cursor.execute(query, (datetime.utcnow(), charge_id))
                        result = cursor.fetchone()
                        conn.commit()
                        return dict(result) if result else None
                    else:
                        conn.rollback()
                        return None
        except Exception as e:
            print(f"Error processing charge payment: {e}")
            raise

    def cancel_charge(self, charge_id: UUID, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Cancel a charge"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.charges 
                        SET status = 'canceled', 
                            description = CASE 
                                WHEN %s IS NOT NULL THEN CONCAT(COALESCE(description, ''), ' [Cancelled: ', %s, ']')
                                ELSE description
                            END,
                            updated_at = %s
                        WHERE id = %s AND status IN ('pending', 'processing')
                        RETURNING *
                    """
                    cursor.execute(query, (reason, reason, datetime.utcnow(), charge_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error cancelling charge: {e}")
            raise

    def refund_charge(self, charge_id: UUID, amount: Optional[int] = None, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Refund a charge (full or partial)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # First get the charge to validate
                    cursor.execute("SELECT amount, status FROM public.charges WHERE id = %s", (charge_id,))
                    charge = cursor.fetchone()
                    
                    if not charge:
                        return None
                    
                    if charge['status'] != 'succeeded':
                        raise Exception("Cannot refund charge that is not succeeded")
                    
                    refund_amount = amount or charge['amount']
                    if refund_amount > charge['amount']:
                        raise Exception("Refund amount cannot exceed original charge amount")
                    
                    query = """
                        UPDATE public.charges 
                        SET status = 'refunded', 
                            description = CASE 
                                WHEN %s IS NOT NULL THEN CONCAT(COALESCE(description, ''), ' [Refunded: ', %s, ']')
                                ELSE description
                            END,
                            updated_at = %s
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, (reason, reason, datetime.utcnow(), charge_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error refunding charge: {e}")
            raise

    def get_all_charges(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all charges with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.charges WHERE 1=1"
                    count_query = "SELECT COUNT(*) FROM public.charges WHERE 1=1"
                    params = []
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('customer_id'):
                            base_query += " AND customer_id = %s"
                            count_query += " AND customer_id = %s"
                            params.append(filters['customer_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                        
                        if filters.get('payment_method'):
                            base_query += " AND payment_method = %s"
                            count_query += " AND payment_method = %s"
                            params.append(filters['payment_method'])
                        
                        if filters.get('currency'):
                            base_query += " AND currency = %s"
                            count_query += " AND currency = %s"
                            params.append(filters['currency'])
                        
                        if filters.get('start_date'):
                            base_query += " AND created_at >= %s"
                            count_query += " AND created_at >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND created_at <= %s"
                            count_query += " AND created_at <= %s"
                            params.append(filters['end_date'])
                        
                        if filters.get('min_amount'):
                            base_query += " AND amount >= %s"
                            count_query += " AND amount >= %s"
                            params.append(filters['min_amount'])
                        
                        if filters.get('max_amount'):
                            base_query += " AND amount <= %s"
                            count_query += " AND amount <= %s"
                            params.append(filters['max_amount'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all charges: {e}")
            return [], 0

    def get_charges_by_customer(self, customer_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get charges by customer"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.charges WHERE customer_id = %s"
                    count_query = "SELECT COUNT(*) FROM public.charges WHERE customer_id = %s"
                    params = [customer_id]
                    
                    if filters:
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                        
                        if filters.get('payment_method'):
                            base_query += " AND payment_method = %s"
                            count_query += " AND payment_method = %s"
                            params.append(filters['payment_method'])
                        
                        if filters.get('start_date'):
                            base_query += " AND created_at >= %s"
                            count_query += " AND created_at >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND created_at <= %s"
                            count_query += " AND created_at <= %s"
                            params.append(filters['end_date'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching charges by customer: {e}")
            return [], 0

    def get_charges_by_organization(self, organization_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get charges by organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.charges WHERE organization_id = %s"
                    count_query = "SELECT COUNT(*) FROM public.charges WHERE organization_id = %s"
                    params = [organization_id]
                    
                    if filters:
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                        
                        if filters.get('customer_id'):
                            base_query += " AND customer_id = %s"
                            count_query += " AND customer_id = %s"
                            params.append(filters['customer_id'])
                        
                        if filters.get('payment_method'):
                            base_query += " AND payment_method = %s"
                            count_query += " AND payment_method = %s"
                            params.append(filters['payment_method'])
                        
                        if filters.get('start_date'):
                            base_query += " AND created_at >= %s"
                            count_query += " AND created_at >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND created_at <= %s"
                            count_query += " AND created_at <= %s"
                            params.append(filters['end_date'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching charges by organization: {e}")
            return [], 0

    def validate_charge_entities(self, organization_id: UUID, customer_id: UUID) -> bool:
        """Validate that organization and customer exist"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.organizations WHERE id = %s) AS org_exists",
                        (organization_id,)
                    )
                    org_exists = cursor.fetchone()['org_exists']
                    
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.customers WHERE id = %s) AS customer_exists",
                        (customer_id,)
                    )
                    customer_exists = cursor.fetchone()['customer_exists']
                    
                    return org_exists and customer_exists
        except Exception as e:
            print(f"Error validating charge entities: {e}")
            return False

    def get_customer_by_id(self, customer_id: UUID) -> Optional[Dict[str, Any]]:
        """Get customer by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.customers WHERE id = %s"
                    cursor.execute(query, (customer_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching customer: {e}")
            return None
        
        
     
    def create_doctor(self, doctor_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new doctor"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # First check for duplicates
                    check_query = """
                        SELECT id FROM public.doctors 
                        WHERE (crm_registry = %s OR cpf = %s) 
                        AND deleted_at IS NULL 
                        LIMIT 1
                    """
                    cursor.execute(check_query, (doctor_data['crm_registry'], doctor_data['cpf']))
                    existing = cursor.fetchone()
                    
                    if existing:
                        raise Exception("Doctor with same CRM or CPF already exists")

                    query = """
                        INSERT INTO public.doctors (
                            id, organization_id, full_name, contact_phone, crm_registry,
                            specialization, address, identity, cpf, date_of_birth,
                            internal_id, npi, dea_registration, dea_issue_date,
                            dea_expiration_date, ssn, ein, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    doctor_id = doctor_data.get('id', uuid.uuid4())
                    now = datetime.utcnow()
                    
                    cursor.execute(query, (
                        doctor_id,
                        doctor_data.get('organization_id'),
                        doctor_data['full_name'],
                        doctor_data['contact_phone'],
                        doctor_data['crm_registry'],
                        doctor_data['specialization'],
                        doctor_data.get('address'),
                        doctor_data.get('identity'),
                        doctor_data['cpf'],
                        doctor_data.get('date_of_birth'),
                        doctor_data.get('internal_id'),
                        doctor_data.get('npi'),
                        doctor_data.get('dea_registration'),
                        doctor_data.get('dea_issue_date'),
                        doctor_data.get('dea_expiration_date'),
                        doctor_data.get('ssn'),
                        doctor_data.get('ein'),
                        now,
                        now
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating doctor: {e}")
            raise

    def get_doctor_by_id(self, doctor_id: UUID) -> Optional[Dict[str, Any]]:
        """Get doctor by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.doctors WHERE id = %s AND deleted_at IS NULL"
                    cursor.execute(query, (doctor_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching doctor: {e}")
            return None

    def get_doctor_by_crm_registry(self, crm_registry: str) -> Optional[Dict[str, Any]]:
        """Get doctor by CRM registry number (exact match)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.doctors WHERE crm_registry = %s AND deleted_at IS NULL"
                    cursor.execute(query, (crm_registry,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching doctor by CRM: {e}")
            return None

    def get_doctor_by_cpf(self, cpf: str) -> Optional[Dict[str, Any]]:
        """Get doctor by CPF (exact match)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.doctors WHERE cpf = %s AND deleted_at IS NULL"
                    cursor.execute(query, (cpf,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching doctor by CPF: {e}")
            return None

    def get_doctors_by_full_name(self, full_name: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get doctors by full name (partial match)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.doctors WHERE full_name ILIKE %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.doctors WHERE full_name ILIKE %s AND deleted_at IS NULL"
                    params = [f"%{full_name}%"]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY full_name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching doctors by name: {e}")
            return [], 0

    def get_doctors_by_specialization(self, specialization: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get doctors by specialization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.doctors WHERE specialization = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.doctors WHERE specialization = %s AND deleted_at IS NULL"
                    params = [specialization]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY full_name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching doctors by specialization: {e}")
            return [], 0

    def get_all_doctors(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all doctors with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.doctors WHERE deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.doctors WHERE deleted_at IS NULL"
                    params = []
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('specialization'):
                            base_query += " AND specialization = %s"
                            count_query += " AND specialization = %s"
                            params.append(filters['specialization'])
                        
                        if filters.get('created_after'):
                            base_query += " AND created_at >= %s"
                            count_query += " AND created_at >= %s"
                            params.append(filters['created_after'])
                        
                        if filters.get('created_before'):
                            base_query += " AND created_at <= %s"
                            count_query += " AND created_at <= %s"
                            params.append(filters['created_before'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY full_name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all doctors: {e}")
            return [], 0

    def get_doctors_by_organization(self, organization_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get doctors by organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.doctors WHERE organization_id = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.doctors WHERE organization_id = %s AND deleted_at IS NULL"
                    params = [organization_id]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY full_name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching doctors by organization: {e}")
            return [], 0

    def search_doctors(self, search_query: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Search doctors by multiple criteria"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT * FROM public.doctors 
                        WHERE deleted_at IS NULL AND (
                            full_name ILIKE %s OR
                            specialization ILIKE %s OR
                            crm_registry ILIKE %s OR
                            cpf ILIKE %s OR
                            internal_id ILIKE %s
                        )
                    """
                    count_query = """
                        SELECT COUNT(*) FROM public.doctors 
                        WHERE deleted_at IS NULL AND (
                            full_name ILIKE %s OR
                            specialization ILIKE %s OR
                            crm_registry ILIKE %s OR
                            cpf ILIKE %s OR
                            internal_id ILIKE %s
                        )
                    """
                    search_param = f"%{search_query}%"
                    params = [search_param, search_param, search_param, search_param, search_param]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY full_name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error searching doctors: {e}")
            return [], 0

    def update_doctor(self, doctor_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing doctor"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(doctor_id)
                    
                    query = f"""
                        UPDATE public.doctors 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s AND deleted_at IS NULL
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating doctor: {e}")
            raise

    def delete_doctor(self, doctor_id: UUID) -> bool:
        """Soft delete a doctor"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.doctors 
                        SET deleted_at = %s 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (datetime.utcnow(), doctor_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting doctor: {e}")
            return False

    def verify_doctor_license(self, crm_registry: str, full_name: str) -> Optional[Dict[str, Any]]:
        """Verify doctor's license with regulatory body"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT id, specialization FROM public.doctors 
                        WHERE crm_registry = %s AND full_name = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (crm_registry, full_name))
                    result = cursor.fetchone()
                    
                    if result:
                        return {
                            "status": "verified",
                            "crm_registry": crm_registry,
                            "full_name": full_name,
                            "specialization": result['specialization'],
                            "verification_date": datetime.utcnow().isoformat()
                        }
                    else:
                        return None
        except Exception as e:
            print(f"Error verifying doctor license: {e}")
            return None

    def check_dea_validity(self, doctor_id: UUID) -> Optional[Dict[str, Any]]:
        """Check DEA registration validity"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT dea_registration, dea_expiration_date 
                        FROM public.doctors 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (doctor_id,))
                    result = cursor.fetchone()
                    
                    if not result:
                        return None
                    
                    dea_registration = result['dea_registration']
                    dea_expiration_date = result['dea_expiration_date']
                    
                    if not dea_registration or not dea_expiration_date:
                        return {"status": "not_registered"}
                    
                    today = date.today()
                    is_valid = dea_expiration_date > today
                    
                    return {
                        "status": "valid" if is_valid else "expired",
                        "dea_registration": dea_registration,
                        "expiration_date": dea_expiration_date,
                        "days_until_expiry": (dea_expiration_date - today).days if is_valid else 0
                    }
        except Exception as e:
            print(f"Error checking DEA validity: {e}")
            return None

    def update_dea_registration(self, doctor_id: UUID, dea_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update DEA registration information"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.doctors 
                        SET dea_registration = %s,
                            dea_issue_date = %s,
                            dea_expiration_date = %s,
                            updated_at = %s
                        WHERE id = %s AND deleted_at IS NULL
                        RETURNING *
                    """
                    cursor.execute(query, (
                        dea_data.get('dea_registration'),
                        dea_data.get('dea_issue_date'),
                        dea_data.get('dea_expiration_date'),
                        datetime.utcnow(),
                        doctor_id
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating DEA registration: {e}")
            raise

    def get_specializations(self) -> List[str]:
        """Get list of all available specializations"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT DISTINCT specialization 
                        FROM public.doctors 
                        WHERE specialization IS NOT NULL AND deleted_at IS NULL
                        ORDER BY specialization
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return [row['specialization'] for row in results]
        except Exception as e:
            print(f"Error fetching specializations: {e}")
            return []

    def validate_doctor_credentials(self, doctor_id: UUID, credentials: Dict[str, Any]) -> bool:
        """Validate doctor's professional credentials"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT cpf, crm_registry, identity 
                        FROM public.doctors 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (doctor_id,))
                    result = cursor.fetchone()
                    
                    if not result:
                        return False
                    
                    
                    if 'cpf' in credentials and credentials['cpf'] != result['cpf']:
                        return False
                    if 'crm_registry' in credentials and credentials['crm_registry'] != result['crm_registry']:
                        return False
                    if 'identity' in credentials and credentials['identity'] != result['identity']:
                        return False
                        
                    return True
        except Exception as e:
            print(f"Error validating doctor credentials: {e}")
            return False
        
        
    
    def create_log(self, log_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new log entry"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Validações
                    if not log_data.get('service_name') or not log_data['service_name'].strip():
                        raise ValueError("Service name cannot be empty")

                    valid_statuses = {"success", "error", "pending"}
                    status = log_data.get('status')
                    if status not in valid_statuses:
                        raise ValueError(f"Invalid status: {status}. Must be one of: {valid_statuses}")

                    if log_data.get('log_description') and len(log_data['log_description']) > 10000:
                        raise ValueError("Log description is too long (max 10000 characters)")

                    if log_data.get('error_details') and len(log_data['error_details']) > 10000:
                        raise ValueError("Error details are too long (max 10000 characters)")

                    query = """
                        INSERT INTO public.logs (
                            id, service_name, start_date, start_times, duration_ms,
                            status, log_description, error_details
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    log_id = log_data.get('id', uuid.uuid4())
                    start_date = log_data.get('start_date', datetime.utcnow())
                    
                    cursor.execute(query, (
                        log_id,
                        log_data['service_name'],
                        start_date,
                        log_data.get('start_times', 0),
                        log_data.get('duration_ms', 0),
                        status,
                        log_data.get('log_description'),
                        log_data.get('error_details')
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating log: {e}")
            raise

    def get_log_by_id(self, log_id: UUID) -> Optional[Dict[str, Any]]:
        """Get log by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.logs WHERE id = %s"
                    cursor.execute(query, (log_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching log: {e}")
            return None

    def get_logs_by_service(self, service_name: str) -> List[Dict[str, Any]]:
        """Get logs by service name"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not service_name or not service_name.strip():
                        raise ValueError("Service name cannot be empty")

                    query = """
                        SELECT * FROM public.logs 
                        WHERE service_name = %s 
                        ORDER BY start_date DESC
                    """
                    cursor.execute(query, (service_name,))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching logs by service: {e}")
            return []

    def get_logs_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get logs by status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    valid_statuses = {"success", "error", "pending"}
                    if status not in valid_statuses:
                        raise ValueError(f"Invalid status: {status}. Must be one of: {valid_statuses}")

                    query = """
                        SELECT * FROM public.logs 
                        WHERE status = %s 
                        ORDER BY start_date DESC
                    """
                    cursor.execute(query, (status,))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching logs by status: {e}")
            return []

    def get_logs_by_service_name(self, service_name: str) -> List[Dict[str, Any]]:
        """Get logs by service name (alias for get_logs_by_service)"""
        return self.get_logs_by_service(service_name)

    def update_log(self, log_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing log"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar se o log existe
                    existing_log = self.get_log_by_id(log_id)
                    if not existing_log:
                        raise ValueError(f"Log not found with ID: {log_id}")
                    
                    # Validações
                    if "status" in update_data:
                        valid_statuses = {"success", "error", "pending"}
                        if update_data["status"] not in valid_statuses:
                            raise ValueError(f"Invalid status: {update_data['status']}")
                    
                    if "log_description" in update_data and update_data["log_description"]:
                        if len(update_data["log_description"]) > 10000:
                            raise ValueError("Log description is too long")
                    
                    if "error_details" in update_data and update_data["error_details"]:
                        if len(update_data["error_details"]) > 10000:
                            raise ValueError("Error details are too long")
                    
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return existing_log
                    
                    params.append(log_id)
                    
                    query = f"""
                        UPDATE public.logs 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating log: {e}")
            raise

    def delete_log_by_id(self, log_id: UUID) -> bool:
        """Delete log by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar se o log existe
                    existing_log = self.get_log_by_id(log_id)
                    if not existing_log:
                        raise ValueError(f"Log not found with ID: {log_id}")
                    
                    query = "DELETE FROM public.logs WHERE id = %s"
                    cursor.execute(query, (log_id,))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting log: {e}")
            return False

    def get_logs_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get logs within a date range"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if start_date > end_date:
                        raise ValueError("Start date cannot be after end date")

                    query = """
                        SELECT * FROM public.logs 
                        WHERE start_date BETWEEN %s AND %s 
                        ORDER BY start_date DESC
                    """
                    cursor.execute(query, (start_date, end_date))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching logs by date range: {e}")
            return []

    def get_logs_by_service_and_status(self, service_name: str, status: str) -> List[Dict[str, Any]]:
        """Get logs by service name and status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not service_name or not service_name.strip():
                        raise ValueError("Service name cannot be empty")

                    valid_statuses = {"success", "error", "pending"}
                    if status not in valid_statuses:
                        raise ValueError(f"Invalid status: {status}")

                    query = """
                        SELECT * FROM public.logs 
                        WHERE service_name = %s AND status = %s 
                        ORDER BY start_date DESC
                    """
                    cursor.execute(query, (service_name, status))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching logs by service and status: {e}")
            return []

    def get_error_logs(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get error logs with optional limit"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.logs 
                        WHERE status = 'error' 
                        ORDER BY start_date DESC
                    """
                    
                    if limit:
                        query += " LIMIT %s"
                        cursor.execute(query, (limit,))
                    else:
                        cursor.execute(query)
                    
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching error logs: {e}")
            return []

    def get_service_statistics(self, service_name: str) -> Dict[str, Any]:
        """Get statistics for a service"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not service_name or not service_name.strip():
                        raise ValueError("Service name cannot be empty")

                    query = """
                        SELECT 
                            COUNT(*) as total_logs,
                            COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
                            COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
                            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count,
                            AVG(duration_ms) as avg_duration_ms,
                            MAX(start_date) as last_log_date
                        FROM public.logs 
                        WHERE service_name = %s
                    """
                    cursor.execute(query, (service_name,))
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching service statistics: {e}")
            return {}

    def get_high_duration_logs(self, threshold_ms: int) -> List[Dict[str, Any]]:
        """Get logs with duration above threshold"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.logs 
                        WHERE duration_ms > %s 
                        ORDER BY duration_ms DESC
                    """
                    cursor.execute(query, (threshold_ms,))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching high duration logs: {e}")
            return []

    def cleanup_old_logs(self, older_than_days: int) -> int:
        """Clean up logs older than specified days"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        DELETE FROM public.logs 
                        WHERE start_date < NOW() - INTERVAL '%s days'
                    """
                    cursor.execute(query, (older_than_days,))
                    deleted_count = cursor.rowcount
                    conn.commit()
                    return deleted_count
        except Exception as e:
            print(f"Error cleaning up old logs: {e}")
            return 0

    def get_all_logs(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all logs with optional limit"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.logs ORDER BY start_date DESC"
                    
                    if limit:
                        query += " LIMIT %s"
                        cursor.execute(query, (limit,))
                    else:
                        cursor.execute(query)
                    
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching all logs: {e}")
            return []

    def get_logs_summary(self) -> Dict[str, Any]:
        """Get summary statistics for all logs"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_logs,
                            COUNT(DISTINCT service_name) as total_services,
                            COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
                            COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
                            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count,
                            AVG(duration_ms) as avg_duration_ms,
                            MAX(start_date) as latest_log_date,
                            MIN(start_date) as earliest_log_date
                        FROM public.logs
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching logs summary: {e}")
            return {}

    def get_service_names(self) -> List[str]:
        """Get list of all service names"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT DISTINCT service_name 
                        FROM public.logs 
                        WHERE service_name IS NOT NULL 
                        ORDER BY service_name
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return [row['service_name'] for row in results]
        except Exception as e:
            print(f"Error fetching service names: {e}")
            return []
        
    
    def create_medical_record(self, medical_record_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new medical record"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    if not medical_record_data.get('patient_id'):
                        raise ValueError("Patient ID is required")
                    
                    if not medical_record_data.get('doctor_id'):
                        raise ValueError("Doctor ID is required")
                    
                    if not medical_record_data.get('diagnosis') or not medical_record_data['diagnosis'].strip():
                        raise ValueError("Diagnosis is required")

                    
                    cursor.execute(
                        "SELECT id FROM public.patients WHERE id = %s",
                        (medical_record_data['patient_id'],)
                    )
                    if not cursor.fetchone():
                        raise ValueError(f"Patient with ID {medical_record_data['patient_id']} not found")

                    
                    cursor.execute(
                        "SELECT id FROM public.doctors WHERE id = %s",
                        (medical_record_data['doctor_id'],)
                    )
                    if not cursor.fetchone():
                        raise ValueError(f"Doctor with ID {medical_record_data['doctor_id']} not found")

                    query = """
                        INSERT INTO public.medical_records 
                        (id, patient_id, doctor_id, diagnosis, treatment, notes, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    record_id = medical_record_data.get('id', uuid.uuid4())
                    now = datetime.utcnow()
                    
                    cursor.execute(query, (
                        record_id,
                        medical_record_data['patient_id'],
                        medical_record_data['doctor_id'],
                        medical_record_data['diagnosis'],
                        medical_record_data.get('treatment'),
                        medical_record_data.get('notes'),
                        now,
                        now
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating medical record: {e}")
            raise

    def get_medical_record_by_id(self, medical_record_id: UUID) -> Optional[Dict[str, Any]]:
        """Get medical record by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name, d.full_name AS doctor_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        JOIN public.doctors d ON mr.doctor_id = d.id
                        WHERE mr.id = %s
                    """
                    cursor.execute(query, (medical_record_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching medical record: {e}")
            return None

    def get_medical_records_by_patient_name(self, patient_name: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get medical records by patient name"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not patient_name or not patient_name.strip():
                        raise ValueError("Patient name cannot be empty")

                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE LOWER(p.name) LIKE LOWER(%s)
                    """
                    count_query = """
                        SELECT COUNT(*) 
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE LOWER(p.name) LIKE LOWER(%s)
                    """
                    params = [f"%{patient_name}%"]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching medical records by patient name: {e}")
            return [], 0

    def get_medical_records_by_patient_id(self, patient_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get medical records by patient ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE mr.patient_id = %s
                    """
                    count_query = "SELECT COUNT(*) FROM public.medical_records WHERE patient_id = %s"
                    params = [patient_id]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching medical records by patient ID: {e}")
            return [], 0

    def get_medical_records_by_doctor_id(self, doctor_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get medical records by doctor ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE mr.doctor_id = %s
                    """
                    count_query = "SELECT COUNT(*) FROM public.medical_records WHERE doctor_id = %s"
                    params = [doctor_id]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching medical records by doctor ID: {e}")
            return [], 0

    def get_medical_records_by_created_at(self, created_at: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get medical records by creation date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE DATE(mr.created_at) = %s
                    """
                    count_query = "SELECT COUNT(*) FROM public.medical_records WHERE DATE(created_at) = %s"
                    params = [created_at]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching medical records by creation date: {e}")
            return [], 0

    def get_medical_records_by_updated_at(self, updated_at: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get medical records by last update date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE DATE(mr.updated_at) = %s
                    """
                    count_query = "SELECT COUNT(*) FROM public.medical_records WHERE DATE(updated_at) = %s"
                    params = [updated_at]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.updated_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching medical records by update date: {e}")
            return [], 0

    def get_all_medical_records(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all medical records with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                    """
                    count_query = "SELECT COUNT(*) FROM public.medical_records"
                    
                    cursor.execute(count_query)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.created_at DESC LIMIT %s OFFSET %s"
                    params = [size, offset]
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all medical records: {e}")
            return [], 0

    def update_medical_record(self, medical_record_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing medical record"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(medical_record_id)
                    
                    query = f"""
                        UPDATE public.medical_records 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating medical record: {e}")
            raise

    def delete_medical_record(self, medical_record_id: UUID) -> bool:
        """Delete a medical record"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "DELETE FROM public.medical_records WHERE id = %s"
                    cursor.execute(query, (medical_record_id,))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting medical record: {e}")
            return False

    def search_medical_records(self, search_query: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Search medical records by diagnosis, treatment, notes, etc."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        WHERE mr.diagnosis ILIKE %s OR mr.treatment ILIKE %s OR mr.notes ILIKE %s
                    """
                    count_query = """
                        SELECT COUNT(*) 
                        FROM public.medical_records 
                        WHERE diagnosis ILIKE %s OR treatment ILIKE %s OR notes ILIKE %s
                    """
                    search_param = f"%{search_query}%"
                    params = [search_param, search_param, search_param]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY mr.created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error searching medical records: {e}")
            return [], 0

    def get_patient_medical_history(self, patient_id: UUID, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get complete medical history for a patient"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            mr.id, mr.patient_id, mr.doctor_id, mr.diagnosis, mr.treatment, mr.notes,
                            mr.created_at, mr.updated_at, p.name AS patient_name, d.full_name AS doctor_name
                        FROM public.medical_records mr
                        JOIN public.patients p ON mr.patient_id = p.id
                        JOIN public.doctors d ON mr.doctor_id = d.id
                        WHERE mr.patient_id = %s
                    """
                    count_query = "SELECT COUNT(*) FROM public.medical_records WHERE patient_id = %s"
                    params = [patient_id]
                    
                    if start_date:
                        base_query += " AND mr.created_at >= %s"
                        count_query += " AND created_at >= %s"
                        params.append(start_date)
                    
                    if end_date:
                        base_query += " AND mr.created_at <= %s"
                        count_query += " AND created_at <= %s"
                        params.append(end_date)
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    base_query += " ORDER BY mr.created_at DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching patient medical history: {e}")
            return [], 0

    def get_medical_record_statistics(self, organization_id: Optional[UUID] = None, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[str, Any]:
        """Get medical record statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT patient_id) as unique_patients,
                            COUNT(DISTINCT doctor_id) as unique_doctors,
                            AVG(LENGTH(diagnosis)) as avg_diagnosis_length,
                            MAX(created_at) as latest_record_date,
                            MIN(created_at) as earliest_record_date
                        FROM public.medical_records
                        WHERE 1=1
                    """
                    params = []
                    
                    if organization_id:
                        query += " AND EXISTS (SELECT 1 FROM public.patients p WHERE p.id = patient_id AND p.organization_id = %s)"
                        params.append(organization_id)
                    
                    if start_date:
                        query += " AND created_at >= %s"
                        params.append(start_date)
                    
                    if end_date:
                        query += " AND created_at <= %s"
                        params.append(end_date)
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching medical record statistics: {e}")
            return {}

    def log_medical_record_action(self, user_id: UUID, action: str, details: str, medical_record_id: Optional[UUID] = None) -> bool:
        """Log action to audit trail for medical records"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        INSERT INTO public.logs (id, user_id, action, details, timestamp, medical_record_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(query, (
                        uuid.uuid4(),
                        user_id,
                        action,
                        details,
                        datetime.utcnow(),
                        medical_record_id
                    ))
                    conn.commit()
                    return True
        except Exception as e:
            print(f"Error logging medical record action: {e}")
            return False

    def validate_medical_record_entities(self, patient_id: UUID, doctor_id: UUID) -> bool:
        """Validate that patient and doctor exist"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.patients WHERE id = %s) AS patient_exists",
                        (patient_id,)
                    )
                    patient_exists = cursor.fetchone()['patient_exists']
                    
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.doctors WHERE id = %s) AS doctor_exists",
                        (doctor_id,)
                    )
                    doctor_exists = cursor.fetchone()['doctor_exists']
                    
                    return patient_exists and doctor_exists
        except Exception as e:
            print(f"Error validating medical record entities: {e}")
            return False
        
     
    def create_patient(self, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new patient"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    if not patient_data.get('name') or not patient_data['name'].strip():
                        raise ValueError("Patient name cannot be empty")

                    if not patient_data.get('organization_id'):
                        raise ValueError("Organization ID is required")

                    
                    cursor.execute(
                        "SELECT id FROM public.organizations WHERE id = %s AND deleted_at IS NULL",
                        (patient_data['organization_id'],)
                    )
                    if not cursor.fetchone():
                        raise ValueError(f"Organization with ID {patient_data['organization_id']} not found")

                    
                    if patient_data.get('cpf'):
                        cursor.execute(
                            "SELECT id FROM public.patients WHERE cpf = %s AND deleted_at IS NULL",
                            (patient_data['cpf'],)
                        )
                        if cursor.fetchone():
                            raise ValueError(f"Patient with CPF {patient_data['cpf']} already exists")

                    
                    if patient_data.get('ssn'):
                        cursor.execute(
                            "SELECT id FROM public.patients WHERE ssn = %s AND deleted_at IS NULL",
                            (patient_data['ssn'],)
                        )
                        if cursor.fetchone():
                            raise ValueError(f"Patient with SSN {patient_data['ssn']} already exists")

                    query = """
                        INSERT INTO public.patients (
                            id, organization_id, cpf, ssn, name, dob, gender, address, contact,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    patient_id = patient_data.get('id', uuid.uuid4())
                    now = datetime.utcnow()
                    
                    cursor.execute(query, (
                        patient_id,
                        patient_data['organization_id'],
                        patient_data.get('cpf'),
                        patient_data.get('ssn'),
                        patient_data['name'],
                        patient_data.get('dob'),
                        patient_data.get('gender'),
                        patient_data.get('address'),
                        patient_data.get('contact'),
                        now,
                        now
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating patient: {e}")
            raise

    def get_patient_by_id(self, patient_id: UUID) -> Optional[Dict[str, Any]]:
        """Get patient by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.patients WHERE id = %s AND deleted_at IS NULL"
                    cursor.execute(query, (patient_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching patient: {e}")
            return None

    def get_patient_by_cpf(self, cpf: str) -> Optional[Dict[str, Any]]:
        """Get patient by CPF"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not cpf or not cpf.strip():
                        raise ValueError("CPF cannot be empty")

                    query = "SELECT * FROM public.patients WHERE cpf = %s AND deleted_at IS NULL"
                    cursor.execute(query, (cpf,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching patient by CPF: {e}")
            return None

    def get_patient_by_ssn(self, ssn: str) -> Optional[Dict[str, Any]]:
        """Get patient by SSN"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not ssn or not ssn.strip():
                        raise ValueError("SSN cannot be empty")

                    query = "SELECT * FROM public.patients WHERE ssn = %s AND deleted_at IS NULL"
                    cursor.execute(query, (ssn,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching patient by SSN: {e}")
            return None

    def get_patients_by_name(self, name: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get patients by name"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not name or not name.strip():
                        raise ValueError("Patient name cannot be empty")

                    base_query = "SELECT * FROM public.patients WHERE name ILIKE %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.patients WHERE name ILIKE %s AND deleted_at IS NULL"
                    params = [f"%{name}%"]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching patients by name: {e}")
            return [], 0

    def get_patients_by_dob(self, dob: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get patients by date of birth"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.patients WHERE dob = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.patients WHERE dob = %s AND deleted_at IS NULL"
                    params = [dob]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching patients by DOB: {e}")
            return [], 0

    def get_patients_by_created_at(self, created_at: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get patients by creation date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.patients WHERE DATE(created_at) = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.patients WHERE DATE(created_at) = %s AND deleted_at IS NULL"
                    params = [created_at]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching patients by creation date: {e}")
            return [], 0

    def get_patients_by_updated_at(self, updated_at: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get patients by last update date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.patients WHERE DATE(updated_at) = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.patients WHERE DATE(updated_at) = %s AND deleted_at IS NULL"
                    params = [updated_at]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY updated_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching patients by update date: {e}")
            return [], 0

    def get_all_patients(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all patients with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.patients WHERE deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.patients WHERE deleted_at IS NULL"
                    params = []
                    
                    if filters and filters.get('organization_id'):
                        base_query += " AND organization_id = %s"
                        count_query += " AND organization_id = %s"
                        params.append(filters['organization_id'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all patients: {e}")
            return [], 0

    def get_patients_by_organization(self, organization_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get patients by organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.patients WHERE organization_id = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.patients WHERE organization_id = %s AND deleted_at IS NULL"
                    params = [organization_id]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching patients by organization: {e}")
            return [], 0

    def search_patients(self, search_query: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Search patients by multiple criteria"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not search_query or not search_query.strip():
                        raise ValueError("Search query cannot be empty")

                    base_query = """
                        SELECT * FROM public.patients 
                        WHERE deleted_at IS NULL AND (
                            name ILIKE %s 
                            OR cpf ILIKE %s 
                            OR ssn ILIKE %s 
                            OR contact ILIKE %s 
                            OR address ILIKE %s
                        )
                    """
                    count_query = """
                        SELECT COUNT(*) FROM public.patients 
                        WHERE deleted_at IS NULL AND (
                            name ILIKE %s 
                            OR cpf ILIKE %s 
                            OR ssn ILIKE %s 
                            OR contact ILIKE %s 
                            OR address ILIKE %s
                        )
                    """
                    search_param = f"%{search_query}%"
                    params = [search_param, search_param, search_param, search_param, search_param]
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY name LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error searching patients: {e}")
            return [], 0

    def update_patient(self, patient_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing patient"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    if 'cpf' in update_data and update_data['cpf']:
                        cursor.execute(
                            "SELECT id FROM public.patients WHERE cpf = %s AND id != %s AND deleted_at IS NULL",
                            (update_data['cpf'], patient_id)
                        )
                        if cursor.fetchone():
                            raise ValueError(f"Patient with CPF {update_data['cpf']} already exists")

                    
                    if 'ssn' in update_data and update_data['ssn']:
                        cursor.execute(
                            "SELECT id FROM public.patients WHERE ssn = %s AND id != %s AND deleted_at IS NULL",
                            (update_data['ssn'], patient_id)
                        )
                        if cursor.fetchone():
                            raise ValueError(f"Patient with SSN {update_data['ssn']} already exists")

                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(patient_id)
                    
                    query = f"""
                        UPDATE public.patients 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s AND deleted_at IS NULL
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating patient: {e}")
            raise

    def delete_patient(self, patient_id: UUID) -> bool:
        """Soft delete a patient"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar se existem registros médicos associados
                    cursor.execute(
                        "SELECT COUNT(*) FROM public.medical_records WHERE patient_id = %s",
                        (patient_id,)
                    )
                    medical_records_count = cursor.fetchone()['count']
                    
                    if medical_records_count > 0:
                        raise ValueError("Cannot delete patient with associated medical records")

                    # Verificar se existem agendamentos associados
                    cursor.execute(
                        "SELECT COUNT(*) FROM public.appointments WHERE patient_id = %s",
                        (patient_id,)
                    )
                    appointments_count = cursor.fetchone()['count']
                    
                    if appointments_count > 0:
                        raise ValueError("Cannot delete patient with associated appointments")

                    query = """
                        UPDATE public.patients 
                        SET deleted_at = %s 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (datetime.utcnow(), patient_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting patient: {e}")
            return False

    def validate_cpf_availability(self, cpf: str, exclude_patient_id: Optional[UUID] = None) -> bool:
        """Validate if CPF is available"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not cpf or not cpf.strip():
                        raise ValueError("CPF cannot be empty")

                    query = "SELECT id FROM public.patients WHERE cpf = %s AND deleted_at IS NULL"
                    params = [cpf]
                    
                    if exclude_patient_id:
                        query += " AND id != %s"
                        params.append(exclude_patient_id)
                    
                    cursor.execute(query, params)
                    return cursor.fetchone() is None
        except Exception as e:
            print(f"Error validating CPF availability: {e}")
            return False

    def validate_ssn_availability(self, ssn: str, exclude_patient_id: Optional[UUID] = None) -> bool:
        """Validate if SSN is available"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if not ssn or not ssn.strip():
                        raise ValueError("SSN cannot be empty")

                    query = "SELECT id FROM public.patients WHERE ssn = %s AND deleted_at IS NULL"
                    params = [ssn]
                    
                    if exclude_patient_id:
                        query += " AND id != %s"
                        params.append(exclude_patient_id)
                    
                    cursor.execute(query, params)
                    return cursor.fetchone() is None
        except Exception as e:
            print(f"Error validating SSN availability: {e}")
            return False

    def get_patient_statistics(self, organization_id: Optional[UUID] = None) -> Dict[str, Any]:
        """Get patient statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_patients,
                            COUNT(CASE WHEN gender = 'MALE' THEN 1 END) as male_count,
                            COUNT(CASE WHEN gender = 'FEMALE' THEN 1 END) as female_count,
                            COUNT(CASE WHEN gender = 'OTHER' THEN 1 END) as other_count,
                            COUNT(CASE WHEN gender IS NULL THEN 1 END) as unknown_gender_count,
                            COUNT(CASE WHEN dob IS NOT NULL AND EXTRACT(YEAR FROM AGE(dob)) < 18 THEN 1 END) as under_18_count,
                            COUNT(CASE WHEN dob IS NOT NULL AND EXTRACT(YEAR FROM AGE(dob)) BETWEEN 18 AND 65 THEN 1 END) as adult_count,
                            COUNT(CASE WHEN dob IS NOT NULL AND EXTRACT(YEAR FROM AGE(dob)) > 65 THEN 1 END) as senior_count,
                            AVG(EXTRACT(YEAR FROM AGE(dob))) as average_age,
                            MAX(created_at) as last_patient_created
                        FROM public.patients 
                        WHERE deleted_at IS NULL
                    """
                    params = []
                    
                    if organization_id:
                        query += " AND organization_id = %s"
                        params.append(organization_id)
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching patient statistics: {e}")
            return {}

    def get_patient_medical_history(self, patient_id: UUID) -> List[Dict[str, Any]]:
        """Get patient's complete medical history"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            mr.id,
                            mr.diagnosis,
                            mr.treatment,
                            mr.notes,
                            mr.created_at as record_date,
                            d.full_name as doctor_name,
                            d.specialization
                        FROM public.medical_records mr
                        JOIN public.doctors d ON mr.doctor_id = d.id
                        WHERE mr.patient_id = %s
                        ORDER BY mr.created_at DESC
                    """
                    cursor.execute(query, (patient_id,))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching patient medical history: {e}")
            return []

    def get_patient_appointments(self, patient_id: UUID, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Get patient's appointments"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT 
                            a.id,
                            a.date_time as appointment_date,
                            a.status,
                            a.notes,
                            d.full_name as doctor_name,
                            d.specialization,
                            o.name as organization_name
                        FROM public.appointments a
                        JOIN public.doctors d ON a.doctor_id = d.id
                        JOIN public.organizations o ON a.organization_id = o.id
                        WHERE a.patient_id = %s
                    """
                    params = [patient_id]
                    
                    if filters and filters.get('status'):
                        base_query += " AND a.status = %s"
                        params.append(filters['status'])
                    
                    if filters and filters.get('start_date'):
                        base_query += " AND a.date_time >= %s"
                        params.append(filters['start_date'])
                    
                    if filters and filters.get('end_date'):
                        base_query += " AND a.date_time <= %s"
                        params.append(filters['end_date'])
                    
                    base_query += " ORDER BY a.date_time DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching patient appointments: {e}")
            return []

    def calculate_patient_age(self, date_of_birth: date) -> int:
        """Calculate patient age from date of birth"""
        try:
            today = date.today()
            age = today.year - date_of_birth.year
            
            # Ajustar se ainda não fez aniversário este ano
            if today.month < date_of_birth.month or (today.month == date_of_birth.month and today.day < date_of_birth.day):
                age -= 1
                
            return age
        except Exception as e:
            print(f"Error calculating patient age: {e}")
            return 0

    def merge_patient_records(self, primary_patient_id: UUID, duplicate_patient_id: UUID) -> bool:
        """Merge duplicate patient records"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar se os pacientes são da mesma organização
                    cursor.execute(
                        "SELECT organization_id FROM public.patients WHERE id IN (%s, %s)",
                        (primary_patient_id, duplicate_patient_id)
                    )
                    organizations = cursor.fetchall()
                    
                    if len(organizations) != 2 or organizations[0]['organization_id'] != organizations[1]['organization_id']:
                        raise ValueError("Cannot merge patients from different organizations")

                    
                    cursor.execute(
                        "UPDATE public.medical_records SET patient_id = %s WHERE patient_id = %s",
                        (primary_patient_id, duplicate_patient_id)
                    )

                    
                    cursor.execute(
                        "UPDATE public.appointments SET patient_id = %s WHERE patient_id = %s",
                        (primary_patient_id, duplicate_patient_id)
                    )

                    
                    cursor.execute(
                        "UPDATE public.patients SET deleted_at = %s WHERE id = %s",
                        (datetime.utcnow(), duplicate_patient_id)
                    )

                    conn.commit()
                    return True
        except Exception as e:
            print(f"Error merging patient records: {e}")
            return False

    def get_patient_dashboard_data(self, patient_id: UUID) -> Dict[str, Any]:
        """Get patient dashboard data"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    cursor.execute(
                        "SELECT * FROM public.patients WHERE id = %s AND deleted_at IS NULL",
                        (patient_id,)
                    )
                    patient_info = cursor.fetchone()

                    if not patient_info:
                        raise ValueError(f"Patient with ID {patient_id} not found")

                    
                    age = 0
                    if patient_info['dob']:
                        age = self.calculate_patient_age(patient_info['dob'])

                    
                    cursor.execute(
                        """
                        SELECT a.id, a.date_time, a.status, d.full_name as doctor_name
                        FROM public.appointments a
                        JOIN public.doctors d ON a.doctor_id = d.id
                        WHERE a.patient_id = %s
                        ORDER BY a.date_time DESC
                        LIMIT 5
                        """,
                        (patient_id,)
                    )
                    recent_appointments = cursor.fetchall()

                    
                    cursor.execute(
                        """
                        SELECT mr.id, mr.diagnosis, mr.created_at, d.full_name as doctor_name
                        FROM public.medical_records mr
                        JOIN public.doctors d ON mr.doctor_id = d.id
                        WHERE mr.patient_id = %s
                        ORDER BY mr.created_at DESC
                        LIMIT 3
                        """,
                        (patient_id,)
                    )
                    medical_history = cursor.fetchall()

                    
                    cursor.execute(
                        """
                        SELECT a.id, a.date_time, d.full_name as doctor_name
                        FROM public.appointments a
                        JOIN public.doctors d ON a.doctor_id = d.id
                        WHERE a.patient_id = %s AND a.status = 'scheduled' AND a.date_time >= %s
                        ORDER BY a.date_time ASC
                        LIMIT 3
                        """,
                        (patient_id, datetime.utcnow())
                    )
                    upcoming_appointments = cursor.fetchall()

                    return {
                        "patient_info": dict(patient_info),
                        "age": age,
                        "recent_appointments": [dict(appt) for appt in recent_appointments],
                        "medical_history_summary": {
                            "total_records": len(medical_history),
                            "recent_records": [dict(record) for record in medical_history]
                        },
                        "upcoming_appointments": [dict(appt) for appt in upcoming_appointments]
                    }
        except Exception as e:
            print(f"Error fetching patient dashboard data: {e}")
            return {}
        
    
    def create_payment_invoice(self, invoice_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new payment invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Validações
                    if not invoice_data.get('invoice_id'):
                        raise ValueError("Invoice ID is required")

                    # Verificar duplicação de Stripe ID
                    if invoice_data.get('stripe_id'):
                        cursor.execute(
                            "SELECT id FROM public.payment_invoices WHERE stripe_id = %s",
                            (invoice_data['stripe_id'],)
                        )
                        if cursor.fetchone():
                            raise ValueError(f"Invoice with Stripe ID {invoice_data['stripe_id']} already exists")

                    query = """
                        INSERT INTO public.payment_invoices (
                            id, stripe_id, invoice_id, amount_paid, amount_requested,
                            currency, payment_type, payment_intent, status,
                            is_default, live_mode, paid_at, created_at_unix,
                            organization_id, subscription_id, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    invoice_id = invoice_data.get('id', uuid.uuid4())
                    now = datetime.utcnow()
                    
                    cursor.execute(query, (
                        invoice_id,
                        invoice_data.get('stripe_id'),
                        invoice_data['invoice_id'],
                        invoice_data.get('amount_paid', 0),
                        invoice_data.get('amount_requested', 0),
                        invoice_data.get('currency', 'usd'),
                        invoice_data.get('payment_type'),
                        invoice_data.get('payment_intent'),
                        invoice_data.get('status', 'draft'),
                        invoice_data.get('is_default', False),
                        invoice_data.get('live_mode', False),
                        invoice_data.get('paid_at'),
                        invoice_data.get('created_at_unix'),
                        invoice_data.get('organization_id'),
                        invoice_data.get('subscription_id'),
                        now,
                        now
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating payment invoice: {e}")
            raise

    def get_payment_invoice_by_id(self, invoice_id: UUID) -> Optional[Dict[str, Any]]:
        """Get payment invoice by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.payment_invoices WHERE id = %s"
                    cursor.execute(query, (invoice_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching payment invoice: {e}")
            return None

    def get_payment_invoice_by_stripe_id(self, stripe_id: str) -> Optional[Dict[str, Any]]:
        """Get payment invoice by Stripe ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.payment_invoices WHERE stripe_id = %s"
                    cursor.execute(query, (stripe_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching payment invoice by Stripe ID: {e}")
            return None

    def get_payment_invoices_by_status(self, status: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get payment invoices by status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.payment_invoices WHERE status = %s"
                    count_query = "SELECT COUNT(*) FROM public.payment_invoices WHERE status = %s"
                    params = [status]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('subscription_id'):
                            base_query += " AND subscription_id = %s"
                            count_query += " AND subscription_id = %s"
                            params.append(filters['subscription_id'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 10) if filters else 10
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching payment invoices by status: {e}")
            return [], 0

    def get_payment_invoices_by_organization(self, organization_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get payment invoices by organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.payment_invoices WHERE organization_id = %s"
                    count_query = "SELECT COUNT(*) FROM public.payment_invoices WHERE organization_id = %s"
                    params = [organization_id]
                    
                    if filters and filters.get('status'):
                        base_query += " AND status = %s"
                        count_query += " AND status = %s"
                        params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 10) if filters else 10
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching payment invoices by organization: {e}")
            return [], 0

    def get_payment_invoices_by_subscription(self, subscription_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get payment invoices by subscription"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.payment_invoices WHERE subscription_id = %s"
                    count_query = "SELECT COUNT(*) FROM public.payment_invoices WHERE subscription_id = %s"
                    params = [subscription_id]
                    
                    if filters and filters.get('status'):
                        base_query += " AND status = %s"
                        count_query += " AND status = %s"
                        params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 10) if filters else 10
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching payment invoices by subscription: {e}")
            return [], 0

    def get_all_payment_invoices(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all payment invoices with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.payment_invoices WHERE 1=1"
                    count_query = "SELECT COUNT(*) FROM public.payment_invoices WHERE 1=1"
                    params = []
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('subscription_id'):
                            base_query += " AND subscription_id = %s"
                            count_query += " AND subscription_id = %s"
                            params.append(filters['subscription_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                        
                        if filters.get('payment_type'):
                            base_query += " AND payment_type = %s"
                            count_query += " AND payment_type = %s"
                            params.append(filters['payment_type'])
                        
                        if filters.get('currency'):
                            base_query += " AND currency = %s"
                            count_query += " AND currency = %s"
                            params.append(filters['currency'])
                        
                        if filters.get('live_mode') is not None:
                            base_query += " AND live_mode = %s"
                            count_query += " AND live_mode = %s"
                            params.append(filters['live_mode'])
                        
                        if filters.get('is_default') is not None:
                            base_query += " AND is_default = %s"
                            count_query += " AND is_default = %s"
                            params.append(filters['is_default'])
                        
                        if filters.get('min_amount') is not None:
                            base_query += " AND amount_paid >= %s"
                            count_query += " AND amount_paid >= %s"
                            params.append(filters['min_amount'])
                        
                        if filters.get('max_amount') is not None:
                            base_query += " AND amount_paid <= %s"
                            count_query += " AND amount_paid <= %s"
                            params.append(filters['max_amount'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 20) if filters else 20
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all payment invoices: {e}")
            return [], 0

    def update_payment_invoice(self, invoice_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing payment invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(invoice_id)
                    
                    query = f"""
                        UPDATE public.payment_invoices 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating payment invoice: {e}")
            raise

    def delete_payment_invoice(self, invoice_id: UUID) -> bool:
        """Delete a payment invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "DELETE FROM public.payment_invoices WHERE id = %s"
                    cursor.execute(query, (invoice_id,))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting payment invoice: {e}")
            return False

    def mark_invoice_as_paid(self, invoice_id: UUID, paid_at: int) -> Optional[Dict[str, Any]]:
        """Mark invoice as paid manually"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.payment_invoices 
                        SET status = 'paid', paid_at = %s, updated_at = %s
                        WHERE id = %s AND status = 'open'
                        RETURNING *
                    """
                    cursor.execute(query, (paid_at, datetime.utcnow(), invoice_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error marking invoice as paid: {e}")
            raise

    def retry_failed_invoice(self, invoice_id: UUID) -> Optional[Dict[str, Any]]:
        """Retry processing for a failed invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.payment_invoices 
                        SET status = 'open', updated_at = %s
                        WHERE id = %s AND status IN ('uncollectible', 'void')
                        RETURNING *
                    """
                    cursor.execute(query, (datetime.utcnow(), invoice_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error retrying failed invoice: {e}")
            raise

    def get_payment_invoice_statistics(self, organization_id: Optional[UUID] = None, start_date_unix: Optional[int] = None, end_date_unix: Optional[int] = None) -> Dict[str, Any]:
        """Get payment invoice statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_invoices,
                            COUNT(CASE WHEN status = 'paid' THEN 1 END) as paid_invoices,
                            COUNT(CASE WHEN status = 'open' THEN 1 END) as pending_invoices,
                            COUNT(CASE WHEN status IN ('uncollectible', 'void') THEN 1 END) as failed_invoices,
                            COALESCE(SUM(amount_paid), 0) as total_revenue,
                            COALESCE(AVG(amount_paid), 0) as average_invoice_amount,
                            currency
                        FROM public.payment_invoices 
                        WHERE 1=1
                    """
                    params = []
                    
                    if organization_id:
                        query += " AND organization_id = %s"
                        params.append(organization_id)
                    
                    if start_date_unix:
                        query += " AND created_at_unix >= %s"
                        params.append(start_date_unix)
                    
                    if end_date_unix:
                        query += " AND created_at_unix <= %s"
                        params.append(end_date_unix)
                    
                    query += " GROUP BY currency"
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    
                    if not result:
                        return {
                            "total_invoices": 0,
                            "paid_invoices": 0,
                            "pending_invoices": 0,
                            "failed_invoices": 0,
                            "total_revenue": 0,
                            "average_invoice_amount": 0.0,
                            "currency": "usd"
                        }
                    
                    return dict(result)
        except Exception as e:
            print(f"Error fetching payment invoice statistics: {e}")
            return {}

    def get_outstanding_invoices(self, organization_id: UUID) -> List[Dict[str, Any]]:
        """Get outstanding (unpaid) invoices for an organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_invoices 
                        WHERE organization_id = %s AND status = 'open'
                        ORDER BY created_at DESC
                    """
                    cursor.execute(query, (organization_id,))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching outstanding invoices: {e}")
            return []

    def apply_discount_to_invoice(self, invoice_id: UUID, discount_amount: float) -> Optional[Dict[str, Any]]:
        """Apply discount to an invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    cursor.execute(
                        "SELECT amount_requested FROM public.payment_invoices WHERE id = %s AND status = 'open'",
                        (invoice_id,)
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        raise ValueError("Invoice not found or not open")
                    
                    current_amount = result['amount_requested']
                    new_amount = current_amount - int(discount_amount)
                    
                    if new_amount <= 0:
                        raise ValueError("Discount amount cannot exceed invoice amount")
                    
                    query = """
                        UPDATE public.payment_invoices 
                        SET amount_requested = %s, updated_at = %s
                        WHERE id = %s
                        RETURNING *
                    """
                    cursor.execute(query, (new_amount, datetime.utcnow(), invoice_id))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error applying discount to invoice: {e}")
            raise

    def search_payment_invoices(self, search_query: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Search payment invoices by multiple criteria"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT * FROM public.payment_invoices 
                        WHERE invoice_id ILIKE %s OR stripe_id ILIKE %s OR payment_intent ILIKE %s
                    """
                    count_query = """
                        SELECT COUNT(*) FROM public.payment_invoices 
                        WHERE invoice_id ILIKE %s OR stripe_id ILIKE %s OR payment_intent ILIKE %s
                    """
                    search_param = f"%{search_query}%"
                    params = [search_param, search_param, search_param]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 20) if filters else 20
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error searching payment invoices: {e}")
            return [], 0

    def get_organization_invoice_summary(self, organization_id: UUID) -> Dict[str, Any]:
        """Get invoice summary for an organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_invoices,
                            COUNT(CASE WHEN status = 'paid' THEN 1 END) as paid_count,
                            COUNT(CASE WHEN status = 'open' THEN 1 END) as pending_count,
                            COUNT(CASE WHEN status IN ('uncollectible', 'void') THEN 1 END) as failed_count,
                            COALESCE(SUM(CASE WHEN status = 'paid' THEN amount_paid ELSE 0 END), 0) as total_paid,
                            COALESCE(SUM(CASE WHEN status = 'open' THEN amount_requested ELSE 0 END), 0) as total_pending,
                            MAX(created_at) as last_invoice_date
                        FROM public.payment_invoices 
                        WHERE organization_id = %s
                    """
                    cursor.execute(query, (organization_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching organization invoice summary: {e}")
            return {}

    def validate_invoice_entities(self, organization_id: UUID, subscription_id: Optional[UUID] = None) -> bool:
        """Validate that organization and subscription exist"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.organizations WHERE id = %s) AS org_exists",
                        (organization_id,)
                    )
                    org_exists = cursor.fetchone()['org_exists']
                    
                    subscription_exists = True
                    if subscription_id:
                        cursor.execute(
                            "SELECT EXISTS (SELECT 1 FROM public.subscriptions WHERE id = %s) AS subscription_exists",
                            (subscription_id,)
                        )
                        subscription_exists = cursor.fetchone()['subscription_exists']
                    
                    return org_exists and subscription_exists
        except Exception as e:
            print(f"Error validating invoice entities: {e}")
            return False

    def get_invoice_organization_name(self, organization_id: UUID) -> Optional[str]:
        """Get organization name for invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT name FROM public.organizations WHERE id = %s"
                    cursor.execute(query, (organization_id,))
                    result = cursor.fetchone()
                    return result['name'] if result else None
        except Exception as e:
            print(f"Error fetching organization name: {e}")
            return None

    def get_invoice_subscription_plan(self, subscription_id: UUID) -> Optional[str]:
        """Get subscription plan name for invoice"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT plan FROM public.subscriptions WHERE id = %s"
                    cursor.execute(query, (subscription_id,))
                    result = cursor.fetchone()
                    return result['plan'] if result else None
        except Exception as e:
            print(f"Error fetching subscription plan: {e}")
            return None

    def bulk_update_invoice_status(self, invoice_ids: List[UUID], new_status: str) -> int:
        """Bulk update invoice status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Criar uma string de placeholders para os IDs
                    placeholders = ','.join(['%s'] * len(invoice_ids))
                    
                    query = f"""
                        UPDATE public.payment_invoices 
                        SET status = %s, updated_at = %s
                        WHERE id IN ({placeholders})
                    """
                    params = [new_status, datetime.utcnow()] + invoice_ids
                    
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"Error bulk updating invoice status: {e}")
            return 0
        
    
    def create_payment_intent(self, payment_intent_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new payment intent"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        INSERT INTO public.payment_intents (
                            id, created_at, updated_at, organization_id, 
                            amount_capturable, amount_details, canceled_at, 
                            capture_method, confirmation_method, currency, 
                            customer_id, description, invoice_id, 
                            last_payment_error, latest_charge, livemode, 
                            metadata, next_action, on_behalf_of, 
                            payment_method_id, payment_method_configuration_details,
                            payment_method_options, payment_method_types, 
                            processing, receipt_email, review, 
                            setup_future_usage, shipping, source, 
                            statement_descriptor, statement_descriptor_suffix, 
                            status, transfer_data, transfer_group, 
                            internal_amount
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    payment_intent_id = payment_intent_data.get('id', uuid.uuid4())
                    now = datetime.utcnow()
                    
                    cursor.execute(query, (
                        payment_intent_id,
                        now,
                        now,
                        payment_intent_data.get('organization_id'),
                        payment_intent_data.get('amount_capturable', 0),
                        json.dumps(payment_intent_data.get('amount_details', {})),
                        payment_intent_data.get('canceled_at'),
                        payment_intent_data.get('capture_method', 'automatic'),
                        payment_intent_data.get('confirmation_method', 'automatic'),
                        payment_intent_data.get('currency', 'usd'),
                        payment_intent_data.get('customer_id'),
                        payment_intent_data.get('description'),
                        payment_intent_data.get('invoice_id'),
                        json.dumps(payment_intent_data.get('last_payment_error', {})),
                        payment_intent_data.get('latest_charge'),
                        payment_intent_data.get('livemode', False),
                        json.dumps(payment_intent_data.get('metadata', {})),
                        json.dumps(payment_intent_data.get('next_action', {})),
                        payment_intent_data.get('on_behalf_of'),
                        payment_intent_data.get('payment_method_id'),
                        json.dumps(payment_intent_data.get('payment_method_configuration_details', {})),
                        json.dumps(payment_intent_data.get('payment_method_options', {})),
                        json.dumps(payment_intent_data.get('payment_method_types', [])),
                        json.dumps(payment_intent_data.get('processing', {})),
                        payment_intent_data.get('receipt_email'),
                        json.dumps(payment_intent_data.get('review', {})),
                        payment_intent_data.get('setup_future_usage'),
                        json.dumps(payment_intent_data.get('shipping', {})),
                        payment_intent_data.get('source'),
                        payment_intent_data.get('statement_descriptor'),
                        payment_intent_data.get('statement_descriptor_suffix'),
                        payment_intent_data.get('status', 'pending'),
                        json.dumps(payment_intent_data.get('transfer_data', {})),
                        payment_intent_data.get('transfer_group'),
                        payment_intent_data.get('internal_amount', 0)
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating payment intent: {e}")
            raise

    def get_payment_intent_by_id(self, payment_intent_id: UUID) -> Optional[Dict[str, Any]]:
        """Get payment intent by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (payment_intent_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching payment intent: {e}")
            return None

    def get_payment_intent_by_stripe_charge_id(self, stripe_charge_id: str) -> Optional[Dict[str, Any]]:
        """Get payment intent by Stripe charge ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE latest_charge = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (stripe_charge_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching payment intent by Stripe charge ID: {e}")
            return None

    def get_payment_intents_by_customer(self, customer_id: UUID, limit: int = 100) -> List[Dict[str, Any]]:
        """Get payment intents by customer ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE customer_id = %s AND deleted_at IS NULL
                        ORDER BY created_at DESC
                        LIMIT %s
                    """
                    cursor.execute(query, (customer_id, limit))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching payment intents by customer: {e}")
            return []

    def get_payment_intents_by_organization(self, organization_id: UUID, limit: int = 100) -> List[Dict[str, Any]]:
        """Get payment intents by organization ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE organization_id = %s AND deleted_at IS NULL
                        ORDER BY created_at DESC
                        LIMIT %s
                    """
                    cursor.execute(query, (organization_id, limit))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching payment intents by organization: {e}")
            return []

    def update_payment_intent_status(self, stripe_charge_id: str, status: str) -> bool:
        """Update payment intent status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.payment_intents 
                        SET status = %s, updated_at = %s 
                        WHERE latest_charge = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (status, datetime.utcnow(), stripe_charge_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating payment intent status: {e}")
            return False

    def update_payment_intent(self, payment_intent_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing payment intent"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            # Handle JSON fields
                            if field in ['amount_details', 'last_payment_error', 'metadata', 
                                       'next_action', 'payment_method_configuration_details',
                                       'payment_method_options', 'payment_method_types',
                                       'processing', 'review', 'shipping', 'transfer_data']:
                                set_clauses.append(f"{field} = %s")
                                params.append(json.dumps(value) if value else '{}')
                            else:
                                set_clauses.append(f"{field} = %s")
                                params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(payment_intent_id)
                    
                    query = f"""
                        UPDATE public.payment_intents 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s AND deleted_at IS NULL
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating payment intent: {e}")
            raise

    def delete_payment_intent(self, payment_intent_id: UUID) -> bool:
        """Soft delete a payment intent"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.payment_intents 
                        SET deleted_at = %s 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (datetime.utcnow(), payment_intent_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting payment intent: {e}")
            return False

    def get_payment_intents_by_status(self, status: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get payment intents by status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE status = %s AND deleted_at IS NULL
                        ORDER BY created_at DESC
                        LIMIT %s
                    """
                    cursor.execute(query, (status, limit))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching payment intents by status: {e}")
            return []

    def search_payment_intents(self, search_query: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Search payment intents by multiple criteria"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT * FROM public.payment_intents 
                        WHERE deleted_at IS NULL AND (
                            description ILIKE %s OR 
                            latest_charge ILIKE %s OR
                            payment_method_id ILIKE %s
                        )
                    """
                    count_query = """
                        SELECT COUNT(*) FROM public.payment_intents 
                        WHERE deleted_at IS NULL AND (
                            description ILIKE %s OR 
                            latest_charge ILIKE %s OR
                            payment_method_id ILIKE %s
                        )
                    """
                    search_param = f"%{search_query}%"
                    params = [search_param, search_param, search_param]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('customer_id'):
                            base_query += " AND customer_id = %s"
                            count_query += " AND customer_id = %s"
                            params.append(filters['customer_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('size', 20) if filters else 20
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error searching payment intents: {e}")
            return [], 0

    def get_payment_intent_statistics(self, organization_id: Optional[UUID] = None) -> Dict[str, Any]:
        """Get payment intent statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_intents,
                            COUNT(CASE WHEN status = 'succeeded' THEN 1 END) as succeeded_count,
                            COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_count,
                            COUNT(CASE WHEN status = 'requires_payment_method' THEN 1 END) as requires_payment_method_count,
                            COUNT(CASE WHEN status = 'requires_confirmation' THEN 1 END) as requires_confirmation_count,
                            COUNT(CASE WHEN status = 'requires_action' THEN 1 END) as requires_action_count,
                            COUNT(CASE WHEN status = 'canceled' THEN 1 END) as canceled_count,
                            COALESCE(SUM(internal_amount), 0) as total_amount,
                            COALESCE(AVG(internal_amount), 0) as average_amount,
                            MAX(created_at) as last_intent_created
                        FROM public.payment_intents 
                        WHERE deleted_at IS NULL
                    """
                    params = []
                    
                    if organization_id:
                        query += " AND organization_id = %s"
                        params.append(organization_id)
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching payment intent statistics: {e}")
            return {}

    def get_recent_payment_intents(self, days: int = 7, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent payment intents"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE created_at >= %s AND deleted_at IS NULL
                        ORDER BY created_at DESC
                        LIMIT %s
                    """
                    cutoff_date = datetime.utcnow() - timedelta(days=days)
                    cursor.execute(query, (cutoff_date, limit))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching recent payment intents: {e}")
            return []

    def get_failed_payment_intents(self, organization_id: Optional[UUID] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Get failed payment intents"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.payment_intents 
                        WHERE status IN ('canceled', 'requires_payment_method') 
                        AND deleted_at IS NULL
                    """
                    params = []
                    
                    if organization_id:
                        query += " AND organization_id = %s"
                        params.append(organization_id)
                    
                    query += " ORDER BY created_at DESC LIMIT %s"
                    params.append(limit)
                    
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching failed payment intents: {e}")
            return []

    def validate_payment_intent_entities(self, organization_id: UUID, customer_id: UUID) -> bool:
        """Validate that organization and customer exist"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.organizations WHERE id = %s) AS org_exists",
                        (organization_id,)
                    )
                    org_exists = cursor.fetchone()['org_exists']
                    
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.customers WHERE id = %s) AS customer_exists",
                        (customer_id,)
                    )
                    customer_exists = cursor.fetchone()['customer_exists']
                    
                    return org_exists and customer_exists
        except Exception as e:
            print(f"Error validating payment intent entities: {e}")
            return False

    def bulk_update_payment_intent_status(self, payment_intent_ids: List[UUID], new_status: str) -> int:
        """Bulk update payment intent status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Criar uma string de placeholders para os IDs
                    placeholders = ','.join(['%s'] * len(payment_intent_ids))
                    
                    query = f"""
                        UPDATE public.payment_intents 
                        SET status = %s, updated_at = %s
                        WHERE id IN ({placeholders}) AND deleted_at IS NULL
                    """
                    params = [new_status, datetime.utcnow()] + payment_intent_ids
                    
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"Error bulk updating payment intent status: {e}")
            return 0
        
        
     
    def create_subscription(self, subscription_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new subscription"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Validações
                    if not subscription_data.get('subscription_number'):
                        raise ValueError("Subscription number is required")
                    
                    if not subscription_data.get('organization_id'):
                        raise ValueError("Organization ID is required")

                    # Verificar duplicação de subscription number
                    cursor.execute(
                        "SELECT id FROM public.subscriptions WHERE subscription_number = %s AND deleted_at IS NULL",
                        (subscription_data['subscription_number'],)
                    )
                    if cursor.fetchone():
                        raise ValueError(f"Subscription with number {subscription_data['subscription_number']} already exists")

                    query = """
                        INSERT INTO public.subscriptions (
                            id, subscription_number, organization_id, plan,
                            start_date, end_date, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                    """
                    
                    subscription_id = subscription_data.get('id', uuid.uuid4())
                    now = datetime.utcnow()
                    
                    cursor.execute(query, (
                        subscription_id,
                        subscription_data['subscription_number'],
                        subscription_data['organization_id'],
                        subscription_data.get('plan'),
                        subscription_data.get('start_date'),
                        subscription_data.get('end_date'),
                        subscription_data.get('status', 'active'),
                        now,
                        now
                    ))
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error creating subscription: {e}")
            raise

    def get_subscription_by_id(self, subscription_id: UUID) -> Optional[Dict[str, Any]]:
        """Get subscription by ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.subscriptions WHERE id = %s AND deleted_at IS NULL"
                    cursor.execute(query, (subscription_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching subscription: {e}")
            return None

    def get_subscription_by_number(self, subscription_number: str) -> Optional[Dict[str, Any]]:
        """Get subscription by subscription number"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.subscriptions WHERE subscription_number = %s AND deleted_at IS NULL"
                    cursor.execute(query, (subscription_number,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error fetching subscription by number: {e}")
            return None

    def get_subscriptions_by_start_date(self, start_date: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get subscriptions by start date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE start_date = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE start_date = %s AND deleted_at IS NULL"
                    params = [start_date]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    base_query += " ORDER BY created_at DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching subscriptions by start date: {e}")
            return [], 0

    def get_subscriptions_by_end_date(self, end_date: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get subscriptions by end date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE end_date = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE end_date = %s AND deleted_at IS NULL"
                    params = [end_date]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    base_query += " ORDER BY created_at DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching subscriptions by end date: {e}")
            return [], 0

    def get_subscriptions_by_status(self, status: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get subscriptions by status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE status = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE status = %s AND deleted_at IS NULL"
                    params = [status]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('start_date'):
                            base_query += " AND start_date >= %s"
                            count_query += " AND start_date >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND end_date <= %s"
                            count_query += " AND end_date <= %s"
                            params.append(filters['end_date'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    base_query += " ORDER BY created_at DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching subscriptions by status: {e}")
            return [], 0

    def get_subscriptions_by_created_at(self, created_at: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get subscriptions by creation date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE DATE(created_at) = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE DATE(created_at) = %s AND deleted_at IS NULL"
                    params = [created_at]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    base_query += " ORDER BY created_at DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching subscriptions by creation date: {e}")
            return [], 0

    def get_subscriptions_by_updated_at(self, updated_at: date, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get subscriptions by last update date"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE DATE(updated_at) = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE DATE(updated_at) = %s AND deleted_at IS NULL"
                    params = [updated_at]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    base_query += " ORDER BY updated_at DESC"
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching subscriptions by update date: {e}")
            return [], 0

    def get_all_subscriptions(self, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get all subscriptions with optional filtering"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE deleted_at IS NULL"
                    params = []
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                        
                        if filters.get('start_date'):
                            base_query += " AND start_date >= %s"
                            count_query += " AND start_date >= %s"
                            params.append(filters['start_date'])
                        
                        if filters.get('end_date'):
                            base_query += " AND end_date <= %s"
                            count_query += " AND end_date <= %s"
                            params.append(filters['end_date'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching all subscriptions: {e}")
            return [], 0

    def get_subscriptions_by_organization(self, organization_id: UUID, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Get subscriptions by organization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = "SELECT * FROM public.subscriptions WHERE organization_id = %s AND deleted_at IS NULL"
                    count_query = "SELECT COUNT(*) FROM public.subscriptions WHERE organization_id = %s AND deleted_at IS NULL"
                    params = [organization_id]
                    
                    if filters and filters.get('status'):
                        base_query += " AND status = %s"
                        count_query += " AND status = %s"
                        params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 100) if filters else 100
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error fetching subscriptions by organization: {e}")
            return [], 0

    def update_subscription(self, subscription_id: UUID, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing subscription"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    set_clauses = []
                    params = []
                    
                    for field, value in update_data.items():
                        if value is not None:
                            set_clauses.append(f"{field} = %s")
                            params.append(value)
                    
                    if not set_clauses:
                        return None
                    
                    set_clauses.append("updated_at = %s")
                    params.append(datetime.utcnow())
                    
                    params.append(subscription_id)
                    
                    query = f"""
                        UPDATE public.subscriptions 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s AND deleted_at IS NULL
                        RETURNING *
                    """
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    conn.commit()
                    return dict(result) if result else None
        except Exception as e:
            print(f"Error updating subscription: {e}")
            raise

    def delete_subscription(self, subscription_id: UUID) -> bool:
        """Soft delete a subscription"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        UPDATE public.subscriptions 
                        SET deleted_at = %s 
                        WHERE id = %s AND deleted_at IS NULL
                    """
                    cursor.execute(query, (datetime.utcnow(), subscription_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting subscription: {e}")
            return False

    def get_active_subscriptions(self, organization_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """Get active subscriptions"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM public.subscriptions WHERE status = 'active' AND deleted_at IS NULL"
                    params = []
                    
                    if organization_id:
                        query += " AND organization_id = %s"
                        params.append(organization_id)
                    
                    query += " ORDER BY created_at DESC"
                    
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching active subscriptions: {e}")
            return []

    def get_expiring_subscriptions(self, days_threshold: int = 30) -> List[Dict[str, Any]]:
        """Get subscriptions expiring within specified days"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT * FROM public.subscriptions 
                        WHERE end_date BETWEEN %s AND %s 
                        AND status = 'active' 
                        AND deleted_at IS NULL
                        ORDER BY end_date ASC
                    """
                    today = date.today()
                    threshold_date = today + timedelta(days=days_threshold)
                    
                    cursor.execute(query, (today, threshold_date))
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching expiring subscriptions: {e}")
            return []

    def get_subscription_statistics(self, organization_id: Optional[UUID] = None) -> Dict[str, Any]:
        """Get subscription statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT 
                            COUNT(*) as total_subscriptions,
                            COUNT(CASE WHEN status = 'active' THEN 1 END) as active_count,
                            COUNT(CASE WHEN status = 'inactive' THEN 1 END) as inactive_count,
                            COUNT(CASE WHEN status = 'suspended' THEN 1 END) as suspended_count,
                            COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_count,
                            COUNT(CASE WHEN end_date < %s THEN 1 END) as expired_count,
                            COUNT(CASE WHEN end_date BETWEEN %s AND %s THEN 1 END) as expiring_soon_count,
                            MAX(created_at) as last_subscription_created
                        FROM public.subscriptions 
                        WHERE deleted_at IS NULL
                    """
                    today = date.today()
                    next_30_days = today + timedelta(days=30)
                    params = [today, today, next_30_days]
                    
                    if organization_id:
                        query += " AND organization_id = %s"
                        params.append(organization_id)
                    
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return dict(result) if result else {}
        except Exception as e:
            print(f"Error fetching subscription statistics: {e}")
            return {}

    def search_subscriptions(self, search_query: str, filters: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Search subscriptions by multiple criteria"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    base_query = """
                        SELECT * FROM public.subscriptions 
                        WHERE deleted_at IS NULL AND (
                            subscription_number ILIKE %s OR 
                            plan ILIKE %s
                        )
                    """
                    count_query = """
                        SELECT COUNT(*) FROM public.subscriptions 
                        WHERE deleted_at IS NULL AND (
                            subscription_number ILIKE %s OR 
                            plan ILIKE %s
                        )
                    """
                    search_param = f"%{search_query}%"
                    params = [search_param, search_param]
                    
                    if filters:
                        if filters.get('organization_id'):
                            base_query += " AND organization_id = %s"
                            count_query += " AND organization_id = %s"
                            params.append(filters['organization_id'])
                        
                        if filters.get('status'):
                            base_query += " AND status = %s"
                            count_query += " AND status = %s"
                            params.append(filters['status'])
                    
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()['count']
                    
                    page = filters.get('page', 1) if filters else 1
                    size = filters.get('page_size', 20) if filters else 20
                    offset = (page - 1) * size
                    
                    base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
                    params.extend([size, offset])
                    
                    cursor.execute(base_query, params)
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results], total
        except Exception as e:
            print(f"Error searching subscriptions: {e}")
            return [], 0

    def validate_subscription_entities(self, organization_id: UUID) -> bool:
        """Validate that organization exists"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.organizations WHERE id = %s) AS org_exists",
                        (organization_id,)
                    )
                    org_exists = cursor.fetchone()['org_exists']
                    return org_exists
        except Exception as e:
            print(f"Error validating subscription entities: {e}")
            return False

    def bulk_update_subscription_status(self, subscription_ids: List[UUID], new_status: str) -> int:
        """Bulk update subscription status"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    placeholders = ','.join(['%s'] * len(subscription_ids))
                    
                    query = f"""
                        UPDATE public.subscriptions 
                        SET status = %s, updated_at = %s
                        WHERE id IN ({placeholders}) AND deleted_at IS NULL
                    """
                    params = [new_status, datetime.utcnow()] + subscription_ids
                    
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"Error bulk updating subscription status: {e}")
            return 0
        
    
        
    
        
    

# Global database instance
db = Database()