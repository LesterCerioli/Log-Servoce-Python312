# Log Microservice

A high-performance, scalable logging microservice built with Python 3.12 and FastAPI, designed for distributed systems and microservices architecture.

## 🚀 Key Features

### Core Logging Capabilities
- **Structured JSON Logging** - Consistent, parseable log format
- **Multiple Log Levels** - DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Performance Metrics** - Automatic duration tracking and performance categorization
- **Error Tracking** - Detailed error context and stack traces

### Microservice Architecture
- **RESTful API** - Clean, standardized endpoints for service communication
- **Async Operations** - Full async/await support for high concurrency
- **Lightweight & Fast** - Optimized for low latency in distributed systems
- **Container Ready** - Docker support for easy deployment

### Multi-Tenancy & Isolation
- **Organization Support** - Full multi-tenant architecture
- **Service Segmentation** - Track logs per service across organizations
- **Data Isolation** - Secure separation between different tenants
- **Flexible Identity** - Support for both organization_id and organization_name

### Advanced Search & Querying
- **Comprehensive Filtering** - Filter by service, level, date range, duration, tags
- **Full-Text Search** - Search across log messages and error details
- **Correlation ID Support** - Track requests across service boundaries
- **Tag-based Filtering** - Categorize and query logs using custom tags

### Security & Validation
- **Input Sanitization** - Automatic cleaning of malicious inputs
- **SQL Injection Protection** - Parameterized queries only
- **XSS Prevention** - HTML encoding and character filtering
- **UUID Validation** - Secure identifier handling

### Performance & Scalability
- **Bulk Operations** - Efficient batch log ingestion
- **Connection Pooling** - Optimized database connections
- **Query Optimization** - Native PostgreSQL queries for performance
- **Paginated Responses** - Handle large datasets efficiently

### Monitoring & Analytics
- **Real-time Metrics** - Success rates, error rates, performance stats
- **Service Health** - Monitor service performance and availability
- **Trend Analysis** - Identify patterns and anomalies
- **Performance Insights** - P50, P95, P99 latency tracking

### Data Management
- **Automatic Retention** - Configurable log cleanup policies
- **Export Capabilities** - JSON and CSV export for analysis
- **Backup Support** - Easy data migration and recovery
- **Storage Efficiency** - Optimized data storage strategies

## 🏗️ Architecture

### Service Components
