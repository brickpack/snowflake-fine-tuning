# Snowflake Cost Optimization Best Practices

This guide outlines proven strategies for reducing Snowflake costs while maintaining or improving performance.

## Table of Contents

- [Warehouse Optimization](#warehouse-optimization)
- [Query Optimization](#query-optimization)
- [Storage Optimization](#storage-optimization)
- [Monitoring and Governance](#monitoring-and-governance)

## Warehouse Optimization

### Right-Sizing Warehouses

**Problem**: Over-provisioned warehouses waste credits on unused compute capacity.

**Solution**:
- Start with smaller warehouses and scale up based on actual usage
- Monitor warehouse load with `WAREHOUSE_LOAD_HISTORY` view
- Use the warehouse monitoring scripts in `cost-optimization/warehouse-monitoring/`

**Example**:
```sql
-- Check warehouse utilization
SELECT
    warehouse_name,
    AVG(avg_running) as avg_concurrent_queries,
    MAX(avg_running) as peak_concurrent_queries
FROM snowflake.account_usage.warehouse_load_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;
```

**Rule of Thumb**:
- X-SMALL/SMALL: < 5 concurrent queries
- MEDIUM: 5-10 concurrent queries
- LARGE: 10-20 concurrent queries
- X-LARGE+: > 20 concurrent queries or very large data volumes

### Auto-Suspend Configuration

**Problem**: Warehouses left running when idle waste credits continuously.

**Solution**:
- Enable auto-suspend with appropriate timeout based on usage patterns
- Recommended settings:
  - High-frequency workloads: 5 minutes (300 seconds)
  - Scheduled batch jobs: 1-2 minutes (60-120 seconds)
  - Ad-hoc analysis: 10 minutes (600 seconds)

**Configuration**:
```sql
ALTER WAREHOUSE my_warehouse
SET AUTO_SUSPEND = 300  -- 5 minutes
    AUTO_RESUME = TRUE;
```

**Best Practice**: Always enable auto-resume to ensure seamless user experience.

### Multi-Cluster Warehouses

**When to Use**:
- High query concurrency (>10 simultaneous queries)
- Predictable peak usage patterns
- Need to maintain SLAs during load spikes

**Scaling Policies**:
- **Standard**: Prioritizes performance, scales up quickly
  - Use for: Time-sensitive workloads, user-facing applications
- **Economy**: Prioritizes cost, waits for queue to build before scaling
  - Use for: Batch processing, cost-sensitive workloads

**Configuration**:
```sql
ALTER WAREHOUSE my_warehouse
SET MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'ECONOMY';
```

### Warehouse Consolidation

**Problem**: Too many small warehouses increase overhead and complexity.

**Solution**:
- Consolidate similar workloads to fewer, appropriately sized warehouses
- Use separate warehouses only when:
  - Workloads have different SLA requirements
  - Need to isolate resource consumption for chargeback
  - Workload patterns differ significantly (streaming vs. batch)

## Query Optimization

### Avoid SELECT *

**Problem**: Retrieving unnecessary columns wastes I/O and network bandwidth.

**Bad**:
```sql
SELECT * FROM large_table WHERE date = CURRENT_DATE();
```

**Good**:
```sql
SELECT id, name, amount, date
FROM large_table
WHERE date = CURRENT_DATE();
```

**Impact**: Can reduce query cost by 50%+ on wide tables.

### Use Clustering Keys

**When to Apply**:
- Tables > 1TB
- Queries frequently filter on specific columns
- Poor partition pruning (>50% of partitions scanned)

**Example**:
```sql
-- Add clustering key
ALTER TABLE large_events_table
CLUSTER BY (event_date, user_id);

-- Monitor clustering effectiveness
SELECT SYSTEM$CLUSTERING_INFORMATION('large_events_table');
```

**Cost Consideration**: Clustering has ongoing maintenance costs. Only cluster if query savings exceed reclustering costs.

### Leverage Result Caching

**Strategy**: Identical queries return cached results within 24 hours at no cost.

**Best Practices**:
- Use consistent query formatting
- Avoid user-specific filters (user_id, session_id) when possible
- Consider materializing frequently accessed aggregations

### Optimize JOIN Operations

**Tips**:
1. **Join Order**: Place smaller tables first in joins
2. **Filter Early**: Apply WHERE clauses before joins when possible
3. **Use Appropriate Join Types**:
   - INNER JOIN when possible (most efficient)
   - Avoid cross joins
4. **Reduce Data Before Joins**: Use CTEs to pre-aggregate

**Example**:
```sql
-- Inefficient
SELECT a.*, b.*
FROM huge_table a
JOIN small_table b ON a.id = b.id
WHERE a.date >= '2024-01-01';

-- Efficient
WITH filtered_data AS (
    SELECT id, col1, col2
    FROM huge_table
    WHERE date >= '2024-01-01'
)
SELECT f.*, s.*
FROM small_table s
JOIN filtered_data f ON f.id = s.id;
```

## Storage Optimization

### Time Travel Retention

**Problem**: Longer time travel retention increases storage costs.

**Default**: 1 day (standard), 0-90 days (configurable)

**Recommendation**:
- Transient data: 0-1 days
- Important tables: 7 days
- Critical tables: 30-90 days (Enterprise only)

**Configuration**:
```sql
-- Reduce time travel for staging tables
ALTER TABLE staging_data
SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- Use transient tables for temporary data
CREATE TRANSIENT TABLE temp_calculations (
    id INT,
    value FLOAT
);
```

### Table Optimization

**Strategies**:
1. **Use Transient Tables**: For intermediate/staging data that doesn't need Fail-safe
2. **Drop Unused Tables**: Identify with:
   ```sql
   SELECT table_name, created, last_altered
   FROM information_schema.tables
   WHERE last_altered < DATEADD(month, -3, CURRENT_DATE());
   ```
3. **Partition Large Tables**: Consider breaking very large tables into smaller date-partitioned tables

### Materialized Views

**Use Cases**:
- Frequently accessed aggregations
- Complex joins on stable data
- Expensive transformations used by multiple queries

**Cost-Benefit**:
- ✅ Saves on repeated query compute
- ❌ Costs for storage and automatic refresh

**Example**:
```sql
CREATE MATERIALIZED VIEW daily_sales_summary AS
SELECT
    DATE_TRUNC('day', sale_date) as day,
    product_id,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM sales
GROUP BY DATE_TRUNC('day', sale_date), product_id;
```

## Monitoring and Governance

### Implement Resource Monitors

**Purpose**: Set spending limits and alerts to prevent runaway costs.

**Example**:
```sql
CREATE RESOURCE MONITOR monthly_limit
WITH CREDIT_QUOTA = 1000
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply to warehouse
ALTER WAREHOUSE my_warehouse
SET RESOURCE_MONITOR = monthly_limit;
```

### Tag Resources for Cost Attribution

**Benefits**:
- Track spend by team/project
- Implement chargeback models
- Identify optimization opportunities

**Implementation**:
```sql
-- Create tags
CREATE TAG cost_center;
CREATE TAG environment;

-- Apply to resources
ALTER WAREHOUSE analytics_wh
SET TAG cost_center = 'analytics';

ALTER DATABASE prod_db
SET TAG environment = 'production';
```

### Regular Cost Reviews

**Cadence**: Weekly or bi-weekly review of:
1. Top 10 most expensive warehouses
2. Top 10 longest-running queries
3. Idle warehouses
4. Storage growth trends

**Automation**: Use the monitoring scripts in this repository:
```bash
# Weekly cost review
python cost-optimization/warehouse-monitoring/analyze_usage.py --days 7

# Identify idle resources
python cost-optimization/idle-detection/find_idle_warehouses.py

# Right-sizing recommendations
python cost-optimization/right-sizing/recommend_sizes.py
```

## Key Metrics to Track

### Cost Metrics
- **Daily/Monthly Credit Consumption**: Track against budget
- **Cost per Query**: Identify expensive queries
- **Cost by Warehouse**: Allocate spending by team/workload

### Performance Metrics
- **Query Execution Time**: P50, P95, P99 percentiles
- **Warehouse Utilization**: Average running queries
- **Queue Time**: Indicates need for scaling

### Efficiency Metrics
- **Bytes Scanned per Query**: Lower is better
- **Partition Pruning Ratio**: % of partitions scanned
- **Result Cache Hit Rate**: Higher is better
- **Spillage**: Disk spill indicates memory pressure

## Quick Wins Checklist

- [ ] Enable auto-suspend on all warehouses (5 min default)
- [ ] Enable auto-resume on all warehouses
- [ ] Set up resource monitors with alerts
- [ ] Review and right-size oversized warehouses
- [ ] Identify and suspend/delete unused warehouses
- [ ] Add clustering keys to large, frequently queried tables
- [ ] Reduce time travel retention on non-critical tables
- [ ] Convert temporary tables to transient
- [ ] Implement result caching strategies
- [ ] Set up automated cost monitoring

## Advanced Optimization Techniques

### Query Acceleration Service

For compute-intensive queries with large data scans, consider enabling query acceleration.

### Search Optimization Service

For selective point lookups on large tables, enable search optimization.

### Snowpipe Streaming

For real-time data ingestion, use Snowpipe Streaming to reduce latency and costs compared to batch loading.

## Resources

- [Official Snowflake Cost Optimization Guide](https://docs.snowflake.com/en/user-guide/cost-understanding-overall)
- Monitoring scripts: `monitoring/` directory
- Cost analysis tools: `cost-optimization/` directory
- Terraform modules: `terraform/` directory

## Getting Help

For questions or additional optimization strategies, consult:
1. Snowflake documentation
2. Your Snowflake account team
3. Internal data platform team
