# Runbook: Responding to Cost Spikes

## Overview

This runbook provides step-by-step procedures for investigating and responding to unexpected Snowflake cost increases.

## Severity Levels

- **P1 - Critical**: Cost spike > 200% of baseline, immediate action required
- **P2 - High**: Cost spike > 150% of baseline, action required within 4 hours
- **P3 - Medium**: Cost spike > 120% of baseline, investigate within 24 hours

## Initial Response (First 15 Minutes)

### 1. Confirm the Alert

Verify the cost spike is real and not a false alarm:

```bash
# Check current daily spend
python cost-optimization/warehouse-monitoring/analyze_usage.py --days 1
```

### 2. Identify the Source

Find which warehouse(s) are consuming excessive credits:

```sql
-- Run in Snowflake
SELECT
    warehouse_name,
    DATE_TRUNC('hour', start_time) as hour,
    SUM(credits_used) as credits_used,
    COUNT(*) as query_count
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY warehouse_name, DATE_TRUNC('hour', start_time)
ORDER BY credits_used DESC
LIMIT 20;
```

### 3. Quick Triage Decision

**If P1 (Critical)**: Proceed to Emergency Actions
**If P2/P3**: Proceed to Investigation

## Emergency Actions (P1 Only)

### Option 1: Suspend Problem Warehouse

```sql
-- Suspend the warehouse immediately
ALTER WAREHOUSE <warehouse_name> SUSPEND;

-- Check impact
SELECT COUNT(*) as active_queries
FROM snowflake.account_usage.query_history
WHERE warehouse_name = '<warehouse_name>'
    AND end_time IS NULL;
```

**⚠️ Warning**: This will terminate active queries. Communicate with affected teams first if possible.

### Option 2: Reduce Warehouse Size

```sql
-- Downsize warehouse
ALTER WAREHOUSE <warehouse_name>
SET WAREHOUSE_SIZE = 'SMALL';
```

### Option 3: Apply Resource Monitor

```sql
-- Create emergency resource monitor
CREATE RESOURCE MONITOR emergency_limit
WITH CREDIT_QUOTA = 100
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 90 PERCENT DO SUSPEND;

-- Apply to warehouse
ALTER WAREHOUSE <warehouse_name>
SET RESOURCE_MONITOR = emergency_limit;
```

## Investigation (15-60 Minutes)

### Step 1: Identify Expensive Queries

```bash
# Analyze slow/expensive queries
python performance/query-profiling/analyze_slow_queries.py --days 1 --threshold 30
```

Or via SQL:

```sql
SELECT
    query_id,
    user_name,
    warehouse_name,
    total_elapsed_time / 1000.0 as seconds,
    partitions_scanned,
    bytes_scanned,
    LEFT(query_text, 100) as query_preview
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    AND warehouse_name = '<warehouse_name>'
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

### Step 2: Check for Abnormal Patterns

**Look for**:
- Runaway queries (hours-long execution)
- Query loops (same query repeated many times)
- Sudden increase in query volume
- Large table scans without filters
- Cartesian joins

```sql
-- Find repeated queries
SELECT
    query_text_checksum,
    COUNT(*) as execution_count,
    user_name,
    AVG(total_elapsed_time / 1000.0) as avg_seconds
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    AND warehouse_name = '<warehouse_name>'
GROUP BY query_text_checksum, user_name
HAVING COUNT(*) > 100
ORDER BY execution_count DESC;
```

### Step 3: Identify Responsible User/Application

```sql
SELECT
    user_name,
    COUNT(*) as query_count,
    SUM(total_elapsed_time / 1000.0) as total_seconds,
    SUM(credits_used_cloud_services) as cloud_services_credits
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    AND warehouse_name = '<warehouse_name>'
GROUP BY user_name
ORDER BY total_seconds DESC;
```

### Step 4: Check Warehouse Configuration Changes

```sql
-- Check recent warehouse modifications
SELECT
    warehouse_name,
    query_text,
    user_name,
    start_time
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%ALTER WAREHOUSE%'
    AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

## Root Cause Categories

### 1. Runaway Query

**Symptoms**:
- Single query running for hours
- Excessive partition scanning
- Disk spillage

**Response**:
```sql
-- Kill the query
SELECT SYSTEM$CANCEL_QUERY('<query_id>');

-- Review query execution plan
SELECT *
FROM TABLE(GET_QUERY_OPERATOR_STATS('<query_id>'));
```

**Prevention**:
- Implement statement timeout
- Add query complexity limits
- Require query review for large tables

### 2. Query Loop

**Symptoms**:
- Same query executed hundreds of times
- Short individual execution time
- Often from automated processes

**Response**:
1. Identify the source application/script
2. Check for infinite loops or missing exit conditions
3. Contact application owner
4. Temporarily revoke user access if needed

**Prevention**:
- Implement rate limiting
- Add circuit breakers to applications
- Monitor query frequency

### 3. Warehouse Misconfiguration

**Symptoms**:
- Warehouse size recently increased
- Auto-suspend disabled
- Warehouse left running idle

**Response**:
```sql
-- Fix configuration
ALTER WAREHOUSE <warehouse_name>
SET WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```

**Prevention**:
- Use Terraform for warehouse management
- Require approval for size changes
- Implement configuration monitoring

### 4. Unexpected Workload Increase

**Symptoms**:
- Many different queries
- Multiple users involved
- Legitimate business need

**Response**:
1. Enable multi-cluster if not already enabled
2. Set up queueing
3. Consider warehouse separation

```sql
ALTER WAREHOUSE <warehouse_name>
SET MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'ECONOMY';
```

### 5. Data Volume Growth

**Symptoms**:
- Same queries taking longer
- Increased bytes scanned
- More partitions scanned

**Response**:
1. Add or optimize clustering keys
2. Implement data retention policies
3. Consider table partitioning strategies

## Resolution Steps

### 1. Implement Immediate Fix

Based on root cause, apply appropriate solution from above.

### 2. Optimize the Workload

```bash
# Get right-sizing recommendations
python cost-optimization/right-sizing/recommend_sizes.py --warehouse <warehouse_name>

# Analyze query performance
python performance/query-profiling/analyze_slow_queries.py
```

### 3. Set Up Preventive Measures

**Resource Monitors**:
```sql
CREATE RESOURCE MONITOR <warehouse_name>_monitor
WITH CREDIT_QUOTA = <normal_daily_usage * 2>
    FREQUENCY = DAILY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;
```

**Query Timeout**:
```sql
ALTER WAREHOUSE <warehouse_name>
SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
```

**Enhanced Monitoring**:
```bash
# Set up automated alerts
python monitoring/alerts/setup_cost_alerts.py --threshold <daily_limit>
```

### 4. Document Incident

Create incident report with:
- Timeline of events
- Root cause analysis
- Estimated cost impact
- Resolution steps taken
- Prevention measures implemented

## Communication Templates

### For Users

```
Subject: Warehouse Suspension - Action Required

The <warehouse_name> warehouse has been suspended due to unexpected high costs.

Impact: Queries using this warehouse will fail until resolved.
Estimated Resolution: <time>
Alternative: Use <alternative_warehouse> for urgent queries.

We are investigating and will update within <timeframe>.
```

### For Management

```
Subject: Cost Spike Alert - Investigation In Progress

Summary:
- Cost spike detected at <time>
- Affected warehouse: <warehouse_name>
- Cost impact: $<amount> (vs $<normal> baseline)
- Current status: <status>
- ETA: <time>

Next steps: <actions>
```

## Post-Incident Review

Within 48 hours of resolution:

1. **Review Monitoring**: Were alerts timely and actionable?
2. **Update Runbooks**: Document any new learnings
3. **Improve Automation**: Enhance detection/response scripts
4. **Team Training**: Share lessons learned
5. **Process Improvements**: Update approval workflows if needed

## Useful Queries Reference

All queries referenced in this runbook are available in:
- `scripts/audit/cost_spike_analysis.sql`
- `scripts/audit/warehouse_health_check.sql`

## Tools Reference

- Cost analysis: `cost-optimization/warehouse-monitoring/`
- Performance profiling: `performance/query-profiling/`
- Alerting: `monitoring/alerts/`

## Escalation

**When to Escalate**:
- Unable to identify root cause within 1 hour
- Cost impact > $10,000
- Business-critical workloads affected
- Suspected security incident

**Escalation Path**:
1. Data Platform Lead
2. Engineering Manager
3. Snowflake Account Team (for platform issues)

## Quick Reference

```bash
# Most common investigation commands
python cost-optimization/warehouse-monitoring/analyze_usage.py --days 1
python performance/query-profiling/analyze_slow_queries.py --days 1 --threshold 30
python cost-optimization/idle-detection/find_idle_warehouses.py

# Suspend warehouse
snowsql -q "ALTER WAREHOUSE <name> SUSPEND;"

# Check warehouse status
snowsql -q "SHOW WAREHOUSES LIKE '<name>';"
```
