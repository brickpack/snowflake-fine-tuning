"""
Common utilities for Snowflake cost optimization scripts.
Provides connection management, query execution, and cost calculation helpers.
"""

import os
import sys
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv
import logging
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SnowflakeConnection:
    """Manages Snowflake database connections with connection pooling."""

    def __init__(self):
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        }

        # Validate required parameters
        required = ['account', 'user', 'password']
        missing = [k for k in required if not self.connection_params.get(k)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake connections."""
        conn = None
        try:
            logger.info(f"Connecting to Snowflake account: {self.connection_params['account']}")
            conn = snowflake.connector.connect(**self.connection_params)
            yield conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Snowflake connection closed")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame."""
        with self.get_connection() as conn:
            try:
                logger.debug(f"Executing query: {query[:100]}...")
                cursor = conn.cursor(DictCursor)

                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetch results
                results = cursor.fetchall()
                df = pd.DataFrame(results)

                logger.info(f"Query returned {len(df)} rows")
                return df
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                raise
            finally:
                cursor.close()

    def execute_script(self, queries: List[str]) -> None:
        """Execute multiple queries in sequence."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for i, query in enumerate(queries):
                    logger.info(f"Executing query {i+1}/{len(queries)}")
                    cursor.execute(query)
                logger.info(f"Successfully executed {len(queries)} queries")
            except Exception as e:
                logger.error(f"Script execution failed: {e}")
                raise
            finally:
                cursor.close()


def get_warehouse_credit_cost() -> float:
    """
    Get the cost per credit for the Snowflake account.
    This should be configured based on your pricing tier.
    """
    # Default costs (adjust based on your contract)
    # Standard Edition: $2/credit
    # Enterprise Edition: $3/credit
    # Business Critical: $4/credit
    return float(os.getenv('SNOWFLAKE_CREDIT_COST', '3.0'))


def calculate_cost(credits_used: float) -> float:
    """Calculate dollar cost from credits used."""
    return credits_used * get_warehouse_credit_cost()


def format_currency(amount: float) -> str:
    """Format a number as USD currency."""
    return f"${amount:,.2f}"


def format_percentage(value: float) -> str:
    """Format a decimal as a percentage."""
    return f"{value * 100:.1f}%"


def get_date_range(days: int = 30) -> tuple:
    """Get start and end dates for analysis period."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def parse_warehouse_size(size: str) -> int:
    """Convert warehouse size to number of credits per hour."""
    size_to_credits = {
        'X-SMALL': 1,
        'SMALL': 2,
        'MEDIUM': 4,
        'LARGE': 8,
        'X-LARGE': 16,
        'XX-LARGE': 32,
        'XXX-LARGE': 64,
        '4X-LARGE': 128,
        '5X-LARGE': 256,
        '6X-LARGE': 512,
    }
    return size_to_credits.get(size.upper(), 0)


def recommend_warehouse_size(credits_per_hour: float) -> str:
    """Recommend optimal warehouse size based on usage."""
    if credits_per_hour <= 1:
        return 'X-SMALL'
    elif credits_per_hour <= 2:
        return 'SMALL'
    elif credits_per_hour <= 4:
        return 'MEDIUM'
    elif credits_per_hour <= 8:
        return 'LARGE'
    elif credits_per_hour <= 16:
        return 'X-LARGE'
    elif credits_per_hour <= 32:
        return 'XX-LARGE'
    elif credits_per_hour <= 64:
        return 'XXX-LARGE'
    elif credits_per_hour <= 128:
        return '4X-LARGE'
    elif credits_per_hour <= 256:
        return '5X-LARGE'
    else:
        return '6X-LARGE'


def get_warehouse_usage_query(days: int = 30) -> str:
    """Generate query to analyze warehouse usage."""
    return f"""
    SELECT
        warehouse_name,
        DATE_TRUNC('hour', start_time) as hour,
        SUM(credits_used) as credits_used,
        COUNT(*) as query_count,
        AVG(execution_time / 1000.0) as avg_execution_seconds,
        MAX(execution_time / 1000.0) as max_execution_seconds,
        SUM(execution_time / 1000.0) as total_execution_seconds
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name, DATE_TRUNC('hour', start_time)
    ORDER BY warehouse_name, hour DESC
    """


def get_warehouse_cost_summary(days: int = 30) -> str:
    """Generate query for warehouse cost summary."""
    return f"""
    SELECT
        warehouse_name,
        SUM(credits_used) as total_credits,
        SUM(credits_used_compute) as compute_credits,
        SUM(credits_used_cloud_services) as cloud_services_credits,
        COUNT(DISTINCT DATE_TRUNC('day', start_time)) as active_days,
        MIN(start_time) as first_usage,
        MAX(end_time) as last_usage
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name
    ORDER BY total_credits DESC
    """


def get_idle_warehouse_query(idle_threshold_minutes: int = 30) -> str:
    """Generate query to find idle warehouses."""
    return f"""
    SELECT
        w.name as warehouse_name,
        w.size as warehouse_size,
        w.auto_suspend as auto_suspend_seconds,
        w.auto_resume as auto_resume_enabled,
        w.state as current_state,
        DATEDIFF(minute, wm.last_usage, CURRENT_TIMESTAMP()) as minutes_since_last_use,
        wm.total_credits
    FROM snowflake.account_usage.warehouses w
    LEFT JOIN (
        SELECT
            warehouse_name,
            MAX(end_time) as last_usage,
            SUM(credits_used) as total_credits
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name
    ) wm ON w.name = wm.warehouse_name
    WHERE w.deleted IS NULL
        AND (
            wm.last_usage IS NULL
            OR DATEDIFF(minute, wm.last_usage, CURRENT_TIMESTAMP()) > {idle_threshold_minutes}
        )
    ORDER BY minutes_since_last_use DESC NULLS FIRST
    """


def get_query_performance_query(days: int = 7) -> str:
    """Generate query to analyze query performance."""
    return f"""
    SELECT
        query_id,
        query_text,
        user_name,
        warehouse_name,
        warehouse_size,
        execution_status,
        total_elapsed_time / 1000.0 as execution_seconds,
        bytes_scanned,
        bytes_written,
        rows_produced,
        compilation_time / 1000.0 as compilation_seconds,
        queued_overload_time / 1000.0 as queued_seconds,
        credits_used_cloud_services,
        start_time
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
        AND warehouse_name IS NOT NULL
    ORDER BY total_elapsed_time DESC
    LIMIT 1000
    """


def send_alert(message: str, severity: str = 'INFO'):
    """
    Send alert via configured channels (email, Slack, PagerDuty).
    This is a placeholder - implement based on your alerting infrastructure.
    """
    logger.log(getattr(logging, severity), f"ALERT: {message}")

    # TODO: Implement actual alerting
    # - Email via SMTP
    # - Slack webhook
    # - PagerDuty API
    # - Custom webhook

    if os.getenv('DRY_RUN', 'false').lower() == 'true':
        logger.info(f"DRY RUN: Would send alert: {message}")
        return

    # Placeholder for actual implementation
    pass


if __name__ == "__main__":
    # Test connection
    try:
        sf = SnowflakeConnection()
        logger.info("Testing Snowflake connection...")
        df = sf.execute_query("SELECT CURRENT_VERSION() as version, CURRENT_ACCOUNT() as account")
        logger.info(f"Successfully connected! Snowflake version: {df['VERSION'].iloc[0]}")
        logger.info(f"Account: {df['ACCOUNT'].iloc[0]}")
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        sys.exit(1)
