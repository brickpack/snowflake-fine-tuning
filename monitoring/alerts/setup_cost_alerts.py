"""
Cost Spike Alert System

Sets up monitoring and alerting for unusual cost spikes and patterns.

Usage:
    python setup_cost_alerts.py [--threshold 1000] [--lookback 24]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from rich.console import Console

sys.path.append(str(Path(__file__).parent.parent.parent))
from snowflake_utils import (
    SnowflakeConnection,
    calculate_cost,
    format_currency,
    send_alert,
    logger
)

console = Console()


def detect_cost_anomalies(sf: SnowflakeConnection, lookback_hours: int = 24) -> pd.DataFrame:
    """Detect cost anomalies using statistical analysis."""
    query = f"""
    WITH hourly_costs AS (
        SELECT
            DATE_TRUNC('hour', start_time) as hour,
            warehouse_name,
            SUM(credits_used) as credits_used
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
        GROUP BY DATE_TRUNC('hour', start_time), warehouse_name
    ),
    baseline AS (
        SELECT
            warehouse_name,
            AVG(credits_used) as avg_credits,
            STDDEV(credits_used) as stddev_credits,
            MAX(credits_used) as max_credits
        FROM hourly_costs
        WHERE hour < DATEADD(hour, -{lookback_hours // 4}, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name
    )
    SELECT
        hc.hour,
        hc.warehouse_name,
        hc.credits_used,
        b.avg_credits,
        b.stddev_credits,
        b.max_credits,
        CASE
            WHEN hc.credits_used > b.avg_credits + (3 * b.stddev_credits) THEN 'CRITICAL'
            WHEN hc.credits_used > b.avg_credits + (2 * b.stddev_credits) THEN 'WARNING'
            ELSE 'NORMAL'
        END as anomaly_level
    FROM hourly_costs hc
    JOIN baseline b ON hc.warehouse_name = b.warehouse_name
    WHERE hc.hour >= DATEADD(hour, -{lookback_hours // 4}, CURRENT_TIMESTAMP())
    ORDER BY hc.hour DESC, hc.credits_used DESC
    """

    df = sf.execute_query(query)
    df['cost'] = df['CREDITS_USED'].apply(calculate_cost)

    return df[df['ANOMALY_LEVEL'].isin(['CRITICAL', 'WARNING'])]


def check_daily_budget(sf: SnowflakeConnection, daily_threshold: float) -> dict:
    """Check if daily spending exceeds threshold."""
    query = """
    SELECT
        DATE(start_time) as date,
        SUM(credits_used) as total_credits
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    GROUP BY DATE(start_time)
    ORDER BY date DESC
    LIMIT 1
    """

    df = sf.execute_query(query)
    if df.empty:
        return {'exceeded': False, 'amount': 0}

    total_cost = calculate_cost(df['TOTAL_CREDITS'].iloc[0])
    exceeded = total_cost > daily_threshold

    return {
        'exceeded': exceeded,
        'amount': total_cost,
        'threshold': daily_threshold,
        'percentage': (total_cost / daily_threshold) * 100
    }


def main():
    parser = argparse.ArgumentParser(description='Monitor and alert on cost anomalies')
    parser.add_argument('--threshold', type=float, default=1000, help='Daily cost threshold in USD')
    parser.add_argument('--lookback', type=int, default=24, help='Hours to look back')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        # Check for anomalies
        anomalies = detect_cost_anomalies(sf, args.lookback)

        if not anomalies.empty:
            critical = anomalies[anomalies['ANOMALY_LEVEL'] == 'CRITICAL']
            if not critical.empty:
                for _, row in critical.iterrows():
                    message = f"CRITICAL: Cost spike detected in {row['WAREHOUSE_NAME']}. " \
                             f"Credits used: {row['CREDITS_USED']:.2f} (avg: {row['AVG_CREDITS']:.2f}). " \
                             f"Cost: {format_currency(row['cost'])}"
                    send_alert(message, 'CRITICAL')
                    console.print(f"[red]{message}[/red]")

        # Check daily budget
        budget_check = check_daily_budget(sf, args.threshold)
        if budget_check['exceeded']:
            message = f"Daily budget exceeded: {format_currency(budget_check['amount'])} " \
                     f"({budget_check['percentage']:.1f}% of ${args.threshold})"
            send_alert(message, 'WARNING')
            console.print(f"[yellow]{message}[/yellow]")
        else:
            console.print(f"[green]No cost anomalies detected. Current daily spend: {format_currency(budget_check['amount'])}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Alert check failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
