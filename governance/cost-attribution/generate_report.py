"""
Cost Attribution Report Generator

Generates detailed cost attribution reports by user, role, warehouse, department,
and project to enable chargeback and showback models.

Usage:
    python generate_report.py [--days 30] [--group-by user|role|warehouse|tag] [--output OUTPUT]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

sys.path.append(str(Path(__file__).parent.parent.parent / 'cost-optimization'))
from snowflake_utils import (
    SnowflakeConnection,
    calculate_cost,
    format_currency,
    format_percentage,
    logger
)

console = Console()


def get_cost_by_user(sf: SnowflakeConnection, days: int = 30) -> pd.DataFrame:
    """Calculate costs attributed to each user."""
    logger.info(f"Calculating cost attribution by user for the last {days} days...")

    query = f"""
    WITH query_costs AS (
        SELECT
            qh.user_name,
            qh.role_name,
            qh.warehouse_name,
            qh.warehouse_size,
            qh.database_name,
            DATE_TRUNC('day', qh.start_time) as date,
            COUNT(*) as query_count,
            SUM(qh.total_elapsed_time) / 1000.0 as total_execution_seconds,
            SUM(qh.execution_time) / 1000.0 as total_query_time_seconds,
            SUM(qh.bytes_scanned) as total_bytes_scanned,
            SUM(qh.rows_produced) as total_rows_produced
        FROM snowflake.account_usage.query_history qh
        WHERE qh.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND qh.execution_status = 'SUCCESS'
            AND qh.warehouse_name IS NOT NULL
        GROUP BY qh.user_name, qh.role_name, qh.warehouse_name, qh.warehouse_size,
                 qh.database_name, DATE_TRUNC('day', qh.start_time)
    ),
    warehouse_costs AS (
        SELECT
            warehouse_name,
            DATE_TRUNC('day', start_time) as date,
            SUM(credits_used) as daily_credits
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name, DATE_TRUNC('day', start_time)
    ),
    daily_warehouse_queries AS (
        SELECT
            warehouse_name,
            date,
            SUM(total_execution_seconds) as total_warehouse_seconds
        FROM query_costs
        GROUP BY warehouse_name, date
    )
    SELECT
        qc.user_name,
        qc.role_name,
        qc.warehouse_name,
        qc.database_name,
        COUNT(DISTINCT qc.date) as active_days,
        SUM(qc.query_count) as total_queries,
        SUM(qc.total_execution_seconds) as total_execution_seconds,
        SUM(qc.total_bytes_scanned) as total_bytes_scanned,
        SUM(qc.total_rows_produced) as total_rows_produced,
        -- Proportional cost calculation
        SUM(
            (qc.total_execution_seconds / NULLIF(dwq.total_warehouse_seconds, 0)) * wc.daily_credits
        ) as attributed_credits
    FROM query_costs qc
    LEFT JOIN daily_warehouse_queries dwq
        ON qc.warehouse_name = dwq.warehouse_name AND qc.date = dwq.date
    LEFT JOIN warehouse_costs wc
        ON qc.warehouse_name = wc.warehouse_name AND qc.date = wc.date
    GROUP BY qc.user_name, qc.role_name, qc.warehouse_name, qc.database_name
    ORDER BY attributed_credits DESC NULLS LAST
    """

    df = sf.execute_query(query)

    if not df.empty:
        df['attributed_cost'] = df['ATTRIBUTED_CREDITS'].apply(lambda x: calculate_cost(float(x)) if pd.notna(x) else 0)

    return df


def get_cost_by_warehouse(sf: SnowflakeConnection, days: int = 30) -> pd.DataFrame:
    """Calculate costs by warehouse."""
    logger.info(f"Calculating cost attribution by warehouse for the last {days} days...")

    query = f"""
    SELECT
        warehouse_name,
        DATE_TRUNC('day', start_time) as date,
        SUM(credits_used) as credits_used,
        SUM(credits_used_compute) as compute_credits,
        SUM(credits_used_cloud_services) as cloud_services_credits,
        COUNT(DISTINCT DATE_TRUNC('hour', start_time)) as active_hours
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name, DATE_TRUNC('day', start_time)
    ORDER BY credits_used DESC
    """

    df = sf.execute_query(query)

    if not df.empty:
        df['cost'] = df['CREDITS_USED'].apply(lambda x: calculate_cost(float(x)))

    return df


def get_cost_by_database(sf: SnowflakeConnection, days: int = 30) -> pd.DataFrame:
    """Calculate costs attributed to each database."""
    logger.info(f"Calculating cost attribution by database for the last {days} days...")

    query = f"""
    WITH query_costs AS (
        SELECT
            qh.database_name,
            qh.warehouse_name,
            DATE_TRUNC('day', qh.start_time) as date,
            SUM(qh.total_elapsed_time) / 1000.0 as total_execution_seconds,
            COUNT(*) as query_count
        FROM snowflake.account_usage.query_history qh
        WHERE qh.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND qh.execution_status = 'SUCCESS'
            AND qh.database_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
        GROUP BY qh.database_name, qh.warehouse_name, DATE_TRUNC('day', qh.start_time)
    ),
    warehouse_costs AS (
        SELECT
            warehouse_name,
            DATE_TRUNC('day', start_time) as date,
            SUM(credits_used) as daily_credits
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name, DATE_TRUNC('day', start_time)
    ),
    daily_warehouse_queries AS (
        SELECT
            warehouse_name,
            date,
            SUM(total_execution_seconds) as total_warehouse_seconds
        FROM query_costs
        GROUP BY warehouse_name, date
    )
    SELECT
        qc.database_name,
        COUNT(DISTINCT qc.warehouse_name) as warehouses_used,
        SUM(qc.query_count) as total_queries,
        SUM(qc.total_execution_seconds) as total_execution_seconds,
        SUM(
            (qc.total_execution_seconds / NULLIF(dwq.total_warehouse_seconds, 0)) * wc.daily_credits
        ) as attributed_credits
    FROM query_costs qc
    LEFT JOIN daily_warehouse_queries dwq
        ON qc.warehouse_name = dwq.warehouse_name AND qc.date = dwq.date
    LEFT JOIN warehouse_costs wc
        ON qc.warehouse_name = wc.warehouse_name AND qc.date = wc.date
    GROUP BY qc.database_name
    ORDER BY attributed_credits DESC NULLS LAST
    """

    df = sf.execute_query(query)

    if not df.empty:
        df['attributed_cost'] = df['ATTRIBUTED_CREDITS'].apply(lambda x: calculate_cost(float(x)) if pd.notna(x) else 0)

    return df


def get_cost_trends(sf: SnowflakeConnection, days: int = 30, group_by: str = 'day') -> pd.DataFrame:
    """Get cost trends over time."""
    logger.info(f"Calculating cost trends for the last {days} days...")

    date_trunc = {
        'hour': 'hour',
        'day': 'day',
        'week': 'week',
        'month': 'month'
    }.get(group_by, 'day')

    query = f"""
    SELECT
        DATE_TRUNC('{date_trunc}', start_time) as period,
        warehouse_name,
        SUM(credits_used) as credits_used,
        SUM(credits_used_compute) as compute_credits,
        SUM(credits_used_cloud_services) as cloud_services_credits
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('{date_trunc}', start_time), warehouse_name
    ORDER BY period DESC, credits_used DESC
    """

    df = sf.execute_query(query)

    if not df.empty:
        df['cost'] = df['CREDITS_USED'].apply(lambda x: calculate_cost(float(x)))

    return df


def generate_summary_stats(user_df: pd.DataFrame, warehouse_df: pd.DataFrame,
                           database_df: pd.DataFrame) -> dict:
    """Generate summary statistics for the report."""
    # Handle column names case-insensitively
    cost_col = 'COST' if 'COST' in warehouse_df.columns else 'cost'
    credits_col = 'CREDITS_USED' if 'CREDITS_USED' in warehouse_df.columns else 'credits_used'

    total_cost = warehouse_df.groupby('WAREHOUSE_NAME')[cost_col].sum().sum() if not warehouse_df.empty and cost_col in warehouse_df.columns else 0
    total_credits = warehouse_df.groupby('WAREHOUSE_NAME')[credits_col].sum().sum() if not warehouse_df.empty and credits_col in warehouse_df.columns else 0

    stats = {
        'total_cost': total_cost,
        'total_credits': float(total_credits),
        'unique_users': len(user_df['USER_NAME'].unique()) if not user_df.empty else 0,
        'unique_warehouses': len(warehouse_df['WAREHOUSE_NAME'].unique()) if not warehouse_df.empty else 0,
        'unique_databases': len(database_df['DATABASE_NAME'].unique()) if not database_df.empty else 0,
        'total_queries': int(user_df['TOTAL_QUERIES'].sum()) if not user_df.empty else 0
    }

    return stats


def display_cost_attribution_report(user_df: pd.DataFrame, warehouse_df: pd.DataFrame,
                                    database_df: pd.DataFrame, trends_df: pd.DataFrame,
                                    stats: dict, days: int):
    """Display comprehensive cost attribution report."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Snowflake Cost Attribution Report[/bold cyan]",
        subtitle=f"Last {days} Days | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Summary
    console.print("\n[bold]Cost Summary:[/bold]")
    console.print(f"Total Cost: [bold green]{format_currency(stats['total_cost'])}[/bold green]")
    console.print(f"Total Credits: {stats['total_credits']:,.2f}")
    console.print(f"Active Users: {stats['unique_users']}")
    console.print(f"Active Warehouses: {stats['unique_warehouses']}")
    console.print(f"Active Databases: {stats['unique_databases']}")
    console.print(f"Total Queries: {stats['total_queries']:,}")

    # Top Cost by User
    if not user_df.empty:
        console.print("\n[bold]Top 15 Users by Cost:[/bold]")
        user_summary = user_df.groupby('USER_NAME').agg({
            'attributed_cost': 'sum',
            'TOTAL_QUERIES': 'sum',
            'TOTAL_EXECUTION_SECONDS': 'sum',
            'ATTRIBUTED_CREDITS': 'sum'
        }).reset_index()
        user_summary = user_summary.sort_values('attributed_cost', ascending=False).head(15)

        user_table = Table(show_header=True, header_style="bold cyan", box=box.ROUNDED)
        user_table.add_column("User", style="cyan")
        user_table.add_column("Cost", justify="right", style="green")
        user_table.add_column("Credits", justify="right")
        user_table.add_column("Queries", justify="right")
        user_table.add_column("Exec Time (hrs)", justify="right")
        user_table.add_column("% of Total", justify="right")

        for _, row in user_summary.iterrows():
            cost_pct = (row['attributed_cost'] / stats['total_cost'] * 100) if stats['total_cost'] > 0 else 0
            user_table.add_row(
                row['USER_NAME'],
                format_currency(row['attributed_cost']),
                f"{float(row['ATTRIBUTED_CREDITS']):,.2f}",
                f"{int(row['TOTAL_QUERIES']):,}",
                f"{float(row['TOTAL_EXECUTION_SECONDS']) / 3600:.1f}",
                f"{cost_pct:.1f}%"
            )

        console.print(user_table)

    # Cost by Warehouse
    if not warehouse_df.empty:
        console.print("\n[bold]Cost by Warehouse:[/bold]")
        # Handle column name case
        cost_col = 'cost' if 'cost' in warehouse_df.columns else 'COST'
        wh_summary = warehouse_df.groupby('WAREHOUSE_NAME').agg({
            cost_col: 'sum',
            'CREDITS_USED': 'sum',
            'COMPUTE_CREDITS': 'sum',
            'CLOUD_SERVICES_CREDITS': 'sum',
            'ACTIVE_HOURS': 'sum'
        }).reset_index()
        wh_summary = wh_summary.sort_values(cost_col, ascending=False).head(10)
        wh_summary.rename(columns={cost_col: 'COST'}, inplace=True)

        wh_table = Table(show_header=True, header_style="bold magenta", box=box.ROUNDED)
        wh_table.add_column("Warehouse", style="cyan")
        wh_table.add_column("Cost", justify="right", style="green")
        wh_table.add_column("Credits", justify="right")
        wh_table.add_column("Compute", justify="right")
        wh_table.add_column("Cloud Svc", justify="right")
        wh_table.add_column("Active Hrs", justify="right")
        wh_table.add_column("% of Total", justify="right")

        for _, row in wh_summary.iterrows():
            cost_pct = (row['COST'] / stats['total_cost'] * 100) if stats['total_cost'] > 0 else 0
            wh_table.add_row(
                row['WAREHOUSE_NAME'],
                format_currency(row['COST']),
                f"{float(row['CREDITS_USED']):,.2f}",
                f"{float(row['COMPUTE_CREDITS']):,.2f}",
                f"{float(row['CLOUD_SERVICES_CREDITS']):,.2f}",
                str(int(row['ACTIVE_HOURS'])),
                f"{cost_pct:.1f}%"
            )

        console.print(wh_table)

    # Cost by Database
    if not database_df.empty:
        console.print("\n[bold]Top 10 Databases by Cost:[/bold]")
        db_table = Table(show_header=True, header_style="bold yellow", box=box.ROUNDED)
        db_table.add_column("Database", style="cyan")
        db_table.add_column("Cost", justify="right", style="green")
        db_table.add_column("Credits", justify="right")
        db_table.add_column("Queries", justify="right")
        db_table.add_column("Warehouses", justify="right")
        db_table.add_column("% of Total", justify="right")

        for _, row in database_df.head(10).iterrows():
            cost = row['attributed_cost']
            cost_pct = (cost / stats['total_cost'] * 100) if stats['total_cost'] > 0 else 0
            db_table.add_row(
                row['DATABASE_NAME'],
                format_currency(cost),
                f"{float(row['ATTRIBUTED_CREDITS']):,.2f}",
                f"{int(row['TOTAL_QUERIES']):,}",
                str(int(row['WAREHOUSES_USED'])),
                f"{cost_pct:.1f}%"
            )

        console.print(db_table)

    # Cost Trends (Last 7 Days)
    if not trends_df.empty:
        console.print("\n[bold]Daily Cost Trend (Last 7 Days):[/bold]")
        cost_col = 'cost' if 'cost' in trends_df.columns else 'COST'
        daily_trends = trends_df.groupby('PERIOD')[cost_col].sum().reset_index()
        daily_trends = daily_trends.sort_values('PERIOD', ascending=False).head(7)
        daily_trends.rename(columns={cost_col: 'COST'}, inplace=True)

        trend_table = Table(show_header=True, header_style="bold blue", box=box.ROUNDED)
        trend_table.add_column("Date", style="cyan")
        trend_table.add_column("Cost", justify="right", style="green")
        trend_table.add_column("Credits", justify="right")

        for _, row in daily_trends.iterrows():
            credits = trends_df[trends_df['PERIOD'] == row['PERIOD']]['CREDITS_USED'].sum()
            trend_table.add_row(
                str(row['PERIOD']),
                format_currency(row['COST']),
                f"{float(credits):,.2f}"
            )

        console.print(trend_table)


def export_to_csv(user_df: pd.DataFrame, warehouse_df: pd.DataFrame,
                  database_df: pd.DataFrame, trends_df: pd.DataFrame, output_file: str):
    """Export all reports to CSV files."""
    base_name = output_file.rsplit('.', 1)[0]

    # Export each dataframe
    if not user_df.empty:
        user_summary = user_df.groupby('USER_NAME').agg({
            'attributed_cost': 'sum',
            'TOTAL_QUERIES': 'sum',
            'TOTAL_EXECUTION_SECONDS': 'sum',
            'ATTRIBUTED_CREDITS': 'sum'
        }).reset_index()
        user_summary.to_csv(f"{base_name}_by_user.csv", index=False)

    if not warehouse_df.empty:
        wh_summary = warehouse_df.groupby('WAREHOUSE_NAME').agg({
            'COST': 'sum',
            'CREDITS_USED': 'sum',
            'COMPUTE_CREDITS': 'sum',
            'CLOUD_SERVICES_CREDITS': 'sum'
        }).reset_index()
        wh_summary.to_csv(f"{base_name}_by_warehouse.csv", index=False)

    if not database_df.empty:
        database_df.to_csv(f"{base_name}_by_database.csv", index=False)

    if not trends_df.empty:
        trends_df.to_csv(f"{base_name}_trends.csv", index=False)

    console.print(f"\n[green]Reports exported to {base_name}_*.csv files[/green]")


def main():
    parser = argparse.ArgumentParser(description='Generate cost attribution reports')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to analyze (default: 30)')
    parser.add_argument('--group-by', choices=['user', 'role', 'warehouse', 'database', 'all'],
                       default='all', help='Group costs by dimension')
    parser.add_argument('--output', help='Export to CSV files (base filename)')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        # Gather cost attribution data
        user_df = pd.DataFrame()
        warehouse_df = pd.DataFrame()
        database_df = pd.DataFrame()
        trends_df = pd.DataFrame()

        if args.group_by in ['user', 'all']:
            user_df = get_cost_by_user(sf, args.days)

        if args.group_by in ['warehouse', 'all']:
            warehouse_df = get_cost_by_warehouse(sf, args.days)

        if args.group_by in ['database', 'all']:
            database_df = get_cost_by_database(sf, args.days)

        trends_df = get_cost_trends(sf, args.days)

        # Generate summary statistics
        stats = generate_summary_stats(user_df, warehouse_df, database_df)

        # Display report
        display_cost_attribution_report(user_df, warehouse_df, database_df,
                                       trends_df, stats, args.days)

        # Export if requested
        if args.output:
            export_to_csv(user_df, warehouse_df, database_df, trends_df, args.output)

    except Exception as e:
        logger.error(f"Cost attribution report failed: {e}")
        console.print(f"\n[red]Error: {e}[/red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
