"""
Idle Warehouse Detection Tool

Identifies warehouses that are idle, underutilized, or have inefficient
auto-suspend settings. Helps eliminate waste from unused resources.

Usage:
    python find_idle_warehouses.py [--threshold 30] [--suspend]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm

sys.path.append(str(Path(__file__).parent.parent))
from snowflake_utils import (
    SnowflakeConnection,
    calculate_cost,
    format_currency,
    get_idle_warehouse_query,
    parse_warehouse_size,
    logger
)

console = Console()


def find_idle_warehouses(sf: SnowflakeConnection, idle_threshold_minutes: int = 30) -> pd.DataFrame:
    """Find warehouses that are idle or have been unused."""
    logger.info(f"Finding warehouses idle for more than {idle_threshold_minutes} minutes...")

    query = get_idle_warehouse_query(idle_threshold_minutes)
    df = sf.execute_query(query)

    if df.empty:
        return df

    # Calculate potential waste
    df['size_credits_per_hour'] = df['WAREHOUSE_SIZE'].apply(parse_warehouse_size)

    # Estimate monthly cost if left running
    df['monthly_waste_if_running'] = df['size_credits_per_hour'].apply(
        lambda x: calculate_cost(x * 24 * 30)  # 24 hours * 30 days
    )

    # Categorize idle status
    def categorize_idle_status(row):
        if pd.isna(row['MINUTES_SINCE_LAST_USE']):
            return 'NEVER_USED'
        elif row['MINUTES_SINCE_LAST_USE'] > 10080:  # 7 days
            return 'ABANDONED'
        elif row['MINUTES_SINCE_LAST_USE'] > 1440:  # 24 hours
            return 'VERY_IDLE'
        else:
            return 'IDLE'

    df['idle_status'] = df.apply(categorize_idle_status, axis=1)

    return df


def analyze_auto_suspend_settings(sf: SnowflakeConnection) -> pd.DataFrame:
    """Analyze auto-suspend configurations and identify opportunities."""
    logger.info("Analyzing auto-suspend settings...")

    query = """
    WITH query_gaps AS (
        SELECT
            warehouse_name,
            start_time,
            DATEDIFF(second,
                LAG(start_time) OVER (PARTITION BY warehouse_name ORDER BY start_time),
                start_time
            ) as seconds_since_last_query
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
    ),
    recent_usage AS (
        SELECT
            warehouse_name,
            COUNT(*) as query_count,
            AVG(seconds_since_last_query) as avg_seconds_between_queries,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY seconds_since_last_query) as median_seconds_between_queries
        FROM query_gaps
        WHERE seconds_since_last_query IS NOT NULL
        GROUP BY warehouse_name
    ),
    idle_time_waste AS (
        SELECT
            wm.warehouse_name,
            SUM(CASE
                WHEN wm.credits_used_compute = 0
                    AND wm.credits_used_cloud_services > 0
                THEN wm.credits_used_cloud_services
                ELSE 0
            END) as idle_credits_last_7d
        FROM snowflake.account_usage.warehouse_metering_history wm
        WHERE wm.start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        GROUP BY wm.warehouse_name
    ),
    warehouse_metadata AS (
        SELECT
            warehouse_name,
            MAX(warehouse_size) as warehouse_size,
            MIN(start_time) as first_seen
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
        GROUP BY warehouse_name
    )
    SELECT
        COALESCE(wm.warehouse_name, ru.warehouse_name, itw.warehouse_name) as warehouse_name,
        COALESCE(wm.warehouse_size, 'UNKNOWN') as warehouse_size,
        NULL as auto_suspend_seconds,
        NULL as auto_resume_enabled,
        'UNKNOWN' as current_state,
        NULL as warehouse_comment,
        COALESCE(ru.query_count, 0) as queries_last_7d,
        ru.avg_seconds_between_queries,
        ru.median_seconds_between_queries,
        COALESCE(itw.idle_credits_last_7d, 0) as idle_credits_last_7d,
        wm.first_seen as warehouse_created,
        NULL as warehouse_owner
    FROM warehouse_metadata wm
    FULL OUTER JOIN recent_usage ru ON wm.warehouse_name = ru.warehouse_name
    FULL OUTER JOIN idle_time_waste itw ON COALESCE(wm.warehouse_name, ru.warehouse_name) = itw.warehouse_name
    ORDER BY idle_credits_last_7d DESC NULLS LAST
    """

    df = sf.execute_query(query)

    if df.empty:
        return df

    # Calculate optimal auto-suspend time
    def calculate_optimal_suspend(row):
        if pd.isna(row['MEDIAN_SECONDS_BETWEEN_QUERIES']) or row['QUERIES_LAST_7D'] < 10:
            return 300  # 5 minutes default for low-usage warehouses

        median_gap = row['MEDIAN_SECONDS_BETWEEN_QUERIES']

        # If queries are very frequent, short suspend time
        if median_gap < 60:
            return 60
        elif median_gap < 300:
            return 180
        elif median_gap < 600:
            return 300
        else:
            return 600  # Max recommended: 10 minutes

    df['optimal_auto_suspend'] = df.apply(calculate_optimal_suspend, axis=1)

    # Calculate waste from suboptimal settings
    df['auto_suspend_minutes'] = df['AUTO_SUSPEND_SECONDS'] / 60
    df['optimal_suspend_minutes'] = df['optimal_auto_suspend'] / 60

    df['suspend_savings_potential'] = df.apply(
        lambda row: calculate_cost(row['IDLE_CREDITS_LAST_7D'] or 0) * 4,  # Monthly estimate
        axis=1
    )

    # Identify issues
    def identify_suspend_issue(row):
        issues = []

        if pd.isna(row['AUTO_SUSPEND_SECONDS']) or row['AUTO_SUSPEND_SECONDS'] is None:
            issues.append("Auto-suspend disabled")
        elif row['AUTO_SUSPEND_SECONDS'] > 600:
            issues.append(f"Auto-suspend too long ({row['auto_suspend_minutes']:.0f}m)")
        elif row['AUTO_SUSPEND_SECONDS'] < 60:
            issues.append(f"Auto-suspend very aggressive ({row['AUTO_SUSPEND_SECONDS']}s)")

        if not row['AUTO_RESUME_ENABLED']:
            issues.append("Auto-resume disabled")

        if row['IDLE_CREDITS_LAST_7D'] and row['IDLE_CREDITS_LAST_7D'] > 1:
            issues.append(f"Idle waste: {row['IDLE_CREDITS_LAST_7D']:.1f} credits")

        if not row['QUERIES_LAST_7D'] or row['QUERIES_LAST_7D'] < 10:
            issues.append("Very low usage")

        return ' | '.join(issues) if issues else 'OK'

    df['issues'] = df.apply(identify_suspend_issue, axis=1)

    return df


def display_idle_report(idle_df: pd.DataFrame, suspend_df: pd.DataFrame) -> None:
    """Display comprehensive idle warehouse report."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Idle Warehouse Detection Report[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Idle warehouses summary
    if not idle_df.empty:
        console.print("\n[bold red]Idle Warehouses:[/bold red]")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Warehouse", style="cyan")
        table.add_column("Size", justify="center")
        table.add_column("Status", justify="center")
        table.add_column("Last Used", justify="right")
        table.add_column("State", justify="center")
        table.add_column("Monthly Waste", justify="right")
        table.add_column("7d Credits", justify="right")

        for _, row in idle_df.iterrows():
            status_color = {
                'NEVER_USED': 'red',
                'ABANDONED': 'red',
                'VERY_IDLE': 'yellow',
                'IDLE': 'yellow'
            }.get(row['idle_status'], 'white')

            last_used = 'Never' if pd.isna(row['MINUTES_SINCE_LAST_USE']) else f"{int(row['MINUTES_SINCE_LAST_USE'])}m ago"
            monthly_waste = format_currency(row['monthly_waste_if_running'])
            credits = f"{row['TOTAL_CREDITS']:.1f}" if pd.notna(row['TOTAL_CREDITS']) else "0.0"

            table.add_row(
                row['WAREHOUSE_NAME'],
                row['WAREHOUSE_SIZE'],
                f"[{status_color}]{row['idle_status']}[/{status_color}]",
                last_used,
                row['CURRENT_STATE'],
                monthly_waste,
                credits
            )

        console.print(table)

        # Summary stats
        total_potential_waste = idle_df['monthly_waste_if_running'].sum()
        console.print(f"\n[bold]Total Idle Warehouses:[/bold] {len(idle_df)}")
        console.print(f"[bold red]Potential Monthly Waste if Running:[/bold red] {format_currency(total_potential_waste)}")

        # Recommendations
        never_used = len(idle_df[idle_df['idle_status'] == 'NEVER_USED'])
        abandoned = len(idle_df[idle_df['idle_status'] == 'ABANDONED'])

        if never_used > 0:
            console.print(f"\n[red]⚠ {never_used} warehouse(s) have NEVER been used - consider deleting[/red]")
        if abandoned > 0:
            console.print(f"[yellow]⚠ {abandoned} warehouse(s) haven't been used in 7+ days - consider suspending[/yellow]")

    # Auto-suspend analysis
    if not suspend_df.empty:
        console.print("\n[bold]Auto-Suspend Configuration Analysis:[/bold]")

        issues_df = suspend_df[suspend_df['issues'] != 'OK']

        if not issues_df.empty:
            table = Table(show_header=True, header_style="bold magenta", show_lines=True)
            table.add_column("Warehouse", style="cyan", width=20)
            table.add_column("Current\nSuspend", justify="center", width=10)
            table.add_column("Optimal\nSuspend", justify="center", width=10)
            table.add_column("Queries\n(7d)", justify="right", width=10)
            table.add_column("Idle\nWaste", justify="right", width=10)
            table.add_column("Issues", width=35)

            for _, row in issues_df.head(15).iterrows():
                current_suspend = f"{row['auto_suspend_minutes']:.0f}m" if pd.notna(row['AUTO_SUSPEND_SECONDS']) else "OFF"
                optimal_suspend = f"{row['optimal_suspend_minutes']:.0f}m"
                queries = f"{int(row['QUERIES_LAST_7D'])}" if pd.notna(row['QUERIES_LAST_7D']) else "0"
                idle_waste = format_currency(row['suspend_savings_potential'])

                table.add_row(
                    row['WAREHOUSE_NAME'],
                    current_suspend,
                    optimal_suspend,
                    queries,
                    idle_waste,
                    row['issues'][:45] + "..." if len(row['issues']) > 45 else row['issues']
                )

            console.print(table)

            total_suspend_savings = suspend_df['suspend_savings_potential'].sum()
            console.print(f"\n[bold green]Potential Monthly Savings from Auto-Suspend Optimization:[/bold green] {format_currency(total_suspend_savings)}")

    console.print("\n")


def suspend_idle_warehouses(sf: SnowflakeConnection, idle_df: pd.DataFrame,
                           status_filter: list = None) -> None:
    """Suspend idle warehouses."""
    if status_filter is None:
        status_filter = ['ABANDONED', 'VERY_IDLE']

    to_suspend = idle_df[
        (idle_df['idle_status'].isin(status_filter)) &
        (idle_df['CURRENT_STATE'] != 'SUSPENDED')
    ]

    if to_suspend.empty:
        console.print("[yellow]No warehouses to suspend[/yellow]")
        return

    console.print(f"\n[bold]Will suspend {len(to_suspend)} warehouse(s):[/bold]")
    for _, row in to_suspend.iterrows():
        console.print(f"  • {row['WAREHOUSE_NAME']} ({row['idle_status']}) - last used {row['MINUTES_SINCE_LAST_USE']:.0f}m ago")

    if not Confirm.ask("\nProceed with suspending these warehouses?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    statements = []
    for _, row in to_suspend.iterrows():
        stmt = f"ALTER WAREHOUSE {row['WAREHOUSE_NAME']} SUSPEND"
        statements.append(stmt)

    try:
        sf.execute_script(statements)
        console.print(f"[green]Successfully suspended {len(statements)} warehouse(s)[/green]")
    except Exception as e:
        console.print(f"[red]Failed to suspend warehouses: {e}[/red]")
        raise


def optimize_auto_suspend(sf: SnowflakeConnection, suspend_df: pd.DataFrame) -> None:
    """Apply optimal auto-suspend settings."""
    to_optimize = suspend_df[
        (suspend_df['issues'] != 'OK') &
        (suspend_df['suspend_savings_potential'] > 10)  # At least $10/month savings
    ]

    if to_optimize.empty:
        console.print("[yellow]No auto-suspend optimizations needed[/yellow]")
        return

    console.print(f"\n[bold]Will optimize auto-suspend for {len(to_optimize)} warehouse(s):[/bold]")
    for _, row in to_optimize.iterrows():
        current = f"{row['auto_suspend_minutes']:.0f}m" if pd.notna(row['AUTO_SUSPEND_SECONDS']) else "OFF"
        optimal = f"{row['optimal_suspend_minutes']:.0f}m"
        console.print(f"  • {row['WAREHOUSE_NAME']}: {current} → {optimal}")

    if not Confirm.ask("\nProceed with optimization?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    statements = []
    for _, row in to_optimize.iterrows():
        stmt = f"""
        ALTER WAREHOUSE {row['WAREHOUSE_NAME']}
        SET AUTO_SUSPEND = {int(row['optimal_auto_suspend'])}
            AUTO_RESUME = TRUE
        """
        statements.append(stmt)

    try:
        sf.execute_script(statements)
        console.print(f"[green]Successfully optimized {len(statements)} warehouse(s)[/green]")
    except Exception as e:
        console.print(f"[red]Failed to optimize: {e}[/red]")
        raise


def main():
    parser = argparse.ArgumentParser(description='Find and manage idle Snowflake warehouses')
    parser.add_argument('--threshold', type=int, default=30,
                       help='Idle threshold in minutes (default: 30)')
    parser.add_argument('--suspend', action='store_true',
                       help='Suspend abandoned/very idle warehouses')
    parser.add_argument('--optimize-suspend', action='store_true',
                       help='Optimize auto-suspend settings')
    parser.add_argument('--output', type=str, help='Save report to CSV file')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        console.print("[cyan]Analyzing warehouse idle status...[/cyan]")

        # Find idle warehouses
        idle_df = find_idle_warehouses(sf, args.threshold)

        # Analyze auto-suspend settings
        suspend_df = analyze_auto_suspend_settings(sf)

        # Display report
        display_idle_report(idle_df, suspend_df)

        # Save output if requested
        if args.output:
            output_path = Path(args.output)
            with pd.ExcelWriter(output_path.with_suffix('.xlsx')) as writer:
                idle_df.to_excel(writer, sheet_name='Idle Warehouses', index=False)
                suspend_df.to_excel(writer, sheet_name='Auto-Suspend Analysis', index=False)
            console.print(f"[green]Report saved to {output_path}[/green]")

        # Take action if requested
        if args.suspend and not idle_df.empty:
            suspend_idle_warehouses(sf, idle_df)

        if args.optimize_suspend and not suspend_df.empty:
            optimize_auto_suspend(sf, suspend_df)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
