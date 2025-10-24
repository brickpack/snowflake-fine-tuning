"""
Query Performance Analysis Tool

Identifies slow queries, analyzes execution patterns, and provides
optimization recommendations.

Usage:
    python analyze_slow_queries.py [--days 7] [--threshold 60] [--limit 100]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

sys.path.append(str(Path(__file__).parent.parent.parent / 'cost-optimization'))
from snowflake_utils import (
    SnowflakeConnection,
    format_currency,
    calculate_cost,
    logger
)

console = Console()


def analyze_slow_queries(sf: SnowflakeConnection, days: int = 7, threshold_seconds: int = 60, limit: int = 100) -> pd.DataFrame:
    """Identify and analyze slow-running queries."""
    logger.info(f"Analyzing queries slower than {threshold_seconds}s from the last {days} days...")

    query = f"""
    SELECT
        query_id,
        query_text,
        user_name,
        role_name,
        warehouse_name,
        warehouse_size,
        database_name,
        schema_name,
        execution_status,

        -- Timing metrics
        start_time,
        end_time,
        total_elapsed_time / 1000.0 as total_seconds,
        execution_time / 1000.0 as execution_seconds,
        compilation_time / 1000.0 as compilation_seconds,
        queued_provisioning_time / 1000.0 as queued_provisioning_seconds,
        queued_repair_time / 1000.0 as queued_repair_seconds,
        queued_overload_time / 1000.0 as queued_overload_seconds,

        -- Resource usage
        bytes_scanned,
        bytes_written,
        bytes_deleted,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        rows_produced,
        rows_inserted,
        rows_updated,
        rows_deleted,
        partitions_scanned,
        partitions_total,

        -- Cost
        credits_used_cloud_services,

        -- Query characteristics
        query_type,
        query_tag,
        transaction_blocked_time / 1000.0 as transaction_blocked_seconds

    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
        AND total_elapsed_time / 1000.0 >= {threshold_seconds}
    ORDER BY total_elapsed_time DESC
    LIMIT {limit}
    """

    return sf.execute_query(query)


def categorize_performance_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Categorize queries by performance issue type."""

    def identify_issues(row):
        issues = []

        # Spilling issues
        if row['BYTES_SPILLED_TO_LOCAL_STORAGE'] and row['BYTES_SPILLED_TO_LOCAL_STORAGE'] > 0:
            spill_gb = row['BYTES_SPILLED_TO_LOCAL_STORAGE'] / (1024**3)
            issues.append(f"Local spill: {spill_gb:.2f}GB")

        if row['BYTES_SPILLED_TO_REMOTE_STORAGE'] and row['BYTES_SPILLED_TO_REMOTE_STORAGE'] > 0:
            spill_gb = row['BYTES_SPILLED_TO_REMOTE_STORAGE'] / (1024**3)
            issues.append(f"Remote spill: {spill_gb:.2f}GB (SEVERE)")

        # Compilation time
        comp_pct = (row['COMPILATION_SECONDS'] / row['TOTAL_SECONDS'] * 100) if row['TOTAL_SECONDS'] > 0 else 0
        if comp_pct > 20:
            issues.append(f"High compilation: {comp_pct:.1f}%")

        # Queueing
        total_queue = (
            (row['QUEUED_PROVISIONING_SECONDS'] or 0) +
            (row['QUEUED_REPAIR_SECONDS'] or 0) +
            (row['QUEUED_OVERLOAD_SECONDS'] or 0)
        )
        if total_queue > 5:
            issues.append(f"Queued: {total_queue:.1f}s")

        # Partition pruning
        if row['PARTITIONS_TOTAL'] and row['PARTITIONS_SCANNED']:
            scan_pct = (row['PARTITIONS_SCANNED'] / row['PARTITIONS_TOTAL'] * 100)
            if scan_pct > 50 and row['PARTITIONS_TOTAL'] > 100:
                issues.append(f"Poor pruning: {scan_pct:.1f}% partitions scanned")

        # Large scans
        if row['BYTES_SCANNED']:
            scan_gb = row['BYTES_SCANNED'] / (1024**3)
            if scan_gb > 100:
                issues.append(f"Large scan: {scan_gb:.1f}GB")

        # Transaction blocking
        if row['TRANSACTION_BLOCKED_SECONDS'] and row['TRANSACTION_BLOCKED_SECONDS'] > 5:
            issues.append(f"Transaction blocked: {row['TRANSACTION_BLOCKED_SECONDS']:.1f}s")

        return ' | '.join(issues) if issues else 'General slowness'

    df['performance_issues'] = df.apply(identify_issues, axis=1)

    # Categorize primary issue
    def primary_category(row):
        issues = row['performance_issues']
        if 'Remote spill' in issues:
            return 'SPILLING_REMOTE'
        elif 'Local spill' in issues:
            return 'SPILLING_LOCAL'
        elif 'High compilation' in issues:
            return 'COMPILATION'
        elif 'Queued' in issues:
            return 'QUEUEING'
        elif 'Poor pruning' in issues:
            return 'PARTITION_PRUNING'
        elif 'Large scan' in issues:
            return 'LARGE_SCAN'
        elif 'Transaction blocked' in issues:
            return 'LOCKING'
        else:
            return 'OTHER'

    df['primary_issue'] = df.apply(primary_category, axis=1)

    return df


def generate_optimization_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """Generate specific optimization recommendations."""
    recommendations = []

    for _, row in df.iterrows():
        query_id = row['QUERY_ID']
        primary_issue = row['primary_issue']
        query_text = row['QUERY_TEXT'][:200] + "..." if len(row['QUERY_TEXT']) > 200 else row['QUERY_TEXT']

        # Generate recommendations based on issue type
        if primary_issue == 'SPILLING_REMOTE':
            rec = {
                'query_id': query_id,
                'issue': 'Remote Disk Spilling',
                'severity': 'CRITICAL',
                'impact': f"{row['TOTAL_SECONDS']:.0f}s execution time",
                'recommendations': [
                    "1. Increase warehouse size for more memory",
                    "2. Add WHERE clauses to reduce data processed",
                    "3. Break query into smaller CTEs",
                    "4. Review JOIN order and types"
                ],
                'query_snippet': query_text
            }

        elif primary_issue == 'SPILLING_LOCAL':
            rec = {
                'query_id': query_id,
                'issue': 'Local Disk Spilling',
                'severity': 'HIGH',
                'impact': f"{row['TOTAL_SECONDS']:.0f}s execution time",
                'recommendations': [
                    "1. Consider increasing warehouse size",
                    "2. Optimize GROUP BY and ORDER BY clauses",
                    "3. Reduce number of columns selected",
                    "4. Use LIMIT where appropriate"
                ],
                'query_snippet': query_text
            }

        elif primary_issue == 'COMPILATION':
            rec = {
                'query_id': query_id,
                'issue': 'High Compilation Time',
                'severity': 'MEDIUM',
                'impact': f"{row['COMPILATION_SECONDS']:.0f}s compilation",
                'recommendations': [
                    "1. Simplify complex expressions",
                    "2. Reduce number of CTEs",
                    "3. Use query result caching",
                    "4. Consider materializing complex views"
                ],
                'query_snippet': query_text
            }

        elif primary_issue == 'PARTITION_PRUNING':
            rec = {
                'query_id': query_id,
                'issue': 'Poor Partition Pruning',
                'severity': 'HIGH',
                'impact': f"{row['PARTITIONS_SCANNED']}/{row['PARTITIONS_TOTAL']} partitions scanned",
                'recommendations': [
                    "1. Add clustering keys on filter columns",
                    "2. Use DATE/TIMESTAMP filters effectively",
                    "3. Ensure predicates match clustering keys",
                    "4. Consider table partitioning strategy"
                ],
                'query_snippet': query_text
            }

        elif primary_issue == 'LARGE_SCAN':
            scan_gb = row['BYTES_SCANNED'] / (1024**3)
            rec = {
                'query_id': query_id,
                'issue': 'Large Table Scan',
                'severity': 'MEDIUM',
                'impact': f"{scan_gb:.1f}GB scanned",
                'recommendations': [
                    "1. Add WHERE clauses to reduce scan size",
                    "2. Use incremental/delta processing",
                    "3. Create materialized views for frequent patterns",
                    "4. Consider table clustering"
                ],
                'query_snippet': query_text
            }

        elif primary_issue == 'QUEUEING':
            total_queue = (
                (row['QUEUED_PROVISIONING_SECONDS'] or 0) +
                (row['QUEUED_REPAIR_SECONDS'] or 0) +
                (row['QUEUED_OVERLOAD_SECONDS'] or 0)
            )
            rec = {
                'query_id': query_id,
                'issue': 'Query Queueing',
                'severity': 'MEDIUM',
                'impact': f"{total_queue:.0f}s queued",
                'recommendations': [
                    "1. Enable multi-cluster warehouses",
                    "2. Separate workloads to different warehouses",
                    "3. Implement query prioritization",
                    "4. Schedule heavy queries for off-peak"
                ],
                'query_snippet': query_text
            }

        else:
            rec = {
                'query_id': query_id,
                'issue': 'General Performance',
                'severity': 'LOW',
                'impact': f"{row['TOTAL_SECONDS']:.0f}s execution time",
                'recommendations': [
                    "1. Review query execution plan",
                    "2. Check for missing indexes/clustering",
                    "3. Optimize JOIN conditions",
                    "4. Consider warehouse size"
                ],
                'query_snippet': query_text
            }

        recommendations.append(rec)

    return pd.DataFrame(recommendations)


def display_performance_report(df: pd.DataFrame, recommendations_df: pd.DataFrame) -> None:
    """Display comprehensive performance report."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Query Performance Analysis Report[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Summary by issue type
    console.print("\n[bold]Performance Issues Summary:[/bold]")
    issue_counts = df['primary_issue'].value_counts()

    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("Issue Type", style="cyan")
    summary_table.add_column("Count", justify="right")
    summary_table.add_column("Avg Time", justify="right")
    summary_table.add_column("Total Time", justify="right")

    for issue, count in issue_counts.items():
        issue_df = df[df['primary_issue'] == issue]
        avg_time = issue_df['TOTAL_SECONDS'].mean()
        total_time = issue_df['TOTAL_SECONDS'].sum()

        summary_table.add_row(
            issue.replace('_', ' ').title(),
            str(count),
            f"{avg_time:.1f}s",
            f"{total_time:.1f}s"
        )

    console.print(summary_table)

    # Top slow queries
    console.print("\n[bold]Top 10 Slowest Queries:[/bold]")
    queries_table = Table(show_header=True, header_style="bold magenta", show_lines=True)
    queries_table.add_column("Query ID", style="cyan", width=15)
    queries_table.add_column("Time", justify="right", width=8)
    queries_table.add_column("Warehouse", width=12)
    queries_table.add_column("Primary Issue", width=18)
    queries_table.add_column("Details", width=50)

    for _, row in df.head(10).iterrows():
        severity_color = {
            'SPILLING_REMOTE': 'red',
            'SPILLING_LOCAL': 'yellow',
            'PARTITION_PRUNING': 'yellow',
            'COMPILATION': 'cyan',
            'OTHER': 'white'
        }.get(row['primary_issue'], 'white')

        queries_table.add_row(
            row['QUERY_ID'][:13] + "...",
            f"{row['TOTAL_SECONDS']:.0f}s",
            row['WAREHOUSE_NAME'] or 'N/A',
            f"[{severity_color}]{row['primary_issue'].replace('_', ' ')}[/{severity_color}]",
            row['performance_issues'][:60] + "..." if len(row['performance_issues']) > 60 else row['performance_issues']
        )

    console.print(queries_table)

    # Top recommendations
    console.print("\n[bold]Top Optimization Recommendations:[/bold]")

    critical_recs = recommendations_df[recommendations_df['severity'] == 'CRITICAL'].head(5)
    if not critical_recs.empty:
        console.print("\n[bold red]CRITICAL Issues:[/bold red]")
        for _, rec in critical_recs.iterrows():
            console.print(f"\n[red]• {rec['issue']}[/red] (Query: {rec['query_id'][:13]}...)")
            console.print(f"  Impact: {rec['impact']}")
            for recommendation in rec['recommendations']:
                console.print(f"  {recommendation}")

    high_recs = recommendations_df[recommendations_df['severity'] == 'HIGH'].head(5)
    if not high_recs.empty:
        console.print("\n[bold yellow]HIGH Priority Issues:[/bold yellow]")
        for _, rec in high_recs.iterrows():
            console.print(f"\n[yellow]• {rec['issue']}[/yellow] (Query: {rec['query_id'][:13]}...)")
            console.print(f"  Impact: {rec['impact']}")
            for recommendation in rec['recommendations'][:2]:  # Top 2 recommendations
                console.print(f"  {recommendation}")

    console.print("\n")


def main():
    parser = argparse.ArgumentParser(description='Analyze slow query performance')
    parser.add_argument('--days', type=int, default=7, help='Number of days to analyze (default: 7)')
    parser.add_argument('--threshold', type=int, default=60, help='Slow query threshold in seconds (default: 60)')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of queries to analyze (default: 100)')
    parser.add_argument('--output', type=str, help='Save report to CSV/Excel file')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        console.print(f"[cyan]Analyzing queries slower than {args.threshold}s from the last {args.days} days...[/cyan]")

        # Analyze slow queries
        df = analyze_slow_queries(sf, args.days, args.threshold, args.limit)

        if df.empty:
            console.print("[green]No slow queries found! System is performing well.[/green]")
            return

        # Categorize issues
        df = categorize_performance_issues(df)

        # Generate recommendations
        recommendations_df = generate_optimization_recommendations(df)

        # Display report
        display_performance_report(df, recommendations_df)

        # Save output if requested
        if args.output:
            output_path = Path(args.output)
            with pd.ExcelWriter(output_path.with_suffix('.xlsx')) as writer:
                df.to_excel(writer, sheet_name='Slow Queries', index=False)
                recommendations_df.to_excel(writer, sheet_name='Recommendations', index=False)
            console.print(f"[green]Report saved to {output_path}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
