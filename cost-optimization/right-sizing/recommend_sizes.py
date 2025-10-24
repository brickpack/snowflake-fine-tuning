"""
Warehouse Right-Sizing Tool

Analyzes warehouse usage patterns and provides specific recommendations
for optimal warehouse sizing based on actual workload requirements.

Usage:
    python recommend_sizes.py [--days 30] [--apply] [--warehouse WAREHOUSE_NAME]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm

sys.path.append(str(Path(__file__).parent.parent))
from snowflake_utils import (
    SnowflakeConnection,
    calculate_cost,
    format_currency,
    format_percentage,
    parse_warehouse_size,
    recommend_warehouse_size,
    logger
)

console = Console()


def analyze_warehouse_patterns(sf: SnowflakeConnection, warehouse_name: str = None, days: int = 30) -> pd.DataFrame:
    """Analyze detailed usage patterns for warehouse sizing."""
    logger.info(f"Analyzing warehouse patterns for the last {days} days...")

    warehouse_filter = f"AND warehouse_name = '{warehouse_name}'" if warehouse_name else ""

    query = f"""
    WITH hourly_usage AS (
        SELECT
            wh.warehouse_name,
            DATE_TRUNC('hour', wh.start_time) as hour,
            SUM(wh.credits_used) as credits_used,
            SUM(wh.credits_used_compute) as compute_credits,
            SUM(wh.credits_used_cloud_services) as cloud_services_credits
        FROM snowflake.account_usage.warehouse_metering_history wh
        WHERE wh.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        {warehouse_filter}
        GROUP BY wh.warehouse_name, DATE_TRUNC('hour', wh.start_time)
    ),
    query_metrics AS (
        SELECT
            warehouse_name,
            DATE_TRUNC('hour', start_time) as hour,
            COUNT(*) as query_count,
            AVG(total_elapsed_time / 1000.0) as avg_query_time_seconds,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_elapsed_time / 1000.0) as p50_query_time,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_elapsed_time / 1000.0) as p95_query_time,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_elapsed_time / 1000.0) as p99_query_time,
            AVG(bytes_scanned) as avg_bytes_scanned,
            AVG(partitions_scanned) as avg_partitions_scanned
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            AND execution_status = 'SUCCESS'
            {warehouse_filter}
        GROUP BY warehouse_name, DATE_TRUNC('hour', start_time)
    )
    SELECT
        hu.warehouse_name,
        COALESCE(MAX(qh.warehouse_size), 'UNKNOWN') as warehouse_size,
        COUNT(DISTINCT hu.hour) as active_hours,
        AVG(hu.credits_used) as avg_credits_per_hour,
        STDDEV(hu.credits_used) as stddev_credits,
        MIN(hu.credits_used) as min_credits_per_hour,
        MAX(hu.credits_used) as max_credits_per_hour,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hu.credits_used) as p50_credits,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hu.credits_used) as p75_credits,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY hu.credits_used) as p95_credits,

        -- Credits breakdown
        AVG(hu.compute_credits) as avg_compute_credits,
        AVG(hu.cloud_services_credits) as avg_cloud_services_credits,

        -- Query performance metrics
        AVG(qm.query_count) as avg_queries_per_hour,
        AVG(qm.avg_query_time_seconds) as avg_query_time_seconds,
        AVG(qm.p95_query_time) as avg_p95_query_time,
        AVG(qm.avg_bytes_scanned) as avg_bytes_scanned,

        -- Concurrency from query_history
        MAX(qh_agg.max_concurrent) as max_concurrent_queries,
        AVG(qh_agg.avg_concurrent) as avg_concurrent_queries

    FROM hourly_usage hu
    LEFT JOIN query_metrics qm
        ON hu.warehouse_name = qm.warehouse_name AND hu.hour = qm.hour
    LEFT JOIN snowflake.account_usage.query_history qh
        ON hu.warehouse_name = qh.warehouse_name
        AND DATE_TRUNC('hour', qh.start_time) = hu.hour
        AND qh.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    LEFT JOIN (
        SELECT
            warehouse_name,
            DATE_TRUNC('hour', start_time) as hour,
            COUNT(*) as max_concurrent,
            COUNT(*) as avg_concurrent
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
        GROUP BY warehouse_name, DATE_TRUNC('hour', start_time)
    ) qh_agg ON hu.warehouse_name = qh_agg.warehouse_name AND hu.hour = qh_agg.hour
    GROUP BY hu.warehouse_name
    ORDER BY avg_credits_per_hour DESC
    """

    return sf.execute_query(query)


def generate_sizing_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """Generate detailed sizing recommendations with rationale."""
    recommendations = []

    for _, row in df.iterrows():
        warehouse = row['WAREHOUSE_NAME']
        current_size = row['WAREHOUSE_SIZE']
        current_credits = parse_warehouse_size(current_size)

        avg_credits = float(row['AVG_CREDITS_PER_HOUR'])
        p95_credits = float(row['P95_CREDITS'])
        max_credits = float(row['MAX_CREDITS_PER_HOUR'])

        # Determine recommendation based on multiple factors
        recommended_size = None
        rationale = []
        confidence = "HIGH"

        # Factor 1: Average usage
        avg_based_size = recommend_warehouse_size(avg_credits)
        avg_based_credits = parse_warehouse_size(avg_based_size)

        # Factor 2: P95 usage (handle spikes)
        p95_based_size = recommend_warehouse_size(p95_credits)
        p95_based_credits = parse_warehouse_size(p95_based_size)

        # Factor 3: Check concurrency (if available)
        max_concurrent = row.get('MAX_CONCURRENT_QUERIES', 0)
        high_concurrency = max_concurrent > current_credits * 8 if pd.notna(max_concurrent) else False

        # Decision logic
        if high_concurrency:
            # High concurrency - consider multi-cluster instead of sizing up
            recommended_size = p95_based_size
            rationale.append(f"High concurrency ({row['MAX_CONCURRENT_QUERIES']:.0f} queries)")
            rationale.append("Consider enabling multi-cluster auto-scaling")
            confidence = "MEDIUM"

        elif avg_credits < current_credits * 0.5:
            # Significantly oversized - recommend based on P95 to handle spikes
            recommended_size = p95_based_size
            rationale.append(f"Average usage ({avg_credits:.1f}) is {format_percentage(avg_credits/current_credits)} of current size")
            rationale.append(f"Recommended size handles P95 workload ({p95_credits:.1f} credits)")

        else:
            # Normal case - balance average and P95
            # Use P95 if it's significantly higher than average (spiky workload)
            if p95_credits > avg_credits * 1.5:
                recommended_size = p95_based_size
                rationale.append("Workload has significant spikes - sizing for P95")
            else:
                recommended_size = avg_based_size
                rationale.append("Consistent workload - sizing for average usage")

        # Calculate potential savings
        recommended_credits = parse_warehouse_size(recommended_size)
        credit_diff = current_credits - recommended_credits
        savings_pct = credit_diff / current_credits if current_credits > 0 else 0

        # Calculate dollar impact (approximate)
        total_credits_used = float(avg_credits) * float(row['ACTIVE_HOURS'])
        current_cost = calculate_cost(total_credits_used)

        # Avoid division by zero for unknown warehouse sizes
        if current_credits > 0:
            projected_cost = calculate_cost(total_credits_used * (recommended_credits / current_credits))
            savings = current_cost - projected_cost
        else:
            # For unknown sizes, estimate based on average usage
            projected_cost = calculate_cost(recommended_credits * float(row['ACTIVE_HOURS']))
            savings = current_cost - projected_cost

        # Additional considerations
        considerations = []

        variability = (float(row['STDDEV_CREDITS']) / avg_credits) if avg_credits > 0 and pd.notna(row['STDDEV_CREDITS']) else 0
        if variability > 0.5:
            considerations.append("⚠ High workload variability")
            considerations.append("→ Consider auto-scaling policies")

        avg_queries = row.get('AVG_QUERIES_PER_HOUR', 0)
        if avg_queries and avg_queries < 10:
            considerations.append("ℹ Low query volume")
            considerations.append("→ Consider consolidating with other warehouses")

        recommendations.append({
            'warehouse': warehouse,
            'current_size': current_size,
            'current_credits_per_hour': current_credits,
            'recommended_size': recommended_size,
            'recommended_credits_per_hour': recommended_credits,
            'avg_credits_used': avg_credits,
            'p95_credits_used': p95_credits,
            'max_credits_used': max_credits,
            'active_hours': float(row['ACTIVE_HOURS']),
            'savings_pct': savings_pct,
            'estimated_savings': savings,
            'confidence': confidence,
            'rationale': ' | '.join(rationale),
            'considerations': ' | '.join(considerations),
            'avg_concurrent': float(row['AVG_CONCURRENT_QUERIES']) if pd.notna(row.get('AVG_CONCURRENT_QUERIES')) else 0,
            'max_concurrent': float(row['MAX_CONCURRENT_QUERIES']) if pd.notna(row.get('MAX_CONCURRENT_QUERIES')) else 0
        })

    return pd.DataFrame(recommendations)


def display_recommendations(recommendations_df: pd.DataFrame) -> None:
    """Display recommendations in a formatted table."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Warehouse Right-Sizing Recommendations[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Summary
    total_savings = recommendations_df['estimated_savings'].sum()
    warehouses_to_change = len(recommendations_df[recommendations_df['current_size'] != recommendations_df['recommended_size']])

    console.print(f"\n[bold]Warehouses Analyzed:[/bold] {len(recommendations_df)}")
    console.print(f"[bold]Warehouses Needing Resize:[/bold] {warehouses_to_change}")
    console.print(f"[bold green]Estimated Total Savings:[/bold green] {format_currency(total_savings)}/month\n")

    # Recommendations table
    table = Table(show_header=True, header_style="bold magenta", show_lines=True)
    table.add_column("Warehouse", style="cyan", width=20)
    table.add_column("Current", justify="center", width=10)
    table.add_column("→ Recommended", justify="center", width=12)
    table.add_column("Avg Usage", justify="right", width=10)
    table.add_column("Savings", justify="right", width=10)
    table.add_column("Confidence", justify="center", width=10)
    table.add_column("Rationale", width=40)

    # Sort by savings descending
    sorted_df = recommendations_df.sort_values('estimated_savings', ascending=False)

    for _, row in sorted_df.iterrows():
        # Color code based on action
        if row['current_size'] == row['recommended_size']:
            action_color = "green"
            arrow = "="
        elif row['recommended_credits_per_hour'] < row['current_credits_per_hour']:
            action_color = "yellow"
            arrow = "↓"
        else:
            action_color = "red"
            arrow = "↑"

        confidence_color = {
            'HIGH': 'green',
            'MEDIUM': 'yellow',
            'LOW': 'red'
        }.get(row['confidence'], 'white')

        table.add_row(
            row['warehouse'],
            row['current_size'],
            f"[{action_color}]{arrow} {row['recommended_size']}[/{action_color}]",
            f"{row['avg_credits_used']:.1f} cr/hr",
            f"[green]{format_currency(row['estimated_savings'])}[/green]" if row['estimated_savings'] > 0 else "$0",
            f"[{confidence_color}]{row['confidence']}[/{confidence_color}]",
            row['rationale'][:60] + "..." if len(row['rationale']) > 60 else row['rationale']
        )

    console.print(table)

    # Detailed considerations
    console.print("\n[bold]Detailed Considerations:[/bold]")
    for _, row in sorted_df.head(10).iterrows():
        if row['considerations']:
            console.print(f"\n[cyan]{row['warehouse']}:[/cyan]")
            console.print(f"  {row['considerations']}")

    console.print("\n")


def apply_recommendations(sf: SnowflakeConnection, recommendations_df: pd.DataFrame,
                         warehouse_filter: str = None) -> None:
    """Apply sizing recommendations to warehouses."""
    changes = recommendations_df[recommendations_df['current_size'] != recommendations_df['recommended_size']]

    if warehouse_filter:
        changes = changes[changes['warehouse'] == warehouse_filter]

    if changes.empty:
        console.print("[yellow]No changes to apply[/yellow]")
        return

    console.print(f"\n[bold]Will resize {len(changes)} warehouse(s):[/bold]")
    for _, row in changes.iterrows():
        console.print(f"  • {row['warehouse']}: {row['current_size']} → {row['recommended_size']}")

    if not Confirm.ask("\nProceed with resizing?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    # Generate ALTER WAREHOUSE statements
    statements = []
    for _, row in changes.iterrows():
        stmt = f"ALTER WAREHOUSE {row['warehouse']} SET WAREHOUSE_SIZE = '{row['recommended_size']}'"
        statements.append(stmt)

    try:
        sf.execute_script(statements)
        console.print(f"[green]Successfully resized {len(statements)} warehouse(s)[/green]")
    except Exception as e:
        console.print(f"[red]Failed to apply changes: {e}[/red]")
        raise


def main():
    parser = argparse.ArgumentParser(description='Generate warehouse right-sizing recommendations')
    parser.add_argument('--days', type=int, default=30, help='Number of days to analyze (default: 30)')
    parser.add_argument('--warehouse', type=str, help='Analyze specific warehouse only')
    parser.add_argument('--apply', action='store_true', help='Apply recommendations (prompts for confirmation)')
    parser.add_argument('--output', type=str, help='Save recommendations to CSV file')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        console.print(f"[cyan]Analyzing warehouse usage patterns...[/cyan]")

        # Analyze patterns
        usage_df = analyze_warehouse_patterns(sf, args.warehouse, args.days)

        if usage_df.empty:
            console.print("[yellow]No usage data found[/yellow]")
            return

        # Generate recommendations
        recommendations_df = generate_sizing_recommendations(usage_df)

        # Display results
        display_recommendations(recommendations_df)

        # Save output if requested
        if args.output:
            recommendations_df.to_csv(args.output, index=False)
            console.print(f"[green]Recommendations saved to {args.output}[/green]")

        # Apply if requested
        if args.apply:
            apply_recommendations(sf, recommendations_df, args.warehouse)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
