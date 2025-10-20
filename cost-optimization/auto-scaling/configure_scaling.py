"""
Auto-Scaling Configuration Tool

Configures intelligent auto-scaling policies for Snowflake warehouses
based on workload patterns and performance requirements.

Usage:
    python configure_scaling.py [--warehouse WAREHOUSE_NAME] [--apply]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
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
    logger
)

console = Console()


def analyze_scaling_requirements(sf: SnowflakeConnection, warehouse_name: str = None, days: int = 30) -> pd.DataFrame:
    """Analyze concurrency patterns to determine scaling requirements."""
    logger.info(f"Analyzing scaling requirements for the last {days} days...")

    warehouse_filter = f"AND warehouse_name = '{warehouse_name}'" if warehouse_name else ""

    query = f"""
    WITH hourly_concurrency AS (
        SELECT
            warehouse_name,
            DATE_TRUNC('hour', start_time) as hour,
            HOUR(start_time) as hour_of_day,
            DAYNAME(start_time) as day_of_week,
            MAX(avg_running) as peak_concurrent,
            AVG(avg_running) as avg_concurrent,
            SUM(avg_queued_load) as total_queued,
            SUM(avg_queued_provisioning) as provisioning_queued,
            COUNT(*) as intervals_with_activity
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        {warehouse_filter}
        GROUP BY warehouse_name, DATE_TRUNC('hour', start_time), HOUR(start_time), DAYNAME(start_time)
    ),
    warehouse_config AS (
        SELECT
            name as warehouse_name,
            size as warehouse_size,
            type as warehouse_type,
            min_cluster_count,
            max_cluster_count,
            scaling_policy,
            auto_suspend,
            auto_resume
        FROM snowflake.account_usage.warehouses
        WHERE deleted IS NULL
    )
    SELECT
        hc.warehouse_name,
        wc.warehouse_size,
        wc.warehouse_type,
        wc.min_cluster_count,
        wc.max_cluster_count,
        wc.scaling_policy,

        -- Overall concurrency stats
        MAX(hc.peak_concurrent) as absolute_peak_concurrent,
        AVG(hc.peak_concurrent) as avg_peak_concurrent,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY hc.peak_concurrent) as p95_concurrent,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY hc.peak_concurrent) as p99_concurrent,

        -- Queue stats
        SUM(CASE WHEN hc.total_queued > 0 THEN 1 ELSE 0 END) as hours_with_queuing,
        AVG(CASE WHEN hc.total_queued > 0 THEN hc.total_queued ELSE NULL END) as avg_queued_when_queuing,
        MAX(hc.total_queued) as max_queued,

        -- Time-based patterns
        AVG(CASE WHEN hc.hour_of_day BETWEEN 9 AND 17 THEN hc.peak_concurrent ELSE NULL END) as business_hours_peak,
        AVG(CASE WHEN hc.hour_of_day NOT BETWEEN 9 AND 17 THEN hc.peak_concurrent ELSE NULL END) as off_hours_peak,
        AVG(CASE WHEN hc.day_of_week IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN hc.peak_concurrent ELSE NULL END) as weekday_peak,
        AVG(CASE WHEN hc.day_of_week IN ('Sat', 'Sun') THEN hc.peak_concurrent ELSE NULL END) as weekend_peak,

        -- Activity patterns
        COUNT(DISTINCT hc.hour) as active_hours_total,
        SUM(hc.intervals_with_activity) as total_active_intervals

    FROM hourly_concurrency hc
    JOIN warehouse_config wc ON hc.warehouse_name = wc.warehouse_name
    GROUP BY hc.warehouse_name, wc.warehouse_size, wc.warehouse_type,
             wc.min_cluster_count, wc.max_cluster_count, wc.scaling_policy
    ORDER BY absolute_peak_concurrent DESC
    """

    return sf.execute_query(query)


def generate_scaling_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """Generate multi-cluster scaling recommendations."""
    recommendations = []

    for _, row in df.iterrows():
        warehouse = row['WAREHOUSE_NAME']
        warehouse_type = row['WAREHOUSE_TYPE']
        current_min = row['MIN_CLUSTER_COUNT'] or 1
        current_max = row['MAX_CLUSTER_COUNT'] or 1
        current_policy = row['SCALING_POLICY']

        peak_concurrent = row['ABSOLUTE_PEAK_CONCURRENT']
        p95_concurrent = row['P95_CONCURRENT']
        avg_peak = row['AVG_PEAK_CONCURRENT']

        hours_with_queuing = row['HOURS_WITH_QUEUING'] or 0
        active_hours = row['ACTIVE_HOURS_TOTAL']
        queue_percentage = (hours_with_queuing / active_hours * 100) if active_hours > 0 else 0

        # Determine if multi-cluster is needed
        # Rule of thumb: More than 8 concurrent queries per cluster suggests need for scaling
        queries_per_cluster_threshold = 8

        needs_multi_cluster = (
            peak_concurrent > queries_per_cluster_threshold or
            queue_percentage > 5 or
            row['MAX_QUEUED'] > 5
        )

        # Calculate recommended cluster counts
        if not needs_multi_cluster:
            recommended_min = 1
            recommended_max = 1
            recommended_policy = None
            rationale = "Low concurrency - single cluster sufficient"
            confidence = "HIGH"

        else:
            # Min clusters: based on typical load (P95)
            recommended_min = max(1, int(p95_concurrent / queries_per_cluster_threshold))

            # Max clusters: based on absolute peak with some headroom
            recommended_max = max(recommended_min, int(peak_concurrent / queries_per_cluster_threshold) + 1)

            # Cap max clusters at reasonable limit
            recommended_max = min(recommended_max, 10)

            # Determine scaling policy
            # Check workload variability
            business_hours_peak = row['BUSINESS_HOURS_PEAK'] or 0
            off_hours_peak = row['OFF_HOURS_PEAK'] or 0

            if business_hours_peak > off_hours_peak * 2:
                # Significant time-based variation - use Economy
                recommended_policy = "ECONOMY"
                rationale = "Time-based workload variation - Economy policy for cost efficiency"
            else:
                # Consistent high load - use Standard
                recommended_policy = "STANDARD"
                rationale = "Consistent high concurrency - Standard policy for performance"

            confidence = "HIGH" if queue_percentage > 10 else "MEDIUM"

        # Check for over-provisioning
        if current_max > 1 and not needs_multi_cluster:
            rationale = "Over-provisioned - reduce to single cluster"

        # Calculate cost impact
        # This is approximate - actual costs depend on scaling behavior
        if recommended_max > current_max:
            cost_impact = "INCREASE (but improves performance)"
        elif recommended_max < current_max:
            # Estimate savings from reducing max clusters
            estimated_savings = calculate_cost(100)  # Placeholder
            cost_impact = f"DECREASE - Est. {format_currency(estimated_savings)}/mo"
        else:
            cost_impact = "NEUTRAL"

        # Build considerations
        considerations = []

        if queue_percentage > 0:
            considerations.append(f"Queuing in {queue_percentage:.1f}% of active hours")

        if row['AVG_QUEUED_WHEN_QUEUING'] and row['AVG_QUEUED_WHEN_QUEUING'] > 2:
            considerations.append(f"Average {row['AVG_QUEUED_WHEN_QUEUING']:.1f} queries queued when queuing occurs")

        if business_hours_peak > off_hours_peak * 1.5:
            considerations.append("Significant business hours vs off-hours variation")

        weekday_peak = row['WEEKDAY_PEAK'] or 0
        weekend_peak = row['WEEKEND_PEAK'] or 0
        if weekday_peak > weekend_peak * 1.5:
            considerations.append("Higher weekday load - consider scheduled scaling")

        recommendations.append({
            'warehouse': warehouse,
            'warehouse_type': warehouse_type,
            'current_min_clusters': current_min,
            'current_max_clusters': current_max,
            'current_policy': current_policy or 'N/A',
            'recommended_min_clusters': recommended_min,
            'recommended_max_clusters': recommended_max,
            'recommended_policy': recommended_policy or 'N/A',
            'peak_concurrent_queries': peak_concurrent,
            'p95_concurrent': p95_concurrent,
            'hours_with_queuing': hours_with_queuing,
            'queue_percentage': queue_percentage,
            'needs_multi_cluster': needs_multi_cluster,
            'cost_impact': cost_impact,
            'rationale': rationale,
            'confidence': confidence,
            'considerations': ' | '.join(considerations) if considerations else 'None'
        })

    return pd.DataFrame(recommendations)


def display_scaling_recommendations(recommendations_df: pd.DataFrame) -> None:
    """Display scaling recommendations in a formatted table."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Multi-Cluster Auto-Scaling Recommendations[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Summary
    needs_changes = len(recommendations_df[
        (recommendations_df['current_min_clusters'] != recommendations_df['recommended_min_clusters']) |
        (recommendations_df['current_max_clusters'] != recommendations_df['recommended_max_clusters'])
    ])

    needs_multi_cluster = len(recommendations_df[recommendations_df['needs_multi_cluster'] == True])

    console.print(f"\n[bold]Warehouses Analyzed:[/bold] {len(recommendations_df)}")
    console.print(f"[bold]Warehouses Needing Multi-Cluster:[/bold] {needs_multi_cluster}")
    console.print(f"[bold]Warehouses Needing Configuration Change:[/bold] {needs_changes}\n")

    # Recommendations table
    table = Table(show_header=True, header_style="bold magenta", show_lines=True)
    table.add_column("Warehouse", style="cyan", width=20)
    table.add_column("Current\nClusters", justify="center", width=10)
    table.add_column("Recommended\nClusters", justify="center", width=12)
    table.add_column("Policy", justify="center", width=10)
    table.add_column("Peak\nConcurrent", justify="right", width=10)
    table.add_column("Queuing", justify="right", width=10)
    table.add_column("Rationale", width=40)

    for _, row in recommendations_df.iterrows():
        current = f"{row['current_min_clusters']}-{row['current_max_clusters']}"
        recommended = f"{row['recommended_min_clusters']}-{row['recommended_max_clusters']}"

        # Color code based on action needed
        if row['recommended_max_clusters'] > row['current_max_clusters']:
            rec_color = "red"  # Need to scale up
        elif row['recommended_max_clusters'] < row['current_max_clusters']:
            rec_color = "green"  # Can scale down
        else:
            rec_color = "white"  # No change

        queue_color = "red" if row['queue_percentage'] > 10 else "yellow" if row['queue_percentage'] > 5 else "green"

        table.add_row(
            row['warehouse'],
            current,
            f"[{rec_color}]{recommended}[/{rec_color}]",
            row['recommended_policy'],
            f"{row['peak_concurrent_queries']:.0f}",
            f"[{queue_color}]{row['queue_percentage']:.1f}%[/{queue_color}]",
            row['rationale'][:50] + "..." if len(row['rationale']) > 50 else row['rationale']
        )

    console.print(table)

    # Detailed considerations for top candidates
    console.print("\n[bold]Detailed Considerations:[/bold]")
    top_candidates = recommendations_df[recommendations_df['needs_multi_cluster'] == True].head(10)

    for _, row in top_candidates.iterrows():
        if row['considerations'] and row['considerations'] != 'None':
            console.print(f"\n[cyan]{row['warehouse']}:[/cyan]")
            console.print(f"  {row['considerations']}")

    console.print("\n")


def apply_scaling_configuration(sf: SnowflakeConnection, recommendations_df: pd.DataFrame,
                               warehouse_filter: str = None) -> None:
    """Apply scaling configuration changes."""
    changes = recommendations_df[
        (recommendations_df['current_min_clusters'] != recommendations_df['recommended_min_clusters']) |
        (recommendations_df['current_max_clusters'] != recommendations_df['recommended_max_clusters']) |
        ((recommendations_df['current_policy'] != recommendations_df['recommended_policy']) &
         (recommendations_df['recommended_policy'] != 'N/A'))
    ]

    if warehouse_filter:
        changes = changes[changes['warehouse'] == warehouse_filter]

    if changes.empty:
        console.print("[yellow]No changes to apply[/yellow]")
        return

    console.print(f"\n[bold]Will configure scaling for {len(changes)} warehouse(s):[/bold]")
    for _, row in changes.iterrows():
        current = f"{row['current_min_clusters']}-{row['current_max_clusters']}"
        recommended = f"{row['recommended_min_clusters']}-{row['recommended_max_clusters']}"
        console.print(f"  • {row['warehouse']}: clusters {current} → {recommended}, policy: {row['recommended_policy']}")

    if not Confirm.ask("\nProceed with configuration?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    statements = []
    for _, row in changes.iterrows():
        # First, ensure warehouse is multi-cluster if needed
        if row['recommended_max_clusters'] > 1 and row['warehouse_type'] == 'STANDARD':
            console.print(f"[yellow]Warning: {row['warehouse']} needs to be converted to MULTI-CLUSTER type[/yellow]")
            # Note: This may require recreating the warehouse

        stmt = f"""
        ALTER WAREHOUSE {row['warehouse']}
        SET MIN_CLUSTER_COUNT = {row['recommended_min_clusters']}
            MAX_CLUSTER_COUNT = {row['recommended_max_clusters']}
        """

        if row['recommended_policy'] != 'N/A':
            stmt += f"\n    SCALING_POLICY = '{row['recommended_policy']}'"

        statements.append(stmt)

    try:
        sf.execute_script(statements)
        console.print(f"[green]Successfully configured {len(statements)} warehouse(s)[/green]")
    except Exception as e:
        console.print(f"[red]Failed to apply changes: {e}[/red]")
        raise


def main():
    parser = argparse.ArgumentParser(description='Configure warehouse auto-scaling policies')
    parser.add_argument('--days', type=int, default=30, help='Number of days to analyze (default: 30)')
    parser.add_argument('--warehouse', type=str, help='Configure specific warehouse only')
    parser.add_argument('--apply', action='store_true', help='Apply scaling configuration')
    parser.add_argument('--output', type=str, help='Save recommendations to CSV file')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        console.print("[cyan]Analyzing concurrency patterns and scaling requirements...[/cyan]")

        # Analyze scaling requirements
        scaling_df = analyze_scaling_requirements(sf, args.warehouse, args.days)

        if scaling_df.empty:
            console.print("[yellow]No warehouse data found[/yellow]")
            return

        # Generate recommendations
        recommendations_df = generate_scaling_recommendations(scaling_df)

        # Display results
        display_scaling_recommendations(recommendations_df)

        # Save output if requested
        if args.output:
            recommendations_df.to_csv(args.output, index=False)
            console.print(f"[green]Recommendations saved to {args.output}[/green]")

        # Apply if requested
        if args.apply:
            apply_scaling_configuration(sf, recommendations_df, args.warehouse)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
