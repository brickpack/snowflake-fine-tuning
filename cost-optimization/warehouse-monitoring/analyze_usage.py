"""
Warehouse Usage Analysis Tool

Analyzes warehouse usage patterns, identifies optimization opportunities,
and provides recommendations for cost reduction.

Usage:
    python analyze_usage.py [--days 30] [--output report.csv]
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from snowflake_utils import (
    SnowflakeConnection,
    calculate_cost,
    format_currency,
    format_percentage,
    get_warehouse_cost_summary,
    parse_warehouse_size,
    recommend_warehouse_size,
    logger
)

console = Console()


def analyze_warehouse_costs(sf: SnowflakeConnection, days: int = 30) -> pd.DataFrame:
    """Analyze warehouse costs and usage patterns."""
    logger.info(f"Analyzing warehouse costs for the last {days} days...")

    query = get_warehouse_cost_summary(days)
    df = sf.execute_query(query)

    if df.empty:
        logger.warning("No warehouse usage data found")
        return df

    # Calculate costs
    df['total_cost'] = df['TOTAL_CREDITS'].apply(calculate_cost)
    df['compute_cost'] = df['COMPUTE_CREDITS'].apply(calculate_cost)
    df['cloud_services_cost'] = df['CLOUD_SERVICES_CREDITS'].apply(calculate_cost)

    # Calculate daily averages
    df['avg_credits_per_day'] = df['TOTAL_CREDITS'] / df['ACTIVE_DAYS']
    df['avg_cost_per_day'] = df['avg_credits_per_day'].apply(calculate_cost)

    return df


def analyze_warehouse_utilization(sf: SnowflakeConnection, days: int = 30) -> pd.DataFrame:
    """Analyze warehouse utilization and identify underutilized resources."""
    logger.info("Analyzing warehouse utilization patterns...")

    # Get warehouse utilization metrics
    query = f"""
    SELECT
        wh.warehouse_name,
        COALESCE(MAX(qh.warehouse_size), 'UNKNOWN') as warehouse_size,
        COUNT(DISTINCT DATE_TRUNC('hour', wh.start_time)) as active_hours,
        SUM(wh.credits_used) as total_credits,
        SUM(wh.credits_used_compute) as compute_credits,
        SUM(wh.credits_used_cloud_services) as cloud_services_credits,
        AVG(wh.credits_used) as avg_credits_per_hour,

        -- Query metrics from query_history
        COUNT(DISTINCT qh.query_id) as total_queries,
        AVG(qh.total_elapsed_time / 1000.0) as avg_query_seconds,
        MAX(qh.total_elapsed_time / 1000.0) as max_query_seconds,
        SUM(qh.total_elapsed_time / 1000.0) as total_query_seconds

    FROM snowflake.account_usage.warehouse_metering_history wh
    LEFT JOIN snowflake.account_usage.query_history qh
        ON wh.warehouse_name = qh.warehouse_name
        AND DATE_TRUNC('hour', wh.start_time) = DATE_TRUNC('hour', qh.start_time)
        AND qh.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    WHERE wh.start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY wh.warehouse_name
    ORDER BY total_credits DESC
    """

    df = sf.execute_query(query)

    if df.empty:
        return df

    # Calculate utilization metrics
    df['cost'] = df['TOTAL_CREDITS'].apply(calculate_cost)
    df['potential_hours'] = days * 24  # Maximum possible hours
    df['utilization_rate'] = df['ACTIVE_HOURS'] / df['potential_hours']

    # Get current warehouse size in credits
    df['current_size_credits'] = df['WAREHOUSE_SIZE'].apply(parse_warehouse_size)

    # Recommend optimal size based on actual usage
    df['recommended_size'] = df['AVG_CREDITS_PER_HOUR'].apply(recommend_warehouse_size)
    df['recommended_size_credits'] = df['recommended_size'].apply(parse_warehouse_size)

    # Calculate potential savings
    df['potential_savings_pct'] = (
        (df['current_size_credits'] - df['recommended_size_credits']) /
        df['current_size_credits']
    ).clip(lower=0)

    df['potential_savings'] = df['cost'] * df['potential_savings_pct']

    return df


def identify_optimization_opportunities(utilization_df: pd.DataFrame) -> pd.DataFrame:
    """Identify specific optimization opportunities."""
    opportunities = []

    for _, row in utilization_df.iterrows():
        warehouse = row['WAREHOUSE_NAME']
        current_size = row['WAREHOUSE_SIZE']
        recommended_size = row['recommended_size']
        utilization = row['utilization_rate']
        potential_savings = row['potential_savings']

        # Low utilization
        if utilization < 0.2:
            opportunities.append({
                'warehouse': warehouse,
                'opportunity': 'Low Utilization',
                'current': f"{format_percentage(utilization)} utilized",
                'recommendation': 'Consider consolidating workloads or reducing size',
                'potential_savings': format_currency(potential_savings),
                'savings_value': potential_savings,  # Numeric value for sorting
                'priority': 'HIGH'
            })

        # Oversized warehouse
        if current_size != recommended_size and potential_savings > 100:
            opportunities.append({
                'warehouse': warehouse,
                'opportunity': 'Oversized Warehouse',
                'current': f"{current_size}",
                'recommendation': f"Resize to {recommended_size}",
                'potential_savings': format_currency(potential_savings),
                'savings_value': potential_savings,  # Numeric value for sorting
                'priority': 'HIGH' if potential_savings > 500 else 'MEDIUM'
            })

    return pd.DataFrame(opportunities)


def generate_cost_report(cost_df: pd.DataFrame, utilization_df: pd.DataFrame,
                        opportunities_df: pd.DataFrame) -> None:
    """Generate a comprehensive cost report."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Snowflake Warehouse Cost Analysis Report[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Overall summary
    total_cost = cost_df['total_cost'].sum()
    total_credits = cost_df['TOTAL_CREDITS'].sum()
    total_potential_savings = utilization_df['potential_savings'].sum()

    console.print(f"\n[bold]Total Spend:[/bold] {format_currency(total_cost)}")
    console.print(f"[bold]Total Credits:[/bold] {total_credits:,.2f}")
    console.print(f"[bold green]Potential Savings:[/bold green] {format_currency(total_potential_savings)}")
    console.print(f"[bold]Potential Reduction:[/bold] {format_percentage(total_potential_savings / total_cost)}\n")

    # Top cost warehouses
    console.print("[bold]Top 10 Warehouses by Cost:[/bold]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Warehouse", style="cyan")
    table.add_column("Size", justify="center")
    table.add_column("Credits", justify="right")
    table.add_column("Cost", justify="right")
    table.add_column("Avg/Day", justify="right")
    table.add_column("Active Days", justify="right")

    top_warehouses = cost_df.nlargest(10, 'total_cost')
    for _, row in top_warehouses.iterrows():
        table.add_row(
            row['WAREHOUSE_NAME'],
            "-",  # Size will be in utilization data
            f"{row['TOTAL_CREDITS']:,.2f}",
            format_currency(row['total_cost']),
            format_currency(row['avg_cost_per_day']),
            str(int(row['ACTIVE_DAYS']))
        )

    console.print(table)

    # Utilization summary
    console.print("\n[bold]Warehouse Utilization:[/bold]")
    util_table = Table(show_header=True, header_style="bold magenta")
    util_table.add_column("Warehouse", style="cyan")
    util_table.add_column("Size", justify="center")
    util_table.add_column("Utilization", justify="right")
    util_table.add_column("Recommended", justify="center")
    util_table.add_column("Potential Savings", justify="right")

    top_util = utilization_df.nlargest(10, 'potential_savings')
    for _, row in top_util.iterrows():
        util_color = "red" if row['utilization_rate'] < 0.3 else "yellow" if row['utilization_rate'] < 0.6 else "green"
        savings_color = "green" if row['potential_savings'] > 100 else "white"

        util_table.add_row(
            row['WAREHOUSE_NAME'],
            row['WAREHOUSE_SIZE'],
            f"[{util_color}]{format_percentage(row['utilization_rate'])}[/{util_color}]",
            row['recommended_size'],
            f"[{savings_color}]{format_currency(row['potential_savings'])}[/{savings_color}]"
        )

    console.print(util_table)

    # Optimization opportunities
    if not opportunities_df.empty:
        console.print("\n[bold]Top Optimization Opportunities:[/bold]")
        opp_table = Table(show_header=True, header_style="bold magenta")
        opp_table.add_column("Warehouse", style="cyan")
        opp_table.add_column("Opportunity", style="yellow")
        opp_table.add_column("Current", justify="right")
        opp_table.add_column("Recommendation")
        opp_table.add_column("Savings", justify="right")
        opp_table.add_column("Priority", justify="center")

        top_opps = opportunities_df.nlargest(10, 'savings_value', keep='first') if 'savings_value' in opportunities_df.columns else opportunities_df.head(10)
        for _, row in top_opps.iterrows():
            priority_color = "red" if row['priority'] == 'HIGH' else "yellow" if row['priority'] == 'MEDIUM' else "green"
            opp_table.add_row(
                row['warehouse'],
                row['opportunity'],
                row['current'],
                row['recommendation'],
                row['potential_savings'],
                f"[{priority_color}]{row['priority']}[/{priority_color}]"
            )

        console.print(opp_table)

    console.print("\n")


def main():
    parser = argparse.ArgumentParser(description='Analyze Snowflake warehouse usage and costs')
    parser.add_argument('--days', type=int, default=30, help='Number of days to analyze (default: 30)')
    parser.add_argument('--output', type=str, help='Output CSV file path')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    try:
        # Connect to Snowflake
        sf = SnowflakeConnection()

        # Run analyses
        console.print(f"[cyan]Analyzing warehouse usage for the last {args.days} days...[/cyan]")

        cost_df = analyze_warehouse_costs(sf, args.days)
        utilization_df = analyze_warehouse_utilization(sf, args.days)
        opportunities_df = identify_optimization_opportunities(utilization_df)

        # Generate report
        generate_cost_report(cost_df, utilization_df, opportunities_df)

        # Save output if requested
        if args.output:
            output_path = Path(args.output)
            if args.json:
                import json
                output_data = {
                    'cost_summary': cost_df.to_dict('records'),
                    'utilization': utilization_df.to_dict('records'),
                    'opportunities': opportunities_df.to_dict('records')
                }
                with open(output_path, 'w') as f:
                    json.dump(output_data, f, indent=2, default=str)
            else:
                # Create a summary report
                with pd.ExcelWriter(output_path.with_suffix('.xlsx')) as writer:
                    cost_df.to_excel(writer, sheet_name='Cost Summary', index=False)
                    utilization_df.to_excel(writer, sheet_name='Utilization', index=False)
                    opportunities_df.to_excel(writer, sheet_name='Opportunities', index=False)

            console.print(f"[green]Report saved to {output_path}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
