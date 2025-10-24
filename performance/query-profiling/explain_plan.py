#!/usr/bin/env python3
"""
Query Plan Analyzer

Retrieves and analyzes the execution plan for a specific Snowflake query.
Provides insights into query performance, execution steps, and optimization opportunities.

Usage:
    python explain_plan.py --query-id <query_id>
    python explain_plan.py --query-id <query_id> --detailed

Example:
    python explain_plan.py --query-id 01bfeed2-0206-c019-000f-cdd30015203a
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import json

# Add parent directory to path to import snowflake_utils
sys.path.append(str(Path(__file__).parent.parent.parent / 'cost-optimization'))

from snowflake_utils import SnowflakeConnection
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from rich import box

console = Console()


def get_query_info(sf: SnowflakeConnection, query_id: str) -> Dict[str, Any]:
    """Get basic information about the query."""
    query = f"""
    SELECT
        query_id,
        query_text,
        query_type,
        user_name,
        role_name,
        warehouse_name,
        warehouse_size,
        database_name,
        schema_name,
        start_time,
        end_time,
        total_elapsed_time,
        execution_time,
        compilation_time,
        bytes_scanned,
        bytes_written,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        rows_produced,
        rows_inserted,
        rows_updated,
        rows_deleted,
        partitions_scanned,
        partitions_total,
        execution_status,
        error_code,
        error_message
    FROM snowflake.account_usage.query_history
    WHERE query_id = '{query_id}'
    """

    df = sf.execute_query(query)
    if df.empty:
        return None

    return df.iloc[0].to_dict()


def get_query_plan(sf: SnowflakeConnection, query_id: str) -> str:
    """Get the execution plan for the query."""
    query = f"SELECT system$explain_plan_json('{query_id}')"

    try:
        df = sf.execute_query(query)
        if df.empty or df.iloc[0, 0] is None:
            return None
        return df.iloc[0, 0]
    except Exception as e:
        console.print(f"[yellow]Warning: Could not retrieve plan JSON: {e}[/yellow]")
        # Try text format instead
        try:
            query = f"SELECT system$explain_plan('{query_id}')"
            df = sf.execute_query(query)
            if df.empty or df.iloc[0, 0] is None:
                return None
            return df.iloc[0, 0]
        except Exception as e2:
            console.print(f"[yellow]Warning: Could not retrieve plan text: {e2}[/yellow]")
            return None


def parse_plan_json(plan_json: str) -> Dict[str, Any]:
    """Parse the JSON execution plan."""
    try:
        return json.loads(plan_json)
    except:
        return None


def analyze_plan(plan_data: Any, detailed: bool = False) -> List[Dict[str, str]]:
    """Analyze the execution plan for performance insights."""
    insights = []

    if isinstance(plan_data, str):
        # Text format plan
        if 'TableScan' in plan_data:
            insights.append({
                'type': 'SCAN',
                'severity': 'INFO',
                'message': 'Query includes table scans'
            })
        if 'JOIN' in plan_data.upper():
            insights.append({
                'type': 'JOIN',
                'severity': 'INFO',
                'message': 'Query includes join operations'
            })
        if 'AGGREGATE' in plan_data.upper():
            insights.append({
                'type': 'AGGREGATE',
                'severity': 'INFO',
                'message': 'Query includes aggregation operations'
            })
        return insights

    # JSON format plan analysis
    if not isinstance(plan_data, dict):
        return insights

    # Analyze various aspects of the plan
    operations = plan_data.get('operations', [])

    for op in operations:
        op_name = op.get('operation', '')

        # Check for expensive operations
        if 'TableScan' in op_name:
            rows = op.get('output_rows', 0)
            if rows > 1000000:
                insights.append({
                    'type': 'LARGE_SCAN',
                    'severity': 'WARNING',
                    'message': f'Large table scan: {rows:,} rows'
                })

        if 'CartesianJoin' in op_name:
            insights.append({
                'type': 'CARTESIAN_JOIN',
                'severity': 'CRITICAL',
                'message': 'Cartesian join detected - may cause performance issues'
            })

        if 'Sort' in op_name:
            rows = op.get('output_rows', 0)
            if rows > 1000000:
                insights.append({
                    'type': 'LARGE_SORT',
                    'severity': 'WARNING',
                    'message': f'Large sort operation: {rows:,} rows'
                })

    return insights


def display_query_info(info: Dict[str, Any]):
    """Display query information in a formatted table."""
    table = Table(title="Query Information", box=box.ROUNDED)
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="white")

    # Helper to get value with both upper and lower case keys
    def get_val(key: str, default='N/A'):
        return str(info.get(key.upper(), info.get(key.lower(), default)))

    # Convert values and add rows
    table.add_row("Query ID", get_val('query_id'))
    table.add_row("Type", get_val('query_type'))
    table.add_row("Status", get_val('execution_status'))
    table.add_row("User", get_val('user_name'))
    table.add_row("Role", get_val('role_name'))
    table.add_row("Warehouse", f"{get_val('warehouse_name')} ({get_val('warehouse_size')})")
    table.add_row("Database", get_val('database_name'))
    table.add_row("Schema", get_val('schema_name'))

    # Helper to get numeric value with case insensitivity
    def get_num(key: str, default=0):
        val = info.get(key.upper(), info.get(key.lower(), default))
        return val if val is not None else default

    start_time = get_val('start_time', None)
    if start_time and start_time != 'N/A':
        table.add_row("Started", start_time)

    # Performance metrics
    total_time = get_num('total_elapsed_time', 0)
    if total_time:
        table.add_row("Total Time", f"{int(total_time):,} ms ({int(total_time)/1000:.2f}s)")

    exec_time = get_num('execution_time', 0)
    if exec_time:
        table.add_row("Execution Time", f"{int(exec_time):,} ms ({int(exec_time)/1000:.2f}s)")

    compile_time = get_num('compilation_time', 0)
    if compile_time:
        table.add_row("Compilation Time", f"{int(compile_time):,} ms ({int(compile_time)/1000:.2f}s)")

    # Data metrics
    bytes_scanned = get_num('bytes_scanned', 0)
    if bytes_scanned and bytes_scanned > 0:
        table.add_row("Bytes Scanned", f"{int(bytes_scanned):,} ({format_bytes(int(bytes_scanned))})")

    bytes_written = get_num('bytes_written', 0)
    if bytes_written and bytes_written > 0:
        table.add_row("Bytes Written", f"{int(bytes_written):,} ({format_bytes(int(bytes_written))})")

    rows_produced = get_num('rows_produced', 0)
    if rows_produced and rows_produced > 0:
        table.add_row("Rows Produced", f"{int(rows_produced):,}")

    # Spilling metrics
    local_spill = get_num('bytes_spilled_to_local_storage', 0)
    if local_spill and local_spill > 0:
        table.add_row("Local Spill", f"{int(local_spill):,} ({format_bytes(int(local_spill))})", style="yellow")

    remote_spill = get_num('bytes_spilled_to_remote_storage', 0)
    if remote_spill and remote_spill > 0:
        table.add_row("Remote Spill", f"{int(remote_spill):,} ({format_bytes(int(remote_spill))})", style="red")

    # Partition pruning
    partitions_scanned = get_num('partitions_scanned', None)
    partitions_total = get_num('partitions_total', None)
    if partitions_scanned is not None and partitions_total is not None and partitions_total > 0:
        pct = (int(partitions_scanned) / int(partitions_total)) * 100
        table.add_row("Partitions", f"{int(partitions_scanned):,} / {int(partitions_total):,} ({pct:.1f}%)")

    # Error info if present
    error_code = get_val('error_code', None)
    if error_code and error_code != 'N/A':
        table.add_row("Error Code", error_code, style="red")
        table.add_row("Error Message", get_val('error_message'), style="red")

    console.print(table)

    # Display query text
    query_text = get_val('query_text', '')
    if query_text and query_text != 'N/A':
        # Truncate if very long
        if len(query_text) > 1000:
            query_text = query_text[:1000] + "\n... (truncated)"

        console.print(Panel(query_text, title="Query Text", border_style="blue"))


def format_bytes(bytes_val: int) -> str:
    """Format bytes into human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def display_insights(insights: List[Dict[str, str]]):
    """Display performance insights."""
    if not insights:
        console.print("[green]No specific performance insights detected.[/green]")
        return

    table = Table(title="Performance Insights", box=box.ROUNDED)
    table.add_column("Type", style="cyan")
    table.add_column("Severity", style="yellow")
    table.add_column("Message", style="white")

    severity_colors = {
        'CRITICAL': 'red',
        'WARNING': 'yellow',
        'INFO': 'blue'
    }

    for insight in insights:
        severity = insight['severity']
        color = severity_colors.get(severity, 'white')
        table.add_row(
            insight['type'],
            f"[{color}]{severity}[/{color}]",
            insight['message']
        )

    console.print(table)


def display_plan_text(plan_text: str, detailed: bool = False):
    """Display the execution plan in text format."""
    if detailed:
        console.print(Panel(plan_text, title="Execution Plan", border_style="green"))
    else:
        # Show truncated version
        lines = plan_text.split('\n')
        if len(lines) > 30:
            truncated = '\n'.join(lines[:30]) + f"\n\n... ({len(lines) - 30} more lines, use --detailed to see full plan)"
            console.print(Panel(truncated, title="Execution Plan (Summary)", border_style="green"))
        else:
            console.print(Panel(plan_text, title="Execution Plan", border_style="green"))


def main():
    parser = argparse.ArgumentParser(description='Analyze Snowflake query execution plan')
    parser.add_argument('--query-id', required=True, help='Query ID to analyze')
    parser.add_argument('--detailed', action='store_true', help='Show detailed execution plan')

    args = parser.parse_args()

    console.print(f"[bold blue]Analyzing query: {args.query_id}[/bold blue]\n")

    # Connect to Snowflake
    sf = SnowflakeConnection()

    try:
        # Get query information
        console.print("[cyan]Retrieving query information...[/cyan]")
        query_info = get_query_info(sf, args.query_id)

        if not query_info:
            console.print(f"[red]Error: Query ID {args.query_id} not found.[/red]")
            console.print("[yellow]Note: Queries may take a few minutes to appear in query_history.[/yellow]")
            return 1

        # Display query information
        display_query_info(query_info)
        console.print()

        # Get execution plan
        console.print("[cyan]Retrieving execution plan...[/cyan]")
        plan_data = get_query_plan(sf, args.query_id)

        if not plan_data:
            console.print("[yellow]Warning: Could not retrieve execution plan for this query.[/yellow]")
            console.print("[yellow]This may happen if the query is too old or was not successfully executed.[/yellow]")
        else:
            # Try to parse as JSON
            plan_json = parse_plan_json(plan_data)

            # Analyze plan
            insights = analyze_plan(plan_json if plan_json else plan_data, args.detailed)

            if insights:
                console.print()
                display_insights(insights)

            # Display plan
            console.print()
            if isinstance(plan_data, str):
                display_plan_text(plan_data, args.detailed)
            else:
                console.print(Panel(
                    json.dumps(plan_data, indent=2)[:2000] + ("..." if len(json.dumps(plan_data)) > 2000 else ""),
                    title="Execution Plan (JSON)",
                    border_style="green"
                ))

        # Performance recommendations based on query info
        console.print()
        recommendations = []

        # Helper to get numeric value with case insensitivity
        def get_num(key: str, default=0):
            val = query_info.get(key.upper(), query_info.get(key.lower(), default))
            return val if val is not None else default

        # Check for spilling
        local_spill = get_num('bytes_spilled_to_local_storage', 0)
        remote_spill = get_num('bytes_spilled_to_remote_storage', 0)

        if remote_spill and remote_spill > 0:
            recommendations.append("Consider increasing warehouse size - remote spilling detected")
        elif local_spill and local_spill > 0:
            recommendations.append("Query spilled to local disk - may benefit from larger warehouse")

        # Check compilation time
        compile_time = get_num('compilation_time', 0)
        total_time = get_num('total_elapsed_time', 1)
        if compile_time and total_time and (int(compile_time) / int(total_time)) > 0.3:
            recommendations.append("High compilation time - consider caching query results or using query result cache")

        # Check partition pruning
        partitions_scanned = get_num('partitions_scanned', None)
        partitions_total = get_num('partitions_total', None)
        if partitions_scanned is not None and partitions_total is not None and int(partitions_total) > 0:
            pct = (int(partitions_scanned) / int(partitions_total)) * 100
            if pct > 50:
                recommendations.append(f"Scanning {pct:.1f}% of partitions - add filters to improve partition pruning")

        if recommendations:
            table = Table(title="Recommendations", box=box.ROUNDED)
            table.add_column("Recommendation", style="yellow")
            for rec in recommendations:
                table.add_row(rec)
            console.print(table)
        else:
            console.print("[green]Query appears to be well-optimized![/green]")

    except Exception as e:
        console.print(f"[red]Error analyzing query: {e}[/red]")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
