"""
Clustering Key Recommendation Tool

Analyzes table access patterns and recommends optimal clustering keys
to improve query performance and reduce scan costs.

Usage:
    python recommend_clustering_keys.py [--database DB] [--schema SCHEMA] [--table TABLE]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

sys.path.append(str(Path(__file__).parent.parent.parent / 'cost-optimization'))
from snowflake_utils import (
    SnowflakeConnection,
    format_currency,
    calculate_cost,
    logger
)

console = Console()


def analyze_table_access_patterns(sf: SnowflakeConnection, database: str = None,
                                  schema: str = None, table: str = None, days: int = 30) -> pd.DataFrame:
    """Analyze how tables are accessed to recommend clustering keys."""
    logger.info(f"Analyzing table access patterns for the last {days} days...")

    filters = []
    if database:
        filters.append(f"AND qh.database_name = '{database}'")
    if schema:
        filters.append(f"AND qh.schema_name = '{schema}'")
    if table:
        filters.append(f"AND t.table_name = '{table}'")

    filter_clause = " ".join(filters)

    query = f"""
    WITH query_filters AS (
        -- Extract WHERE clause predicates from queries
        SELECT
            database_name,
            schema_name,
            query_text,
            query_id,
            total_elapsed_time / 1000.0 as execution_seconds,
            partitions_scanned,
            partitions_total,
            bytes_scanned,
            rows_produced
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND execution_status = 'SUCCESS'
            AND query_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE')
            AND partitions_scanned IS NOT NULL
            {filter_clause}
    ),
    table_info AS (
        SELECT
            t.table_catalog as database_name,
            t.table_schema as schema_name,
            t.table_name,
            t.row_count,
            t.bytes as table_bytes,
            t.clustering_key,
            c.column_name,
            c.data_type,
            c.ordinal_position
        FROM snowflake.account_usage.tables t
        JOIN snowflake.information_schema.columns c
            ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.deleted IS NULL
            AND t.table_type = 'BASE TABLE'
            AND t.row_count > 100000  -- Only analyze larger tables
            {filter_clause}
    )
    SELECT
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.row_count,
        ti.table_bytes,
        ti.clustering_key as current_clustering_key,

        -- Query stats
        COUNT(DISTINCT qf.query_id) as query_count,
        AVG(qf.execution_seconds) as avg_execution_seconds,
        SUM(qf.execution_seconds) as total_execution_seconds,

        -- Partition efficiency
        AVG(CASE WHEN qf.partitions_total > 0
            THEN qf.partitions_scanned::FLOAT / qf.partitions_total
            ELSE 1 END) as avg_partition_scan_ratio,
        AVG(qf.bytes_scanned) as avg_bytes_scanned,

        -- Column usage (simplified - in practice would need query parsing)
        -- This is a placeholder for demonstration
        LISTAGG(DISTINCT ti.column_name, ', ') as table_columns

    FROM table_info ti
    LEFT JOIN query_filters qf
        ON ti.database_name = qf.database_name
        AND ti.schema_name = qf.schema_name
    WHERE ti.row_count > 0
    GROUP BY ti.database_name, ti.schema_name, ti.table_name,
             ti.row_count, ti.table_bytes, ti.clustering_key
    HAVING COUNT(DISTINCT qf.query_id) >= 5  -- At least 5 queries
    ORDER BY total_execution_seconds DESC NULLS LAST
    LIMIT 100
    """

    return sf.execute_query(query)


def get_table_clustering_info(sf: SnowflakeConnection, database: str, schema: str, table: str) -> dict:
    """Get detailed clustering information for a specific table."""
    query = f"""
    SELECT
        SYSTEM$CLUSTERING_INFORMATION('{table}') as clustering_info
    FROM {database}.{schema}.{table}
    LIMIT 1
    """

    try:
        result = sf.execute_query(query)
        if not result.empty:
            import json
            return json.loads(result['CLUSTERING_INFO'].iloc[0])
    except Exception as e:
        logger.warning(f"Could not get clustering info for {database}.{schema}.{table}: {e}")
        return {}


def recommend_clustering_keys(access_patterns_df: pd.DataFrame) -> pd.DataFrame:
    """Generate clustering key recommendations based on access patterns."""
    recommendations = []

    for _, row in access_patterns_df.iterrows():
        database = row['DATABASE_NAME']
        schema = row['SCHEMA_NAME']
        table = row['TABLE_NAME']
        current_key = row['CURRENT_CLUSTERING_KEY']

        partition_scan_ratio = row['AVG_PARTITION_SCAN_RATIO']
        query_count = row['QUERY_COUNT']
        avg_execution = row['AVG_EXECUTION_SECONDS']
        table_size_gb = row['TABLE_BYTES'] / (1024**3) if row['TABLE_BYTES'] else 0

        # Determine if clustering is beneficial
        needs_clustering = (
            partition_scan_ratio > 0.5 or  # Scanning more than 50% of partitions
            (not current_key and query_count > 20) or  # High query volume, no clustering
            (table_size_gb > 100)  # Large table
        )

        if not needs_clustering:
            continue

        # Generate recommendations
        # In a real implementation, this would analyze query predicates
        # For now, provide general guidance
        if not current_key:
            priority = "HIGH" if query_count > 50 else "MEDIUM"
            recommendation = "Add clustering key"

            # Common clustering key candidates
            suggested_keys = []
            if 'date' in str(row['TABLE_COLUMNS']).lower():
                suggested_keys.append("DATE/TIMESTAMP columns (for time-series data)")
            if 'id' in str(row['TABLE_COLUMNS']).lower():
                suggested_keys.append("High-cardinality ID columns")

            suggested_keys_text = " or ".join(suggested_keys) if suggested_keys else "Analyze query WHERE clauses for most filtered columns"

        else:
            if partition_scan_ratio > 0.7:
                priority = "HIGH"
                recommendation = "Re-evaluate clustering key"
                suggested_keys_text = "Current clustering not effective - analyze query patterns"
            else:
                priority = "LOW"
                recommendation = "Monitor clustering health"
                suggested_keys_text = "Current clustering appears adequate"

        # Calculate potential impact
        # Assume 30% performance improvement and cost reduction
        current_cost = calculate_cost(row['AVG_BYTES_SCANNED'] / (1024**3) * 0.01) * query_count  # Rough estimate
        potential_savings = current_cost * 0.3

        considerations = []
        if table_size_gb > 500:
            considerations.append("Large table - reclustering will be expensive")
        if query_count > 100:
            considerations.append("High query volume - significant performance impact")
        if partition_scan_ratio > 0.8:
            considerations.append("Very poor partition pruning - clustering highly beneficial")

        recommendations.append({
            'database': database,
            'schema': schema,
            'table': table,
            'table_size_gb': table_size_gb,
            'current_clustering_key': current_key or 'None',
            'query_count': query_count,
            'avg_execution_seconds': avg_execution,
            'partition_scan_ratio': partition_scan_ratio,
            'priority': priority,
            'recommendation': recommendation,
            'suggested_keys': suggested_keys_text,
            'potential_monthly_savings': potential_savings * 30,  # Monthly estimate
            'considerations': ' | '.join(considerations) if considerations else 'None'
        })

    return pd.DataFrame(recommendations)


def generate_clustering_ddl(recommendations_df: pd.DataFrame) -> list:
    """Generate ALTER TABLE statements for applying clustering keys."""
    ddl_statements = []

    for _, row in recommendations_df[recommendations_df['priority'].isin(['HIGH', 'MEDIUM'])].iterrows():
        # Note: This generates placeholder DDL - actual column names would come from query analysis
        comment = f"""
        -- Table: {row['database']}.{row['schema']}.{row['table']}
        -- Priority: {row['priority']}
        -- Recommendation: {row['recommendation']}
        -- Suggested keys: {row['suggested_keys']}
        -- Potential savings: {format_currency(row['potential_monthly_savings'])}/month

        -- IMPORTANT: Replace <column_name> with actual columns from query WHERE clauses
        -- Example clustering keys:
        --   For time-series: ORDER_DATE, CREATED_AT, etc.
        --   For ID-based: CUSTOMER_ID, PRODUCT_ID, etc.
        --   Multi-column: (DATE_COLUMN, ID_COLUMN)

        -- ALTER TABLE {row['database']}.{row['schema']}.{row['table']}
        --     CLUSTER BY (<column_name>);

        -- After adding clustering, monitor with:
        -- SELECT SYSTEM$CLUSTERING_INFORMATION('{row['table']}', '(<column_name>)');
        """
        ddl_statements.append(comment)

    return ddl_statements


def display_clustering_recommendations(recommendations_df: pd.DataFrame) -> None:
    """Display clustering recommendations report."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Table Clustering Recommendations[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    if recommendations_df.empty:
        console.print("\n[green]No clustering optimizations needed! Tables are well-optimized.[/green]\n")
        return

    # Summary
    total_savings = recommendations_df['potential_monthly_savings'].sum()
    high_priority = len(recommendations_df[recommendations_df['priority'] == 'HIGH'])

    console.print(f"\n[bold]Tables Analyzed:[/bold] {len(recommendations_df)}")
    console.print(f"[bold red]High Priority Optimizations:[/bold red] {high_priority}")
    console.print(f"[bold green]Potential Monthly Savings:[/bold green] {format_currency(total_savings)}\n")

    # Recommendations table
    table = Table(show_header=True, header_style="bold magenta", show_lines=True)
    table.add_column("Table", style="cyan", width=30)
    table.add_column("Size", justify="right", width=10)
    table.add_column("Queries", justify="right", width=10)
    table.add_column("Scan\nRatio", justify="right", width=10)
    table.add_column("Priority", justify="center", width=10)
    table.add_column("Recommendation", width=40)

    sorted_df = recommendations_df.sort_values(['priority', 'potential_monthly_savings'], ascending=[True, False])

    for _, row in sorted_df.head(20).iterrows():
        priority_color = {
            'HIGH': 'red',
            'MEDIUM': 'yellow',
            'LOW': 'green'
        }.get(row['priority'], 'white')

        scan_color = 'red' if row['partition_scan_ratio'] > 0.7 else 'yellow' if row['partition_scan_ratio'] > 0.5 else 'green'

        table_name = f"{row['schema']}.{row['table']}"

        table.add_row(
            table_name,
            f"{row['table_size_gb']:.1f}GB",
            str(int(row['query_count'])),
            f"[{scan_color}]{row['partition_scan_ratio']:.1%}[/{scan_color}]",
            f"[{priority_color}]{row['priority']}[/{priority_color}]",
            row['recommendation']
        )

    console.print(table)

    # Detailed recommendations
    console.print("\n[bold]Top Recommendations:[/bold]")
    for _, row in sorted_df[sorted_df['priority'] == 'HIGH'].head(5).iterrows():
        console.print(f"\n[cyan]{row['database']}.{row['schema']}.{row['table']}:[/cyan]")
        console.print(f"  Current: {row['current_clustering_key']}")
        console.print(f"  Suggested: {row['suggested_keys']}")
        console.print(f"  Savings: {format_currency(row['potential_monthly_savings'])}/month")
        if row['considerations'] != 'None':
            console.print(f"  Considerations: {row['considerations']}")

    console.print("\n")


def main():
    parser = argparse.ArgumentParser(description='Recommend table clustering keys')
    parser.add_argument('--days', type=int, default=30, help='Days of query history to analyze (default: 30)')
    parser.add_argument('--database', type=str, help='Analyze specific database only')
    parser.add_argument('--schema', type=str, help='Analyze specific schema only')
    parser.add_argument('--table', type=str, help='Analyze specific table only')
    parser.add_argument('--output', type=str, help='Save recommendations to file')
    parser.add_argument('--generate-ddl', action='store_true', help='Generate ALTER TABLE DDL statements')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        console.print("[cyan]Analyzing table access patterns and clustering opportunities...[/cyan]")

        # Analyze access patterns
        access_df = analyze_table_access_patterns(sf, args.database, args.schema, args.table, args.days)

        if access_df.empty:
            console.print("[yellow]No tables found matching criteria or insufficient query history[/yellow]")
            return

        # Generate recommendations
        recommendations_df = recommend_clustering_keys(access_df)

        # Display report
        display_clustering_recommendations(recommendations_df)

        # Generate DDL if requested
        if args.generate_ddl and not recommendations_df.empty:
            ddl_statements = generate_clustering_ddl(recommendations_df)
            ddl_file = 'clustering_ddl.sql'
            with open(ddl_file, 'w') as f:
                f.write('\n\n'.join(ddl_statements))
            console.print(f"[green]DDL statements saved to {ddl_file}[/green]")

        # Save output if requested
        if args.output:
            output_path = Path(args.output)
            recommendations_df.to_csv(output_path, index=False)
            console.print(f"[green]Recommendations saved to {output_path}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
