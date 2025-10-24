"""
Metadata Tagging Automation

Applies standardized metadata tags to Snowflake resources for
cost attribution, compliance, and governance.

Usage:
    python apply_tags.py [--resource-type warehouse] [--apply]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm

sys.path.append(str(Path(__file__).parent.parent.parent / 'cost-optimization'))
from snowflake_utils import (
    SnowflakeConnection,
    logger
)

console = Console()


# Standard tag schema
TAG_SCHEMA = {
    'cost_center': {
        'description': 'Cost center for chargeback',
        'allowed_values': ['engineering', 'analytics', 'data_science', 'ops', 'product']
    },
    'data_classification': {
        'description': 'Data sensitivity level',
        'allowed_values': ['public', 'internal', 'confidential', 'restricted']
    },
    'owner': {
        'description': 'Team or individual responsible',
        'allowed_values': None  # Free-form
    },
    'environment': {
        'description': 'Environment type',
        'allowed_values': ['prod', 'staging', 'dev', 'test']
    },
    'compliance': {
        'description': 'Compliance requirements',
        'allowed_values': ['hipaa', 'sox', 'gdpr', 'none']
    },
    'project': {
        'description': 'Project or initiative',
        'allowed_values': None  # Free-form
    }
}


def create_tag_schema(sf: SnowflakeConnection, database: str = 'GOVERNANCE') -> None:
    """Create tag definitions in Snowflake."""
    console.print(f"[cyan]Creating tag schema in {database} database...[/cyan]")

    statements = [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        f"CREATE SCHEMA IF NOT EXISTS {database}.TAGS",
        "USE SCHEMA {database}.TAGS"
    ]

    # Create each tag
    for tag_name, tag_info in TAG_SCHEMA.items():
        if tag_info['allowed_values']:
            allowed = "', '".join(tag_info['allowed_values'])
            stmt = f"""
            CREATE TAG IF NOT EXISTS {tag_name}
            ALLOWED_VALUES '{allowed}'
            COMMENT = '{tag_info['description']}'
            """
        else:
            stmt = f"""
            CREATE TAG IF NOT EXISTS {tag_name}
            COMMENT = '{tag_info['description']}'
            """
        statements.append(stmt)

    try:
        sf.execute_script(statements)
        console.print("[green]Tag schema created successfully[/green]")
    except Exception as e:
        console.print(f"[red]Failed to create tag schema: {e}[/red]")
        raise


def get_untagged_resources(sf: SnowflakeConnection, resource_type: str = 'warehouse') -> pd.DataFrame:
    """Find resources missing required tags."""

    if resource_type == 'warehouse':
        query = """
        SELECT
            'WAREHOUSE' as resource_type,
            qh.warehouse_name as resource_name,
            MAX(qh.warehouse_size) as warehouse_size,
            NULL as owner,
            MIN(qh.start_time) as created_on
        FROM snowflake.account_usage.query_history qh
        WHERE qh.warehouse_name IS NOT NULL
            AND qh.start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
        GROUP BY qh.warehouse_name
        """
    elif resource_type == 'database':
        query = """
        SELECT
            'DATABASE' as resource_type,
            database_name as resource_name,
            database_owner as owner,
            created as created_on
        FROM snowflake.account_usage.databases
        WHERE deleted IS NULL
        """
    elif resource_type == 'table':
        query = """
        SELECT
            'TABLE' as resource_type,
            table_catalog || '.' || table_schema || '.' || table_name as resource_name,
            table_owner as owner,
            created as created_on
        FROM snowflake.account_usage.tables
        WHERE deleted IS NULL
            AND table_type = 'BASE TABLE'
        LIMIT 1000
        """
    else:
        raise ValueError(f"Unsupported resource type: {resource_type}")

    df = sf.execute_query(query)

    # Check for existing tags (simplified - would need tag reference query)
    # For demo purposes, mark all as untagged
    df['missing_tags'] = 'cost_center, owner, environment'
    df['tag_compliance'] = 'NON_COMPLIANT'

    return df


def generate_tagging_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """Generate tag recommendations based on resource patterns."""
    recommendations = []

    for _, row in df.iterrows():
        resource_name = row['RESOURCE_NAME']
        resource_type = row['RESOURCE_TYPE']

        # Infer tags from naming conventions
        name_lower = resource_name.lower()

        # Infer environment
        if any(env in name_lower for env in ['prod', 'prd', 'production']):
            env = 'prod'
        elif 'staging' in name_lower or 'stg' in name_lower:
            env = 'staging'
        elif 'dev' in name_lower or 'development' in name_lower:
            env = 'dev'
        else:
            env = 'UNKNOWN'

        # Infer cost center from name
        if 'analytics' in name_lower or 'reporting' in name_lower:
            cost_center = 'analytics'
        elif 'ml' in name_lower or 'ds' in name_lower:
            cost_center = 'data_science'
        elif 'eng' in name_lower or 'etl' in name_lower:
            cost_center = 'engineering'
        else:
            cost_center = 'UNKNOWN'

        # Data classification (conservative default)
        data_class = 'internal'

        recommendations.append({
            'resource_type': resource_type,
            'resource_name': resource_name,
            'current_owner': row['OWNER'],
            'recommended_environment': env,
            'recommended_cost_center': cost_center,
            'recommended_data_classification': data_class,
            'confidence': 'HIGH' if env != 'UNKNOWN' and cost_center != 'UNKNOWN' else 'LOW',
            'action_required': 'REVIEW' if env == 'UNKNOWN' else 'APPROVE'
        })

    return pd.DataFrame(recommendations)


def generate_tagging_ddl(recommendations_df: pd.DataFrame, database: str = 'GOVERNANCE') -> list:
    """Generate ALTER statements to apply tags."""
    statements = []

    for _, row in recommendations_df.iterrows():
        resource_type = row['resource_type']
        resource_name = row['resource_name']

        tags = []
        if row['recommended_environment'] != 'UNKNOWN':
            tags.append(f"{database}.TAGS.environment = '{row['recommended_environment']}'")
        if row['recommended_cost_center'] != 'UNKNOWN':
            tags.append(f"{database}.TAGS.cost_center = '{row['recommended_cost_center']}'")
        if row['current_owner']:
            tags.append(f"{database}.TAGS.owner = '{row['current_owner']}'")
        tags.append(f"{database}.TAGS.data_classification = '{row['recommended_data_classification']}'")

        if tags:
            tag_clause = ', '.join(tags)
            stmt = f"ALTER {resource_type} {resource_name} SET TAG {tag_clause};"
            statements.append(stmt)

    return statements


def display_tagging_report(recommendations_df: pd.DataFrame) -> None:
    """Display tagging recommendations."""
    console.print("\n[bold cyan]Resource Tagging Recommendations[/bold cyan]\n")

    console.print(f"[bold]Total Resources:[/bold] {len(recommendations_df)}")
    console.print(f"[bold]Requiring Review:[/bold] {len(recommendations_df[recommendations_df['action_required'] == 'REVIEW'])}\n")

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Resource", style="cyan", width=40)
    table.add_column("Type", width=12)
    table.add_column("Environment", width=12)
    table.add_column("Cost Center", width=15)
    table.add_column("Confidence", justify="center", width=10)

    for _, row in recommendations_df.head(20).iterrows():
        conf_color = 'green' if row['confidence'] == 'HIGH' else 'yellow'

        table.add_row(
            row['resource_name'][:38] + "..." if len(row['resource_name']) > 38 else row['resource_name'],
            row['resource_type'],
            row['recommended_environment'],
            row['recommended_cost_center'],
            f"[{conf_color}]{row['confidence']}[/{conf_color}]"
        )

    console.print(table)


def main():
    parser = argparse.ArgumentParser(description='Apply metadata tags to Snowflake resources')
    parser.add_argument('--resource-type', choices=['warehouse', 'database', 'table'],
                       default='warehouse', help='Type of resource to tag')
    parser.add_argument('--tag-database', default='GOVERNANCE', help='Database for tag definitions')
    parser.add_argument('--create-schema', action='store_true', help='Create tag schema')
    parser.add_argument('--apply', action='store_true', help='Apply tags')
    parser.add_argument('--output', type=str, help='Save DDL to file')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        # Create tag schema if requested
        if args.create_schema:
            create_tag_schema(sf, args.tag_database)

        # Find untagged resources
        console.print(f"[cyan]Analyzing {args.resource_type} resources...[/cyan]")
        untagged_df = get_untagged_resources(sf, args.resource_type)

        if untagged_df.empty:
            console.print("[green]All resources are properly tagged![/green]")
            return

        # Generate recommendations
        recommendations_df = generate_tagging_recommendations(untagged_df)

        # Display report
        display_tagging_report(recommendations_df)

        # Generate DDL
        ddl_statements = generate_tagging_ddl(recommendations_df, args.tag_database)

        if args.output:
            with open(args.output, 'w') as f:
                f.write('\n'.join(ddl_statements))
            console.print(f"\n[green]DDL saved to {args.output}[/green]")

        # Apply if requested
        if args.apply:
            if Confirm.ask(f"\nApply tags to {len(recommendations_df)} resources?"):
                sf.execute_script(ddl_statements)
                console.print("[green]Tags applied successfully[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Tagging failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
