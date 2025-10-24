"""
RBAC Role and Privilege Audit Tool

Audits Snowflake roles, users, grants, and privilege assignments to identify
security risks, over-privileged accounts, and compliance issues.

Usage:
    python audit_roles.py [--role ROLE] [--user USER] [--output OUTPUT]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree

sys.path.append(str(Path(__file__).parent.parent.parent / 'cost-optimization'))
from snowflake_utils import (
    SnowflakeConnection,
    logger
)

console = Console()


def get_role_hierarchy(sf: SnowflakeConnection) -> pd.DataFrame:
    """Get role hierarchy and inheritance structure."""
    logger.info("Analyzing role hierarchy...")

    query = """
    SELECT
        grantee_name as child_role,
        name as parent_role,
        granted_on,
        granted_by,
        created_on
    FROM snowflake.account_usage.grants_to_roles
    WHERE granted_on = 'ROLE'
        AND deleted_on IS NULL
    ORDER BY child_role, parent_role
    """

    return sf.execute_query(query)


def get_user_role_assignments(sf: SnowflakeConnection, username: str = None) -> pd.DataFrame:
    """Get user to role assignments."""
    logger.info("Analyzing user role assignments...")

    user_filter = f"AND grantee_name = '{username}'" if username else ""

    query = f"""
    SELECT
        grantee_name as user_name,
        role as role_name,
        granted_by,
        created_on,
        deleted_on
    FROM snowflake.account_usage.grants_to_users
    WHERE deleted_on IS NULL
        {user_filter}
    ORDER BY grantee_name, role
    """

    return sf.execute_query(query)


def get_role_privileges(sf: SnowflakeConnection, role_name: str = None) -> pd.DataFrame:
    """Get privileges granted to roles."""
    logger.info("Analyzing role privileges...")

    role_filter = f"AND grantee_name = '{role_name}'" if role_name else ""

    query = f"""
    SELECT
        grantee_name as role_name,
        privilege,
        granted_on,
        name as object_name,
        table_catalog as database_name,
        table_schema as schema_name,
        granted_by,
        created_on
    FROM snowflake.account_usage.grants_to_roles
    WHERE deleted_on IS NULL
        AND granted_on != 'ROLE'  -- Exclude role grants (handled separately)
        {role_filter}
    ORDER BY grantee_name, privilege, granted_on
    """

    return sf.execute_query(query)


def get_privileged_roles(sf: SnowflakeConnection) -> pd.DataFrame:
    """Identify roles with high-privilege grants."""
    logger.info("Identifying privileged roles...")

    query = """
    WITH dangerous_privileges AS (
        SELECT
            grantee_name as role_name,
            privilege,
            granted_on,
            name as object_name,
            CASE
                WHEN privilege IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'SYSADMIN') THEN 'CRITICAL'
                WHEN privilege IN ('CREATE USER', 'CREATE ROLE', 'MANAGE GRANTS') THEN 'CRITICAL'
                WHEN privilege = 'OWNERSHIP' AND granted_on = 'DATABASE' THEN 'HIGH'
                WHEN privilege IN ('CREATE DATABASE', 'CREATE WAREHOUSE') THEN 'HIGH'
                WHEN privilege = 'USAGE' AND granted_on = 'WAREHOUSE' THEN 'MEDIUM'
                ELSE 'LOW'
            END as risk_level
        FROM snowflake.account_usage.grants_to_roles
        WHERE deleted_on IS NULL
    )
    SELECT
        role_name,
        COUNT(*) as total_privileges,
        SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) as critical_privileges,
        SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_privileges,
        SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) as medium_privileges,
        LISTAGG(DISTINCT CASE WHEN risk_level = 'CRITICAL' THEN privilege END, ', ') as critical_privs
    FROM dangerous_privileges
    GROUP BY role_name
    HAVING critical_privileges > 0 OR high_privileges > 5
    ORDER BY critical_privileges DESC, high_privileges DESC
    """

    return sf.execute_query(query)


def get_inactive_users(sf: SnowflakeConnection, days: int = 90) -> pd.DataFrame:
    """Find users who haven't logged in recently."""
    logger.info(f"Finding users inactive for {days} days...")

    query = f"""
    WITH user_logins AS (
        SELECT
            user_name,
            MAX(event_timestamp) as last_login
        FROM snowflake.account_usage.login_history
        WHERE is_success = 'YES'
        GROUP BY user_name
    ),
    all_users AS (
        SELECT DISTINCT
            grantee_name as user_name
        FROM snowflake.account_usage.grants_to_users
        WHERE deleted_on IS NULL
    )
    SELECT
        au.user_name,
        ul.last_login,
        DATEDIFF(day, ul.last_login, CURRENT_TIMESTAMP()) as days_since_login,
        CASE
            WHEN ul.last_login IS NULL THEN 'NEVER_LOGGED_IN'
            WHEN DATEDIFF(day, ul.last_login, CURRENT_TIMESTAMP()) > 180 THEN 'HIGHLY_INACTIVE'
            WHEN DATEDIFF(day, ul.last_login, CURRENT_TIMESTAMP()) > 90 THEN 'INACTIVE'
            ELSE 'RECENTLY_ACTIVE'
        END as status
    FROM all_users au
    LEFT JOIN user_logins ul ON au.user_name = ul.user_name
    WHERE ul.last_login IS NULL
        OR DATEDIFF(day, ul.last_login, CURRENT_TIMESTAMP()) > {days}
    ORDER BY days_since_login DESC NULLS FIRST
    """

    return sf.execute_query(query)


def audit_role_usage(sf: SnowflakeConnection, days: int = 30) -> pd.DataFrame:
    """Audit which roles are actually being used."""
    logger.info(f"Auditing role usage over the last {days} days...")

    query = f"""
    WITH role_usage AS (
        SELECT
            role_name,
            COUNT(DISTINCT user_name) as unique_users,
            COUNT(*) as total_queries,
            MAX(start_time) as last_used
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND role_name IS NOT NULL
        GROUP BY role_name
    ),
    all_roles AS (
        SELECT DISTINCT name as role_name
        FROM snowflake.account_usage.roles
        WHERE deleted_on IS NULL
    )
    SELECT
        ar.role_name,
        COALESCE(ru.unique_users, 0) as unique_users,
        COALESCE(ru.total_queries, 0) as total_queries,
        ru.last_used,
        DATEDIFF(day, ru.last_used, CURRENT_TIMESTAMP()) as days_since_used,
        CASE
            WHEN ru.last_used IS NULL THEN 'NEVER_USED'
            WHEN DATEDIFF(day, ru.last_used, CURRENT_TIMESTAMP()) > 90 THEN 'UNUSED'
            WHEN ru.total_queries < 10 THEN 'RARELY_USED'
            ELSE 'ACTIVE'
        END as usage_status
    FROM all_roles ar
    LEFT JOIN role_usage ru ON ar.role_name = ru.role_name
    ORDER BY total_queries DESC NULLS LAST
    """

    return sf.execute_query(query)


def identify_security_issues(privileged_df: pd.DataFrame, inactive_df: pd.DataFrame,
                             role_usage_df: pd.DataFrame) -> list:
    """Identify security issues and compliance violations."""
    issues = []

    # Check for highly privileged roles
    if not privileged_df.empty:
        critical_roles = privileged_df[privileged_df['CRITICAL_PRIVILEGES'] > 0]
        for _, row in critical_roles.iterrows():
            issues.append({
                'severity': 'CRITICAL',
                'category': 'Privileged Role',
                'issue': f"Role '{row['ROLE_NAME']}' has {int(row['CRITICAL_PRIVILEGES'])} critical privileges",
                'detail': row['CRITICAL_PRIVS'],
                'recommendation': "Review and restrict critical privileges. Consider role separation."
            })

    # Check for inactive users with access
    if not inactive_df.empty:
        highly_inactive = inactive_df[inactive_df['STATUS'] == 'HIGHLY_INACTIVE']
        for _, row in highly_inactive.iterrows():
            issues.append({
                'severity': 'HIGH',
                'category': 'Inactive User',
                'issue': f"User '{row['USER_NAME']}' inactive for {int(row['DAYS_SINCE_LOGIN'])} days",
                'detail': f"Last login: {row['LAST_LOGIN'] or 'Never'}",
                'recommendation': "Disable or remove this user account."
            })

    # Check for unused roles with privileges
    if not role_usage_df.empty:
        unused_roles = role_usage_df[role_usage_df['USAGE_STATUS'] == 'NEVER_USED']
        if len(unused_roles) > 0:
            issues.append({
                'severity': 'MEDIUM',
                'category': 'Unused Roles',
                'issue': f"{len(unused_roles)} roles have never been used",
                'detail': ', '.join(unused_roles['ROLE_NAME'].head(5).tolist()),
                'recommendation': "Review and consider removing unused roles."
            })

    return issues


def display_audit_report(hierarchy_df: pd.DataFrame, user_roles_df: pd.DataFrame,
                        privileged_df: pd.DataFrame, inactive_df: pd.DataFrame,
                        role_usage_df: pd.DataFrame, issues: list):
    """Display comprehensive RBAC audit report."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Snowflake RBAC Security Audit Report[/bold cyan]",
        subtitle=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))

    # Summary statistics
    console.print("\n[bold]Audit Summary:[/bold]")
    console.print(f"Total Roles: {len(role_usage_df)}")
    console.print(f"Total Users: {len(user_roles_df['USER_NAME'].unique()) if not user_roles_df.empty else 0}")
    console.print(f"Privileged Roles: {len(privileged_df)}")
    console.print(f"Inactive Users: {len(inactive_df)}")
    console.print(f"Security Issues Found: {len(issues)}")

    # Security Issues
    if issues:
        console.print("\n[bold red]Security Issues:[/bold red]")
        issues_table = Table(show_header=True, header_style="bold red")
        issues_table.add_column("Severity", style="red")
        issues_table.add_column("Category", style="yellow")
        issues_table.add_column("Issue")
        issues_table.add_column("Recommendation")

        for issue in sorted(issues, key=lambda x: {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}[x['severity']]):
            severity_color = {
                'CRITICAL': 'red bold',
                'HIGH': 'red',
                'MEDIUM': 'yellow',
                'LOW': 'white'
            }[issue['severity']]

            issues_table.add_row(
                f"[{severity_color}]{issue['severity']}[/{severity_color}]",
                issue['category'],
                issue['issue'],
                issue['recommendation']
            )

        console.print(issues_table)

    # Privileged Roles
    if not privileged_df.empty:
        console.print("\n[bold]Highly Privileged Roles:[/bold]")
        priv_table = Table(show_header=True, header_style="bold magenta")
        priv_table.add_column("Role", style="cyan")
        priv_table.add_column("Critical", justify="right", style="red")
        priv_table.add_column("High", justify="right", style="yellow")
        priv_table.add_column("Total", justify="right")
        priv_table.add_column("Critical Privileges")

        for _, row in privileged_df.head(10).iterrows():
            priv_table.add_row(
                row['ROLE_NAME'],
                str(int(row['CRITICAL_PRIVILEGES'])),
                str(int(row['HIGH_PRIVILEGES'])),
                str(int(row['TOTAL_PRIVILEGES'])),
                str(row['CRITICAL_PRIVS'])[:50] + "..." if pd.notna(row['CRITICAL_PRIVS']) and len(str(row['CRITICAL_PRIVS'])) > 50 else str(row['CRITICAL_PRIVS'] or '')
            )

        console.print(priv_table)

    # Inactive Users
    if not inactive_df.empty:
        console.print("\n[bold]Inactive Users (Top 10):[/bold]")
        inactive_table = Table(show_header=True, header_style="bold yellow")
        inactive_table.add_column("User", style="cyan")
        inactive_table.add_column("Last Login")
        inactive_table.add_column("Days Inactive", justify="right")
        inactive_table.add_column("Status", style="red")

        for _, row in inactive_df.head(10).iterrows():
            inactive_table.add_row(
                row['USER_NAME'],
                str(row['LAST_LOGIN']) if pd.notna(row['LAST_LOGIN']) else 'Never',
                str(int(row['DAYS_SINCE_LOGIN'])) if pd.notna(row['DAYS_SINCE_LOGIN']) else 'N/A',
                row['STATUS']
            )

        console.print(inactive_table)

    # Role Usage
    if not role_usage_df.empty:
        console.print("\n[bold]Role Usage Summary (Top 10 Active):[/bold]")
        usage_table = Table(show_header=True, header_style="bold green")
        usage_table.add_column("Role", style="cyan")
        usage_table.add_column("Users", justify="right")
        usage_table.add_column("Queries", justify="right")
        usage_table.add_column("Last Used")
        usage_table.add_column("Status")

        active_roles = role_usage_df[role_usage_df['USAGE_STATUS'] == 'ACTIVE'].head(10)
        for _, row in active_roles.iterrows():
            usage_table.add_row(
                row['ROLE_NAME'],
                str(int(row['UNIQUE_USERS'])),
                str(int(row['TOTAL_QUERIES'])),
                str(row['LAST_USED']) if pd.notna(row['LAST_USED']) else 'Never',
                row['USAGE_STATUS']
            )

        console.print(usage_table)


def main():
    parser = argparse.ArgumentParser(description='Audit Snowflake RBAC configuration')
    parser.add_argument('--role', help='Audit specific role')
    parser.add_argument('--user', help='Audit specific user')
    parser.add_argument('--inactive-days', type=int, default=90,
                       help='Days to consider user inactive (default: 90)')
    parser.add_argument('--output', help='Save report to CSV file')

    args = parser.parse_args()

    try:
        sf = SnowflakeConnection()

        # Gather audit data
        hierarchy_df = get_role_hierarchy(sf)
        user_roles_df = get_user_role_assignments(sf, args.user)
        role_privileges_df = get_role_privileges(sf, args.role)
        privileged_df = get_privileged_roles(sf)
        inactive_df = get_inactive_users(sf, args.inactive_days)
        role_usage_df = audit_role_usage(sf)

        # Identify security issues
        issues = identify_security_issues(privileged_df, inactive_df, role_usage_df)

        # Display report
        display_audit_report(hierarchy_df, user_roles_df, privileged_df,
                           inactive_df, role_usage_df, issues)

        # Save to CSV if requested
        if args.output:
            with pd.ExcelWriter(args.output) as writer:
                hierarchy_df.to_excel(writer, sheet_name='Role Hierarchy', index=False)
                user_roles_df.to_excel(writer, sheet_name='User Roles', index=False)
                role_privileges_df.to_excel(writer, sheet_name='Role Privileges', index=False)
                privileged_df.to_excel(writer, sheet_name='Privileged Roles', index=False)
                inactive_df.to_excel(writer, sheet_name='Inactive Users', index=False)
                role_usage_df.to_excel(writer, sheet_name='Role Usage', index=False)
                pd.DataFrame(issues).to_excel(writer, sheet_name='Security Issues', index=False)

            console.print(f"\n[green]Report saved to {args.output}[/green]")

    except Exception as e:
        logger.error(f"RBAC audit failed: {e}")
        console.print(f"\n[red]Error: {e}[/red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
