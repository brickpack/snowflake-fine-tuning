/**
 * Snowflake Warehouse Management
 *
 * This module manages Snowflake warehouse resources with standardized
 * configurations for cost optimization and performance.
 */

terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
}

# Standard warehouse template with cost-optimized defaults
resource "snowflake_warehouse" "warehouse" {
  for_each = var.warehouses

  name    = each.key
  comment = each.value.comment

  # Sizing
  warehouse_size = each.value.size

  # Multi-cluster configuration
  min_cluster_count = try(each.value.min_cluster_count, 1)
  max_cluster_count = try(each.value.max_cluster_count, 1)
  scaling_policy    = try(each.value.scaling_policy, "STANDARD")

  # Auto-suspend and auto-resume for cost optimization
  auto_suspend  = try(each.value.auto_suspend, 300) # 5 minutes default
  auto_resume   = try(each.value.auto_resume, true)

  # Resource monitoring
  resource_monitor = try(each.value.resource_monitor, null)

  # Statement parameters for performance
  statement_timeout_in_seconds       = try(each.value.statement_timeout, 3600)
  statement_queued_timeout_in_seconds = try(each.value.queued_timeout, 0)

  # Tags for governance
  dynamic "tag" {
    for_each = try(each.value.tags, {})
    content {
      name  = tag.key
      value = tag.value
    }
  }
}

# Resource monitor for cost control
resource "snowflake_resource_monitor" "warehouse_monitor" {
  for_each = var.resource_monitors

  name            = each.key
  credit_quota    = each.value.credit_quota
  frequency       = try(each.value.frequency, "MONTHLY")
  start_timestamp = try(each.value.start_timestamp, null)
  end_timestamp   = try(each.value.end_timestamp, null)

  # Actions on threshold breach
  notify_triggers          = try(each.value.notify_triggers, [80, 100])
  suspend_triggers         = try(each.value.suspend_triggers, [100])
  suspend_immediate_triggers = try(each.value.suspend_immediate_triggers, [110])

  notify_users = try(each.value.notify_users, [])
}

# Outputs
output "warehouse_ids" {
  description = "Map of warehouse names to their IDs"
  value       = { for k, v in snowflake_warehouse.warehouse : k => v.id }
}

output "warehouse_arns" {
  description = "Map of warehouse names to their qualified names"
  value       = { for k, v in snowflake_warehouse.warehouse : k => v.fully_qualified_name }
}
