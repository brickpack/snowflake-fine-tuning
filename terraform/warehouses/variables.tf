variable "warehouses" {
  description = "Map of warehouse configurations"
  type = map(object({
    comment           = string
    size              = string
    min_cluster_count = optional(number)
    max_cluster_count = optional(number)
    scaling_policy    = optional(string)
    auto_suspend      = optional(number)
    auto_resume       = optional(bool)
    resource_monitor  = optional(string)
    statement_timeout = optional(number)
    queued_timeout    = optional(number)
    tags              = optional(map(string))
  }))
  default = {}
}

variable "resource_monitors" {
  description = "Map of resource monitor configurations"
  type = map(object({
    credit_quota               = number
    frequency                  = optional(string)
    start_timestamp            = optional(string)
    end_timestamp              = optional(string)
    notify_triggers            = optional(list(number))
    suspend_triggers           = optional(list(number))
    suspend_immediate_triggers = optional(list(number))
    notify_users               = optional(list(string))
  }))
  default = {}
}
