variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-southeast-1"
}

variable "project" {
  description = "Short name used as a prefix for resource names."
  type        = string
  default     = "olist"
}

variable "budget_email" {
  description = "Email that receives cost alerts."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget; alerts fire at 50%, 80% and 100% of actual spend."
  type        = number
  default     = 5
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit for the Athena workgroup (bytes). Guards against runaway queries."
  type        = number
  default     = 1073741824 # 1 GiB
}
