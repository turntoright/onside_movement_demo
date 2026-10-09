variable "env" {
  description = "local (LocalStack) or aws"
  type        = string

  validation {
    condition     = contains(["local", "aws"], var.env)
    error_message = "env must be local or aws."
  }
}

variable "localstack_endpoint" {
  description = "LocalStack URL for env=local; empty means real AWS with the default credential chain."
  type        = string
  default     = ""

  # Guard: a local deploy can never fall through to real AWS, and an AWS deploy never
  # points at LocalStack.
  validation {
    condition     = (var.env == "local") == (var.localstack_endpoint != "")
    error_message = "env=local needs localstack_endpoint; env=aws must not set it."
  }
}

variable "aws_account_id" {
  description = "Account the provider may act on (env=aws only); the provider refuses any other account."
  type        = string
  default     = ""

  validation {
    condition     = var.env != "aws" || can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "env=aws requires a 12-digit aws_account_id."
  }
}

variable "region" {
  type    = string
  default = "ap-southeast-2"
}
