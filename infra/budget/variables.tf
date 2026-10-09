variable "aws_account_id" {
  description = "The only account the provider may act on."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be 12 digits."
  }
}

variable "email" {
  description = "Where the budget alerts go."
  type        = string

  validation {
    condition     = can(regex("^[^@ ]+@[^@ ]+$", var.email))
    error_message = "email must be an email address."
  }
}

variable "region" {
  type    = string
  default = "ap-southeast-2"
}
