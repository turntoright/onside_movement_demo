# The $5 monthly cost budget, created before the demo stack (spec §8). Its own root and
# state, so `make destroy ENV=aws` never removes it with the stack.
provider "aws" {
  region              = var.region
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = {
      project = "onside-movement-demo"
      env     = "aws"
    }
  }
}

resource "aws_budgets_budget" "monthly" {
  name         = "onside-demo-monthly"
  budget_type  = "COST"
  limit_amount = "5"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.email]
  }
}

output "budget_name" {
  value = aws_budgets_budget.monthly.name
}
