terraform {
  # 1.9 or later: variable validation may reference other variables.
  required_version = ">= 1.9"

  # The state file per environment comes from `make deploy` (-backend-config=path=...).
  backend "local" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}
