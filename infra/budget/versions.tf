terraform {
  required_version = ">= 1.9"

  # State: infra/budget/state/aws.tfstate, passed by `make budget` (-backend-config=path=...).
  # `make deploy ENV=aws` refuses until this state holds the budget (spec §8).
  backend "local" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}
