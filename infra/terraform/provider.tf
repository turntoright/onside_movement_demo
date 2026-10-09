# One codebase for LocalStack and AWS: a non-empty localstack_endpoint switches every
# service endpoint, test credentials and path-style S3 on (Plan 2 spike provider block).
locals {
  localstack = var.localstack_endpoint != ""
}

provider "aws" {
  region                      = var.region
  access_key                  = local.localstack ? "test" : null
  secret_key                  = local.localstack ? "test" : null
  skip_credentials_validation = local.localstack
  skip_metadata_api_check     = local.localstack
  skip_requesting_account_id  = local.localstack
  s3_use_path_style           = local.localstack
  allowed_account_ids         = local.localstack ? null : [var.aws_account_id]

  dynamic "endpoints" {
    for_each = local.localstack ? [var.localstack_endpoint] : []
    content {
      dynamodb = endpoints.value
      events   = endpoints.value
      iam      = endpoints.value
      lambda   = endpoints.value
      logs     = endpoints.value
      s3       = endpoints.value
      sqs      = endpoints.value
      sts      = endpoints.value
    }
  }

  default_tags {
    tags = {
      project = "onside-movement-demo"
      env     = var.env
    }
  }
}
