terraform {
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 6.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

variable "localstack" {
  type    = bool
  default = true
}

provider "aws" {
  region                      = "ap-southeast-2"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3       = "http://localhost:4566"
    sqs      = "http://localhost:4566"
    events   = "http://localhost:4566"
    firehose = "http://localhost:4566"
    dynamodb = "http://localhost:4566"
    lambda   = "http://localhost:4566"
    iam      = "http://localhost:4566"
    sts      = "http://localhost:4566"
    logs     = "http://localhost:4566"
  }
}

resource "aws_s3_bucket" "raw" {
  bucket        = "tf-spike-raw"
  force_destroy = true
}

resource "aws_dynamodb_table" "items" {
  name         = "tf-spike-items"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"
  attribute {
    name = "event_id"
    type = "S"
  }
}

resource "aws_sqs_queue" "dlq" {
  name                      = "tf-spike-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "visit" {
  name                       = "tf-spike-visit"
  visibility_timeout_seconds = 30
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_event_bus" "bus" {
  name = "tf-spike-bus"
}

resource "aws_cloudwatch_event_rule" "a" {
  name           = "rule-a"
  event_bus_name = aws_cloudwatch_event_bus.bus.name
  event_pattern = jsonencode({
    source        = ["onside.app"]
    "detail-type" = ["CheckIn", "CheckOut"]
  })
}

resource "aws_cloudwatch_event_target" "a_sqs" {
  rule           = aws_cloudwatch_event_rule.a.name
  event_bus_name = aws_cloudwatch_event_bus.bus.name
  target_id      = "visit-queue"
  arn            = aws_sqs_queue.visit.arn
}

resource "aws_sqs_queue_policy" "visit" {
  queue_url = aws_sqs_queue.visit.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.visit.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.a.arn } }
    }]
  })
}

# Lambda
data "archive_file" "fn" {
  type        = "zip"
  source_file = "${path.module}/handler.py"
  output_path = "${path.module}/fn.zip"
}

resource "aws_iam_role" "lambda" {
  name = "tf-spike-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "perm"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], Resource = aws_sqs_queue.visit.arn },
      { Effect = "Allow", Action = ["dynamodb:PutItem"], Resource = aws_dynamodb_table.items.arn },
      { Effect = "Allow", Action = ["events:PutEvents"], Resource = aws_cloudwatch_event_bus.bus.arn },
      { Effect = "Allow", Action = ["logs:*"], Resource = "*" },
    ]
  })
}

resource "aws_lambda_function" "fn" {
  function_name    = "tf-spike-fn"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.fn.output_path
  source_code_hash = data.archive_file.fn.output_base64sha256
  timeout          = 25
  environment {
    variables = {
      TABLE = aws_dynamodb_table.items.name
      BUS   = aws_cloudwatch_event_bus.bus.name
    }
  }
}

resource "aws_lambda_event_source_mapping" "visit" {
  event_source_arn        = aws_sqs_queue.visit.arn
  function_name           = aws_lambda_function.fn.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

# Firehose -> S3
resource "aws_iam_role" "firehose" {
  name = "tf-spike-firehose"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "firehose.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "perm"
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["s3:*"], Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"] }]
  })
}

resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "tf-spike-raw"
  destination = "extended_s3"
  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.raw.arn
    prefix              = "raw/"
    buffering_interval  = 60
    buffering_size      = 1
  }
}

resource "aws_iam_role" "events_to_firehose" {
  name = "tf-spike-events-fh"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "events.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "events_to_firehose" {
  name = "perm"
  role = aws_iam_role.events_to_firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["firehose:PutRecord", "firehose:PutRecordBatch"], Resource = aws_kinesis_firehose_delivery_stream.raw.arn }]
  })
}

resource "aws_cloudwatch_event_rule" "b" {
  name           = "rule-b"
  event_bus_name = aws_cloudwatch_event_bus.bus.name
  event_pattern = jsonencode({
    source = ["onside.app", "onside.standdown"]
    detail = { poison = [{ exists = false }] }
  })
}

resource "aws_cloudwatch_event_target" "b_fh" {
  rule           = aws_cloudwatch_event_rule.b.name
  event_bus_name = aws_cloudwatch_event_bus.bus.name
  target_id      = "firehose"
  arn            = aws_kinesis_firehose_delivery_stream.raw.arn
  role_arn       = aws_iam_role.events_to_firehose.arn
}
