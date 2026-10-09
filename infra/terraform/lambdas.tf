# Built by `make package` (build/ is gitignored); boto3 comes from the runtime.
data "archive_file" "standdown_checker" {
  type        = "zip"
  source_dir  = "${path.module}/../../build/standdown_checker"
  output_path = "${path.module}/../../build/standdown_checker.zip"
}

data "archive_file" "archiver" {
  type        = "zip"
  source_dir  = "${path.module}/../../build/archiver"
  output_path = "${path.module}/../../build/archiver.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_cloudwatch_log_group" "standdown_checker" {
  name              = "/aws/lambda/onside-standdown-checker"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "archiver" {
  name              = "/aws/lambda/onside-archiver"
  retention_in_days = 7
}

resource "aws_iam_role" "standdown_checker" {
  name               = "onside-standdown-checker"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "standdown_checker" {
  name = "standdown-checker"
  role = aws_iam_role.standdown_checker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
        Resource = aws_sqs_queue.visit.arn
      },
      {
        # TransactWriteItems is authorised per contained action: Update on person_state,
        # Put on evaluations.
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
        Resource = aws_dynamodb_table.person_state.arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem"]
        Resource = aws_dynamodb_table.evaluations.arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem"]
        Resource = aws_dynamodb_table.properties.arn
      },
      {
        Effect   = "Allow"
        Action   = ["events:PutEvents"]
        Resource = aws_cloudwatch_event_bus.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.standdown_checker.arn}:*"
      },
    ]
  })
}

resource "aws_iam_role" "archiver" {
  name               = "onside-archiver"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "archiver" {
  name = "archiver"
  role = aws_iam_role.archiver.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
        Resource = aws_sqs_queue.archive.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.data.arn}/raw/*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.archiver.arn}:*"
      },
    ]
  })
}

resource "aws_lambda_function" "standdown_checker" {
  function_name    = "onside-standdown-checker"
  role             = aws_iam_role.standdown_checker.arn
  runtime          = "python3.12"
  handler          = "standdown_checker.handler.handler"
  filename         = data.archive_file.standdown_checker.output_path
  source_code_hash = data.archive_file.standdown_checker.output_base64sha256
  timeout          = 25 # below the visit queue's 30 s visibility timeout
  memory_size      = 256

  environment {
    variables = {
      PERSON_TABLE     = aws_dynamodb_table.person_state.name
      EVALUATION_TABLE = aws_dynamodb_table.evaluations.name
      PROPERTY_TABLE   = aws_dynamodb_table.properties.name
      EVENT_BUS_NAME   = aws_cloudwatch_event_bus.events.name
      PYTHONTZPATH     = "" # zoneinfo uses the bundled tzdata, not the system database (spec §2)
    }
  }

  depends_on = [aws_cloudwatch_log_group.standdown_checker, aws_iam_role_policy.standdown_checker]
}

resource "aws_lambda_function" "archiver" {
  function_name    = "onside-archiver"
  role             = aws_iam_role.archiver.arn
  runtime          = "python3.12"
  handler          = "archiver.handler.handler"
  filename         = data.archive_file.archiver.output_path
  source_code_hash = data.archive_file.archiver.output_base64sha256
  timeout          = 25 # below the archive queue's 60 s visibility timeout
  memory_size      = 256

  environment {
    variables = {
      BUCKET = aws_s3_bucket.data.bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.archiver, aws_iam_role_policy.archiver]
}

resource "aws_lambda_event_source_mapping" "visits" {
  event_source_arn        = aws_sqs_queue.visit.arn
  function_name           = aws_lambda_function.standdown_checker.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 5
  }
}

resource "aws_lambda_event_source_mapping" "archive" {
  event_source_arn = aws_sqs_queue.archive.arn
  function_name    = aws_lambda_function.archiver.arn
  # AWS rejects a batch size above 10 without a batching window (spec §6.5).
  batch_size                         = 100
  maximum_batching_window_in_seconds = 5

  scaling_config {
    maximum_concurrency = 2
  }
}
