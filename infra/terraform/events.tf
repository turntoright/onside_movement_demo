locals {
  dlq_retention_seconds = 14 * 24 * 3600
}

resource "aws_cloudwatch_event_bus" "events" {
  name = "onside-events"
}

# Rule A: app check-ins and check-outs to the stand-down queue. No poison filter: poison
# events must reach the queue to demo the DLQ. The source filter stops Lambda output loops.
resource "aws_cloudwatch_event_rule" "visits" {
  name           = "onside-visits"
  event_bus_name = aws_cloudwatch_event_bus.events.name
  event_pattern = jsonencode({
    source        = ["onside.app"]
    "detail-type" = ["CheckIn", "CheckOut"]
  })
}

# Rule B: everything from both sources except poison, to the archive queue (no transformer).
resource "aws_cloudwatch_event_rule" "archive" {
  name           = "onside-archive"
  event_bus_name = aws_cloudwatch_event_bus.events.name
  event_pattern = jsonencode({
    source = ["onside.app", "onside.standdown"]
    detail = { poison = [{ exists = false }] }
  })
}

resource "aws_sqs_queue" "visit_dlq" {
  name                      = "onside-visit-dlq"
  message_retention_seconds = local.dlq_retention_seconds
}

resource "aws_sqs_queue" "visit" {
  name                       = "onside-visit-queue"
  visibility_timeout_seconds = 30
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.visit_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "archive_dlq" {
  name                      = "onside-archive-dlq"
  message_retention_seconds = local.dlq_retention_seconds
}

resource "aws_sqs_queue" "archive" {
  name                       = "onside-archive-queue"
  visibility_timeout_seconds = 60 # spec 7.2 §6.5: batch window 5 s plus a 25 s Lambda timeout
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.archive_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_event_target" "visits" {
  rule           = aws_cloudwatch_event_rule.visits.name
  event_bus_name = aws_cloudwatch_event_bus.events.name
  target_id      = "visit-queue"
  arn            = aws_sqs_queue.visit.arn
}

resource "aws_cloudwatch_event_target" "archive" {
  rule           = aws_cloudwatch_event_rule.archive.name
  event_bus_name = aws_cloudwatch_event_bus.events.name
  target_id      = "archive-queue"
  arn            = aws_sqs_queue.archive.arn
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
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.visits.arn } }
    }]
  })
}

resource "aws_sqs_queue_policy" "archive" {
  queue_url = aws_sqs_queue.archive.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.archive.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.archive.arn } }
    }]
  })
}
