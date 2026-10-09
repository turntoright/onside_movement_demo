# Read by the generator from state/<env>/outputs.json (`make deploy` saves them).
output "region" {
  value = var.region
}

output "bucket" {
  value = aws_s3_bucket.data.bucket
}

output "person_table" {
  value = aws_dynamodb_table.person_state.name
}

output "evaluation_table" {
  value = aws_dynamodb_table.evaluations.name
}

output "property_table" {
  value = aws_dynamodb_table.properties.name
}

output "bus_name" {
  value = aws_cloudwatch_event_bus.events.name
}

output "visit_queue_url" {
  value = aws_sqs_queue.visit.url
}

output "visit_dlq_url" {
  value = aws_sqs_queue.visit_dlq.url
}

output "archive_queue_url" {
  value = aws_sqs_queue.archive.url
}

output "archive_dlq_url" {
  value = aws_sqs_queue.archive_dlq.url
}

output "standdown_function" {
  value = aws_lambda_function.standdown_checker.function_name
}

output "archiver_function" {
  value = aws_lambda_function.archiver.function_name
}
