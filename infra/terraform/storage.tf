data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data" {
  # The account id keeps the name globally unique on AWS (LocalStack: 000000000000).
  bucket        = "onside-${var.env}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

# Deny any non-TLS access to the bucket.
resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_dynamodb_table" "person_state" {
  name         = "onside-person-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "person_id"

  attribute {
    name = "person_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "evaluations" {
  name         = "onside-evaluations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "checkin_event_id"

  attribute {
    name = "checkin_event_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "properties" {
  name         = "onside-properties"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "property_id"

  attribute {
    name = "property_id"
    type = "S"
  }
}
