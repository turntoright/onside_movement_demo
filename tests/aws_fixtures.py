"""A moto copy of the Terraform stack (infra/terraform), for unit tests only."""

from __future__ import annotations

from dataclasses import dataclass

import boto3

REGION = "ap-southeast-2"
# The Terraform table names (infra/terraform/storage.tf; M7: the onside- prefix).
PERSON_TABLE = "onside-person-state"
EVALUATION_TABLE = "onside-evaluations"
PROPERTY_TABLE = "onside-properties"
TABLE_KEYS = {
    PERSON_TABLE: "person_id",
    EVALUATION_TABLE: "checkin_event_id",
    PROPERTY_TABLE: "property_id",
}


@dataclass
class MotoStack:
    dynamodb: object
    s3: object
    sqs: object
    events: object
    bucket: str
    bus_name: str
    queue_urls: dict[str, str]

    @classmethod
    def create(cls) -> MotoStack:
        dynamodb = boto3.client("dynamodb", region_name=REGION)
        for table, key in TABLE_KEYS.items():
            dynamodb.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        s3 = boto3.client("s3", region_name=REGION)
        bucket = "onside-local-000000000000"
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
        sqs = boto3.client("sqs", region_name=REGION)
        queue_urls = {
            name: sqs.create_queue(QueueName=f"onside-{name.replace('_', '-')}")["QueueUrl"]
            for name in ("visit_queue", "visit_dlq", "archive_queue", "archive_dlq")
        }
        events = boto3.client("events", region_name=REGION)
        events.create_event_bus(Name="onside-events")
        return cls(dynamodb, s3, sqs, events, bucket, "onside-events", queue_urls)

    def outputs(self) -> dict:
        """The same shape as `terraform output -json` (infra/terraform/outputs.tf)."""
        values = {
            "region": REGION,
            "bucket": self.bucket,
            "person_table": PERSON_TABLE,
            "evaluation_table": EVALUATION_TABLE,
            "property_table": PROPERTY_TABLE,
            "bus_name": self.bus_name,
            "visit_queue_url": self.queue_urls["visit_queue"],
            "visit_dlq_url": self.queue_urls["visit_dlq"],
            "archive_queue_url": self.queue_urls["archive_queue"],
            "archive_dlq_url": self.queue_urls["archive_dlq"],
            "standdown_function": "onside-standdown-checker",
            "archiver_function": "onside-archiver",
        }
        return {k: {"sensitive": False, "type": "string", "value": v} for k, v in values.items()}

    def put_properties(self, properties: dict[str, dict]) -> None:
        for property_id, prop in properties.items():
            self.dynamodb.put_item(
                TableName=PROPERTY_TABLE,
                Item={
                    "property_id": {"S": property_id},
                    "property_type": {"S": prop["property_type"]},
                    "owner_company_id": {"S": prop["owner_company_id"]},
                },
            )
