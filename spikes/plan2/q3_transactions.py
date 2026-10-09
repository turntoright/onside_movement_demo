import sys, json, boto3, botocore
from decimal import Decimal
mode = sys.argv[1]
if mode == "moto":
    from moto import mock_aws
    m = mock_aws(); m.start(); kw = {}
else:
    kw = dict(endpoint_url="http://localhost:4566")
kw.update(region_name="ap-southeast-2", aws_access_key_id="t", aws_secret_access_key="t")
c = boto3.client("dynamodb", **kw); r = boto3.resource("dynamodb", **kw)
for n, k in (("person_state", "person_id"), ("evaluations", "checkin_event_id")):
    try: c.delete_table(TableName=n)
    except Exception: pass
    c.create_table(TableName=n, KeySchema=[{"AttributeName": k, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": k, "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
def tx(person, read, ev):
    cond = "attribute_not_exists(person_id)" if read == 0 else "version = :read"
    vals = {":new": {"N": str(read + 1)}}
    if read: vals[":read"] = {"N": str(read)}
    return dict(TransactItems=[
      {"Update": {"TableName": "person_state", "Key": {"person_id": {"S": person}},
        "UpdateExpression": "SET version = :new", "ConditionExpression": cond,
        "ExpressionAttributeValues": vals}},
      {"Put": {"TableName": "evaluations", "Item": {"checkin_event_id": {"S": ev}, "x": {"N": "1"}},
        "ConditionExpression": "attribute_not_exists(checkin_event_id)"}}])
def run(label, args):
    try:
        c.transact_write_items(**tx(*args)); print(label, "OK")
    except botocore.exceptions.ClientError as e:
        print(label, type(e).__name__, e.response["Error"], "| CancellationReasons=", json.dumps(e.response.get("CancellationReasons")))
        print("   str:", str(e)[:300])
run("setup first (version 0, ev E1)", ("p1", 0, "E1"))     # p1 v1, E1 exists
run("(a) version conflict only (read=0 but exists, new ev E2)", ("p1", 0, "E2"))
run("(a2) version conflict only (read=5, new ev E3)", ("p1", 5, "E3"))
run("(b) eval exists only (read=1 correct, ev E1)", ("p1", 1, "E1"))
run("(c) both (read=0 stale, ev E1)", ("p1", 0, "E1"))
run("(c2) both (read=7, ev E1)", ("p1", 7, "E1"))
run("(ok) correct (read=1, E4)", ("p1", 1, "E4"))
# decimals
it = c.get_item(TableName="person_state", Key={"person_id": {"S": "p1"}}, ConsistentRead=True)["Item"]
print("client get version:", it["version"], type(it["version"]["N"]))
it2 = r.Table("person_state").get_item(Key={"person_id": "p1"}, ConsistentRead=True)["Item"]
print("resource get version:", repr(it2["version"]), type(it2["version"]).__name__)
# resource-level transaction? resource.meta.client uses TypeSerializer
cc = r.meta.client
try:
    cc.transact_write_items(TransactItems=[{"Put":{"TableName":"evaluations","Item":{"checkin_event_id":"E1","x":1},"ConditionExpression":"attribute_not_exists(checkin_event_id)"}}])
except botocore.exceptions.ClientError as e:
    print("resource.meta.client:", type(e).__name__, json.dumps(e.response.get("CancellationReasons")))
