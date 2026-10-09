import json, os, time, boto3
_T0 = time.time()
EP = os.environ.get("AWS_ENDPOINT_URL")  # LocalStack injects AWS_ENDPOINT_URL into lambda containers
ddb = boto3.resource("dynamodb")
events = boto3.client("events")
TABLE = os.environ["TABLE"]; BUS = os.environ["BUS"]
_cold = True
def handler(event, context):
    global _cold
    if _cold:
        print("COLD_INIT_SECONDS", round(time.time() - _T0, 3), "endpoint", EP); _cold = False
    t = ddb.Table(TABLE)
    fails, out = [], []
    for rec in event["Records"]:
        try:
            env = json.loads(rec["body"])
            d = env["detail"]
            if d.get("poison"):
                raise ValueError("poison")
            t.put_item(Item={"event_id": d["event_id"], "seq": d.get("seq", 0), "dt": env["detail-type"]})
            out.append({"Source": "onside.standdown", "DetailType": "CheckInEvaluated",
                        "Detail": json.dumps({"event_id": d["event_id"] + "-eval"}), "EventBusName": BUS})
        except Exception as e:
            print("FAIL", rec["messageId"], e)
            fails.append({"itemIdentifier": rec["messageId"]})
    if out:
        r = events.put_events(Entries=out)
        if r["FailedEntryCount"]:
            raise RuntimeError("put_events failed")
    return {"batchItemFailures": fails}
