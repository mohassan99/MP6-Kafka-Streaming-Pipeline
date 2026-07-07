import requests
import json

# === Fill in your identity info ===
YOUR_EMAIL = "<your-email>@illinois.edu"
YOUR_SECRET = "<REDACTED-grading-secret>"

# === Fill in your infrastructure values ===
HBASE_TABLE = "post_engagement"
HBASE_MASTER_IP = "<hbase-master-ip>"
PRODUCER_IP = "<producer-ip>:5000"
CONSUMER1_IP = "<consumer1-ip>:5000"
CONSUMER2_IP = "<consumer2-ip>:5000"

# === API Gateway URL ===
API_GATEWAY_URL = "https://2e37erweu6.execute-api.us-east-1.amazonaws.com/PROD/"

# === Prepare payload ===
input_payload = {
    "submitterEmail": YOUR_EMAIL,
    "secret": YOUR_SECRET,
    "hbase_table": HBASE_TABLE,
    "hbase_master_ip": HBASE_MASTER_IP,
    "producer_ip": PRODUCER_IP,
    "consumer1_ip": CONSUMER1_IP,
    "consumer2_ip": CONSUMER2_IP
}

# === Final payload for API Gateway ===
execution_payload = {
    "input": input_payload,
    "stateMachineArn": "arn:aws:states:us-east-1:913708708374:stateMachine:mp7_kafka_grader"
}

# === Send the POST request to the token validation Lambda API ===
try:
    response = requests.post(API_GATEWAY_URL, json={
    "body": json.dumps(execution_payload)  # Wrap payload in "body"
})
    print("Status:", response.status_code, response.reason)
    print("Response Text:\n", response.text)
except Exception as e:
    print("Failed to invoke the token validation Lambda API:", e)
