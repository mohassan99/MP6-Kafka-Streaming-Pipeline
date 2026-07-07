from kafka import KafkaConsumer
import json
import happybase
import time
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.sasl.oauth import AbstractTokenProvider
from flask import Flask, jsonify
import threading

app = Flask(__name__)
is_ready = False

@app.route('/health', methods=['GET'])
def health_check():
    if is_ready:
        return jsonify({"status": "UP"}), 200
    return jsonify({"status": "INITIALIZING"}), 503

def start_flask():
    app.run(host='0.0.0.0', port=5000)

threading.Thread(target=start_flask, daemon=True).start()

# TODO 1: IAM Authentication
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

# TODO 2: Kafka broker endpoints
KAFKA_BROKERS = [
    "b-1.mp6mskcluster.qlu57w.c7.kafka.us-east-1.amazonaws.com:9098",
    "b-2.mp6mskcluster.qlu57w.c7.kafka.us-east-1.amazonaws.com:9098"
]
TOPIC = "social_engagement"

# TODO 3: HBase config
HBASE_HOST = "172.31.29.27"
HBASE_PORT = 9090
TABLE_NAME = "post_engagement"

hbase_conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)

def get_hbase_table():
    global hbase_conn
    try:
        if not hbase_conn.transport.isOpen():
            hbase_conn.transport.open()
    except Exception:
        print("Reconnecting to HBase...")
        hbase_conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    return hbase_conn.table(TABLE_NAME)

# TODO 4: Store in HBase
def store_in_hbase(message):
    try:
        post_id = message.get('post_id')
        user_id = message.get('user_id')
        if not post_id or not user_id:
            print("Invalid message: missing post_id or user_id. Skipping.")
            return
        likes = message.get('likes', '0')
        comments = message.get('comments', '0')
        timestamp = message.get('timestamp', str(int(time.time())))
        row_key = f"{post_id}_{timestamp}"
        table = get_hbase_table()
        table.put(row_key.encode(), {
            b'engagement:post_id': str(post_id).encode(),
            b'engagement:user_id': str(user_id).encode(),
            b'engagement:likes': str(likes).encode(),
            b'engagement:comments': str(comments).encode(),
            b'engagement:timestamp': str(timestamp).encode(),
        })
        print(f"Stored row {row_key} in HBase")
    except Exception as e:
        print(f"Error storing engagement in HBase: {e}")

# TODO 5: Kafka Consumer
try:
    tp = MSKTokenProvider()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        group_id='engagement_analytics_group',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"Listening for engagement events on topic: {TOPIC}...")
    is_ready = True
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")

if is_ready:
    for message in consumer:
        try:
            print(f"Received: {message.value}")
            store_in_hbase(message.value)
            time.sleep(1)
        except json.JSONDecodeError:
            print("Error decoding JSON message. Skipping...")
        except Exception as e:
            print(f"Unexpected error: {e}")