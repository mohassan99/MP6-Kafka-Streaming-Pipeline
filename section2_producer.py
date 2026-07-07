from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import socket

# Configuration
# Copy your Kafka broker client below
REGION = 'us-east-1'
TOPIC_NAME = 'social_engagement'

#TODO: Provide your Kafka broker private endpoint here
KAFKA_BROKERS = [
    "b-1.mp6mskcluster.qlu57w.c7.kafka.us-east-1.amazonaws.com:9098",
    "b-2.mp6mskcluster.qlu57w.c7.kafka.us-east-1.amazonaws.com:9098"
]

# =======================
# TODO: Implement IAM Authentication
# =======================
class MSKTokenProvider(AbstractTokenProvider):
    # code here
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

# =======================
# TODO: Initialize Kafka Producer
# =======================
producer = KafkaProducer(
    # code here
    bootstrap_servers=KAFKA_BROKERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Flask App
app = Flask(__name__)

# =======================
# TODO: Flask Route to Produce Message
# =======================
@app.route("/produce", methods=["POST"])
def produce():
    '''
    The JSON payload has the format {"topic": <TOPIC-NAME>, "message": <MESSAGE>}
    '''
    #code here
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "Invalid or missing JSON payload"}), 400

        topic = data.get("topic")
        message = data.get("message")

        if not topic or message is None:
            return jsonify({"error": "Payload must include 'topic' and 'message'"}), 400

        producer.send(topic, value=message)
        producer.flush()

        return jsonify({
            "status": "success",
            "topic": topic,
            "message": message
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


# =======================
# TODO: Health Route to check health
# =======================
@app.route("/health", methods=["GET"])
def health_check():
    # code here
    try:
        return jsonify({
            "status": "UP",
            "host": socket.gethostname()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "DOWN",
            "error": str(e)
        }), 500


# =======================
# Flask App Runner
# =======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)