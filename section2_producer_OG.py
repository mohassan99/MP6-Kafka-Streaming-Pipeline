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
   "b-1.xxx.amazonaws.com:9098", 
    "b-2.xxx.amazonaws.com:9098"
]

# =======================
# TODO: Implement IAM Authentication
# =======================
class MSKTokenProvider(AbstractTokenProvider):
    # code here

# =======================
# TODO: Initialize Kafka Producer
# =======================
producer = KafkaProducer(
    # code here
)

# Flask App
app = Flask(__name__)

# =======================
# TODO: Flask Route to Produce Message
# =======================
@app.route("/produce", methods=["POST"])
def produce():
   '''
   The jSON payload has the format {"topic": <TOPIC-NAME>, "message": <MESSAGE>}
   '''
    #code here

# =======================
# TODO: Health Route to check health
# =======================
@app.route("/health", methods=["GET"])
def health_check():
   # code here

# =======================
# Flask App Runner
# =======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
