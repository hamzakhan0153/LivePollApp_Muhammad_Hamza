from kafka import KafkaProducer
import json
from poll_response_api import PollResponseAPI

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    compression_type='gzip',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    #linger_ms=10,         
    batch_size=16384
)

topic_name = "LivePoll"

poll_api = PollResponseAPI()

print(json.dumps(poll_api.survey_questions, indent=4))

print("Starting Kafka Producer...")

try:
    while True:
        # Generate fake response
        response = poll_api.poll_response_api()

        # Send to Kafka
        producer.send(topic_name, value=response)

        print("Sent:", response)


except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()
