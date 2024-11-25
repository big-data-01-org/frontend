from confluent_kafka import Consumer
class KafkaConsumer:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': 'kafka-service:9092',
            'group.id': 'streamlit-consumer-group',
            'auto.offset.reset': 'earliest',
        }
        self.consumer = Consumer(self.consumer_config)
        self.message = ''

    def subscribe(self, topic: str):
        self.consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

    def consume_messages(self):
        print("Waiting for messages...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                print(f"Received message: {msg.value().decode('utf-8')}")
                self.message = msg.value().decode('utf-8')
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()