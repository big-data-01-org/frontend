from confluent_kafka import Consumer
import os
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
        os.write(1, b'Subscribed to topic...\n')

    def consume_messages(self):
        os.write(1, b'Waiting for messages...\n')
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    os.write(1, b'No messages found...\n')
                    continue
                if msg.error():
                    os.write(1, b'Consumer error...\n')
                    continue
                
                os.write(1, msg.value()+'\n')
                self.message = msg.value()
        except KeyboardInterrupt:
            pass
        finally:
            os.write(1, b'Closing consumer...\n')
            self.consumer.close()