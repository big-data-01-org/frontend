import threading
import streamlit as st
from kafka_service.consumer import KafkaConsumer
import time

def run_consumer(consumer: KafkaConsumer):
    consumer.consume_messages()

if __name__ == "__main__":
    kafka_consumer = KafkaConsumer()
    kafka_consumer.subscribe('test-topic')

    consumer_thread = threading.Thread(target=run_consumer, args=(kafka_consumer,))
    consumer_thread.start()

    # Streamlit UI code here
    st.title("Kafka Consumer")
    st.write("Consuming messages from Kafka...")
    
    print("Consuming messages from Kafka...")

    # Create a placeholder for the messages
    message_placeholder = st.empty()

    # Function to update the message placeholder
    def update_messages():
        while True:
            if kafka_consumer.messages:
                message_placeholder.text_area("Messages", "\n".join(kafka_consumer.messages), height=300)
            time.sleep(1)  # Adjust the sleep time as needed

    # Start a thread to update the messages
    update_thread = threading.Thread(target=update_messages)
    update_thread.start()