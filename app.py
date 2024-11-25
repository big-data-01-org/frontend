import threading
import streamlit as st
from kafka_service.consumer import KafkaConsumer
import time
import os
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
    
    # Create a placeholder for the messages
    
    # Function to update the message placeholder
    while True:
        if kafka_consumer.message:
            os.write(1,f"Message: {kafka_consumer.message}"+'\n')
            st.write(f"Message: {kafka_consumer.message}")
        time.sleep(1)  # Adjust the sleep time as needed
