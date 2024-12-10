import threading
import streamlit as st
from kafka_service.consumer import KafkaConsumer
import time
import os
import requests

def run_consumer(consumer: KafkaConsumer):
    consumer.consume_messages()

if __name__ == "__main__":
    kafka_consumer = KafkaConsumer()
    kafka_consumer.subscribe('olympics')

    consumer_thread = threading.Thread(target=run_consumer, args=(kafka_consumer,))
    consumer_thread.start()

    # Streamlit UI code here
    st.title("Kafka Consumer")
    st.write("Consuming messages from Kafka...")
    
    request = "http://10.123.3.156:30503/predict?country=USA&year=2020"

    response = requests.get(request)

    st.write(response.result)

    # Create a placeholder for the messages
    """
    # Function to update the message placeholder
    while True:
        if len(kafka_consumer.message) > 0:
            st.write(f"Message: {kafka_consumer.message}")
            kafka_consumer.message = ''
        #time.sleep(1)  # Adjust the sleep time as needed
    """