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
    
    # Input fields for country and year
    country = st.text_input("Country NOC", value="USA")
    year = st.text_input("Year", value="2020")

    # URL preview
    base_url = "http://10.123.3.156:30503/predict"
    constructed_url = f"{base_url}?country={country}&year={year}"

    # Button to trigger API request
    if st.button("Get Prediction"):
        try:
            # Make the GET request
            response = requests.get(constructed_url)
            
            # Display the response
            if response.status_code == 200:
                st.success("Request Successful!")
                st.write("Response:", response.text)
            else:
                st.error(f"Request failed with status code: {response.status_code}")
                st.write("Error details:", response.text)
        except Exception as e:
            st.error("An error occurred while making the request.")
            st.write(str(e))

    # Create a placeholder for the messages
    """
    # Function to update the message placeholder
    while True:
        if len(kafka_consumer.message) > 0:
            st.write(f"Message: {kafka_consumer.message}")
            kafka_consumer.message = ''
        #time.sleep(1)  # Adjust the sleep time as needed
    """