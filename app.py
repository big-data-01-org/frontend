import threading
import streamlit as st
from kafka_service.consumer import KafkaConsumer

kc = KafkaConsumer()
kc.subscribe("test-topic")
consumer_thread = threading.Thread(target=kc.consume_messages, args=(st,))
consumer_thread.start()

for message in kc.messages:
    st.write(message)



