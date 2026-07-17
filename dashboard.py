import json
import time
from collections import deque

import streamlit as st
from confluent_kafka import Consumer, KafkaException

st.set_page_config(page_title="Orders Dashboard", layout="wide")
st.title("Live Orders Dashboard")

consumer_config = {
    'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094',
    'group.id': 'orders-dashboard',
    'auto.offset.reset': 'latest',
    'session.timeout.ms': 10000,
}

if "consumer" not in st.session_state:
    st.session_state.consumer = Consumer(consumer_config)
    st.session_state.consumer.subscribe(['orders'])
    st.session_state.messages = deque(maxlen=50)
    st.session_state.error_count = 0
    st.session_state.last_msg_time = None

status_placeholder = st.empty()
table_placeholder = st.empty()

consumer = st.session_state.consumer

# Poll for a burst of messages each rerun so you can see stalls clearly
polled_this_run = 0
start = time.time()
while time.time() - start < 2:
    msg = consumer.poll(timeout=0.5)
    if msg is None:
        continue
    if msg.error():
        st.session_state.error_count += 1
        continue
    try:
        order = json.loads(msg.value().decode('utf-8'))
    except Exception:
        continue
    order["_partition"] = msg.partition()
    order["_offset"] = msg.offset()
    st.session_state.messages.appendleft(order)
    st.session_state.last_msg_time = time.time()
    polled_this_run += 1

gap = None
if st.session_state.last_msg_time:
    gap = time.time() - st.session_state.last_msg_time

with status_placeholder.container():
    c1, c2, c3 = st.columns(3)
    c1.metric("Messages this refresh", polled_this_run)
    c2.metric("Seconds since last message", f"{gap:.1f}" if gap else "n/a")
    c3.metric("Consumer errors seen", st.session_state.error_count)
    if gap and gap > 5:
        st.warning(f"No new messages in {gap:.1f}s — pipeline may be stalled")

with table_placeholder.container():
    if st.session_state.messages:
        st.dataframe(list(st.session_state.messages), use_container_width=True)
    else:
        st.info("Waiting for messages...")

time.sleep(1)
st.rerun()