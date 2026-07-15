import uuid                           # Generates unique IDs
from streamlit import json            # JSON library (typically use Python's built-in json module)
from confluent_kafka import Producer  # Kafka producer client

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'   # Address of the Kafka broker
}

# Create a Kafka producer instance
producer = Producer(producer_config)


def delivery_report(err, msg):
    if err: 
        # Print error if delivery fails
        print(f"Delivery Failed: {err}")  
    else:
        # Print success message
        print(f"Delivered {msg.value().decode('utf-8')}")
             


# Sample order message
order = {
    "order_id": str(uuid.uuid4()),    # Generate a unique order ID
    "user": "nana",                   # Customer name
    "item": "mushroom pizza",         # Item ordered
    "quantity": 2                     # Number of items ordered
}

# Convert the Python dictionary to JSON bytes
value = json.dumps(order).encode('utf-8')

# Send the message to the 'orders' Kafka topic
producer.produce(
    topic='orders',
    value=value,
    callback=delivery_report
)

# Wait until all queued messages have been delivered
producer.flush()