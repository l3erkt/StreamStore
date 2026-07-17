import json
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition


consumer_config = {
    # Address of the Kafka broker
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',   # Consumer group ID
    'group.id': 'order-tracker',
    # Start reading from the earliest message
    'auto.offset.reset': 'earliest'  
}


consumer = Consumer(consumer_config)


consumer.assign([TopicPartition("orders", 0, 0)])
print("Consumer is running and subscribed to orders topic")

try:
    while True:
        # Poll for messages with a timeout of 1 second
        msg = consumer.poll(1.0)  

        if msg is None:
            continue  # No message received, continue polling
        if msg.error():
            print(f"Consumer error: {msg.error()}")  # Print any consumer errors
            continue
        
        value = msg.value().decode('utf-8')  # Decode the message value
        order = json.loads(value)
        
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']} (Order ID: {order['order_id']})")
        
except KeyboardInterrupt:
    print("Consumer interrupted by user")
    
    
finally:
    consumer.close()  # Close the consumer to free resources
    print("Consumer closed") 