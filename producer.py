import uuid
import json
import time
import random
from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094',
    'acks': 'all',
    'retries': 5,
    'retry.backoff.ms': 500,
}

producer = Producer(producer_config)

ITEMS = ["chicken bowl", "burrito", "salad", "tacos", "quesadilla"]
USERS = ["nicole", "sam", "raj", "mei", "diego"]


def delivery_report(err, msg):
    if err:
        print(f"[{time.strftime('%H:%M:%S')}] DELIVERY FAILED: {err}")
    else:
        print(
            f"[{time.strftime('%H:%M:%S')}] OK -> "
            f"{msg.topic()} [partition {msg.partition()}] offset {msg.offset()}"
        )


def make_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user": random.choice(USERS),
        "item": random.choice(ITEMS),
        "quantity": random.randint(1, 3),
        "ts": time.time(),
    }


if __name__ == "__main__":
    print("Producing to 'orders' every 1s. Ctrl+C to stop.")
    try:
        while True:
            order = make_order()
            value = json.dumps(order).encode('utf-8')
            producer.produce(
                topic='orders',
                key=order["order_id"],
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)  # serve delivery callbacks without blocking
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping, flushing remaining messages...")
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            print(f"Warning: {remaining} messages were not delivered")