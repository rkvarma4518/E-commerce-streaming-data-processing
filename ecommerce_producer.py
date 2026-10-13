"""
Simple E-commerce streaming data generator.

- Generates random events: page_view, add_to_cart, order_placed, payment
- Prints every event in the terminal
- Sends every event to a Kafka topic
- Reads all secrets/config from a .env file

Run:  python ecommerce_producer.py
Stop: Ctrl + C
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------
# 1. Load settings from .env file
# ---------------------------------------------------------------
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
API_KEY = os.getenv("KAFKA_API_KEY")        # optional (needed for cloud Kafka)
API_SECRET = os.getenv("KAFKA_API_SECRET")  # optional (needed for cloud Kafka)
INTERVAL = float(os.getenv("EVENT_INTERVAL_SECONDS", "1"))

# ---------------------------------------------------------------
# 2. Create Kafka producer
# ---------------------------------------------------------------
kafka_config = {"bootstrap.servers": BOOTSTRAP_SERVERS}

# If API key/secret are given, connect securely (e.g. Confluent Cloud)
if API_KEY and API_SECRET:
    kafka_config.update({
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": API_KEY,
        "sasl.password": API_SECRET,
    })

producer = Producer(kafka_config)

# ---------------------------------------------------------------
# 3. Sample data used to build random events
# ---------------------------------------------------------------
PRODUCTS = [
    {"product_id": "P101", "name": "Wireless Mouse", "price": 799},
    {"product_id": "P102", "name": "Mechanical Keyboard", "price": 2999},
    {"product_id": "P103", "name": "USB-C Charger", "price": 1299},
    {"product_id": "P104", "name": "Bluetooth Speaker", "price": 1999},
    {"product_id": "P105", "name": "Laptop Bag", "price": 1499},
    {"product_id": "P106", "name": "Smart Watch", "price": 4999},
]
CITIES = ["Mumbai", "Delhi", "Pune", "Nashik", "Bengaluru", "Chennai"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]
PAYMENT_STATUS = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]  # mostly success


# ---------------------------------------------------------------
# 4. Functions that create each type of event
# ---------------------------------------------------------------
def base_event(event_type):
    """Fields that every event has."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": f"U{random.randint(1, 50)}",
        "city": random.choice(CITIES),
    }


def page_view_event():
    event = base_event("page_view")
    event["product_id"] = random.choice(PRODUCTS)["product_id"]
    return event


def add_to_cart_event():
    product = random.choice(PRODUCTS)
    event = base_event("add_to_cart")
    event["product_id"] = product["product_id"]
    event["quantity"] = random.randint(1, 3)
    return event


def order_event():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    event = base_event("order_placed")
    event["order_id"] = f"O{random.randint(10000, 99999)}"
    event["product_id"] = product["product_id"]
    event["product_name"] = product["name"]
    event["quantity"] = quantity
    event["total_amount"] = product["price"] * quantity
    return event


def payment_event():
    event = base_event("payment")
    event["order_id"] = f"O{random.randint(10000, 99999)}"
    event["payment_method"] = random.choice(PAYMENT_METHODS)
    event["payment_status"] = random.choice(PAYMENT_STATUS)
    event["amount"] = random.choice(PRODUCTS)["price"] * random.randint(1, 3)
    return event


# Page views happen more often than orders, just like a real store
EVENT_GENERATORS = [
    page_view_event, page_view_event, page_view_event,
    add_to_cart_event, add_to_cart_event,
    order_event,
    payment_event,
]


# ---------------------------------------------------------------
# 5. Kafka delivery report (tells us if message reached Kafka)
# ---------------------------------------------------------------
def delivery_report(error, message):
    if error is not None:
        print(f"Failed to send: {error}")
    else:
        print(f"Sent to topic '{message.topic()}' "
              f"[partition {message.partition()}, offset {message.offset()}]")


# ---------------------------------------------------------------
# 6. Main loop: generate -> print -> send to Kafka
# ---------------------------------------------------------------
def main():
    print(f"Sending events to Kafka topic '{TOPIC}' at {BOOTSTRAP_SERVERS}")
    print("Press Ctrl + C to stop\n")

    try:
        while True:
            event = random.choice(EVENT_GENERATORS)()   # create random event

            print(json.dumps(event))                    # print in terminal

            producer.produce(                           # send to Kafka
                topic=TOPIC,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)                            # trigger delivery report

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()                                # send anything still pending
        print("Done.")


if __name__ == "__main__":
    main()
