import json
import time

from kafka import KafkaProducer

from energy_simulator import DEVICES, generate_reading


# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


TOPIC = "energy-data"


print("=" * 60)
print("ENERGY STREAM - KAFKA PRODUCER")
print("=" * 60)
print("Sending simulated IoT data to Kafka...")
print("Topic:", TOPIC)
print("Press Ctrl+C to stop")
print("=" * 60)


try:

    while True:

        # Generate readings for all virtual devices
        for device in DEVICES:

            reading = generate_reading(device)

            # Send reading to Kafka
            producer.send(TOPIC, value=reading)

            print(
                f"[SENT] "
                f"{reading['device_id']} | "
                f"{reading['device']} | "
                f"Power: {reading['power']} W"
            )

        # Make sure messages are delivered
        producer.flush()

        # Generate one batch every 2 seconds
        time.sleep(2)


except KeyboardInterrupt:

    print("\nProducer stopped by user.")


finally:

    producer.close()
