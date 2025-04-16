import csv
import json
import time
import argparse
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def create_kafka_producer(bootstrap_servers):
    """Creates a Kafka producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=10,
            batch_size=16384
        )
        print(f"Kafka producer connected to {bootstrap_servers}")
        return producer
    except NoBrokersAvailable:
        print(f"Error: Could not connect to Kafka brokers at {bootstrap_servers}")
        print("Ensure Kafka is running and accessible.")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred creating Kafka producer: {e}")
        exit(1)

def stream_csv_to_kafka(csv_filepath, topic, producer, speed_factor=1.0):
    """Reads a CSV file row by row and sends data to a Kafka topic."""
    print(f"Starting to stream data from {csv_filepath} to Kafka topic '{topic}'...")
    count = 0
    try:
        with open(csv_filepath, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames
            print(f"CSV Headers: {headers}")

            for row in reader:
                message = {}
                for key, value in row.items():
                    if value is None or value == '':
                        message[key] = None
                        continue
                    try:
                        if key in ['DOT_CODE', 'FL_NUMBER', 'CRS_DEP_TIME', 'CRS_ARR_TIME']:
                             message[key] = int(value) if value else None
                        elif key in ['DEP_TIME', 'DEP_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON',
                                    'TAXI_IN', 'ARR_TIME', 'ARR_DELAY', 'CANCELLED', 'DIVERTED',
                                    'CRS_ELAPSED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE',
                                    'DELAY_DUE_CARRIER', 'DELAY_DUE_WEATHER', 'DELAY_DUE_NAS',
                                    'DELAY_DUE_SECURITY', 'DELAY_DUE_LATE_AIRCRAFT']:
                            message[key] = float(value) if value else None
                        else:
                            message[key] = value
                    except ValueError:
                        message[key] = value

                producer.send(topic, value=message)
                count += 1

                if count % 1000 == 0:
                    print(f"Sent {count} records...")
                    producer.flush()

                if speed_factor > 0:
                    time.sleep(1.0 / speed_factor if speed_factor > 0 else 0)

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        exit(1)
    except Exception as e:
        print(f"An error occurred during streaming: {e}")
    finally:
        print(f"\nFinished streaming. Total records sent: {count}")
        if producer:
            print("Flushing remaining messages...")
            producer.flush()
            print("Closing Kafka producer.")
            producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream CSV data to Kafka")
    parser.add_argument("--csv", default="flight_data.csv", help="Path to the input CSV file.")
    parser.add_argument("--topic", default="flight_data_stream", help="Kafka topic name.")
    parser.add_argument("--servers", default="localhost:9092", help="Kafka bootstrap servers (e.g., 'host1:port1,host2:port2').")
    parser.add_argument("--speed", type=float, default=100.0, help="Records per second simulation speed (0 for max speed).")

    args = parser.parse_args()

    kafka_producer = create_kafka_producer(args.servers)
    if kafka_producer:
        stream_csv_to_kafka(args.csv, args.topic, kafka_producer, args.speed)