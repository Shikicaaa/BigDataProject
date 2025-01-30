from kafka import KafkaConsumer
import csv
import json

CSV_FILE = "udemy_courses.csv"

# Definisanje potrošača
consumer = KafkaConsumer(
    "test",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

HEADER = [
    "id", "title", "url", "is_paid", "instructor_names", "category", "headline",
    "num_subscribers", "rating", "num_reviews", "instructional_level", "objectives", "curriculum"
]

def write_to_csv(data):
    file_exists = False
    try:
        with open(CSV_FILE, "r"):
            file_exists = True
    except FileNotFoundError:
        pass

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=HEADER)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def consume_and_store():
    for message in consumer:
        data = message.value
        write_to_csv(data)
        print(f"Stored: {data['title']}")

if __name__ == "__main__":
    consume_and_store()
