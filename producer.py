from kafka import KafkaProducer
import requests
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_to_kafka(api_url, topic):
    response = requests.get(api_url, stream=True)
    if response.status_code == 200:
        for line in response.iter_lines():
            if line:
                try:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith("data: "):
                        message = json.loads(decoded_line.replace("data: ", ""))
                        producer.send(topic, message)
                        print(f"Poslata poruka: {message}")
                except json.JSONDecodeError:
                    print(f"Nevažeći JSON: {line}")
                except Exception as e:
                    print(f"Greska pri obradi poruke: {e}")
    producer.flush()

stream_to_kafka("http://localhost:5000/stream/type1", "test")
