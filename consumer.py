import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "voc_audio_raw",
    "voc_text_ready",
    bootstrap_servers="localhost:9092",
    group_id="voc_multi_consumer",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🟢 Consumer started. Listening to voc_audio_raw and voc_text_ready...")

for message in consumer:
    topic = message.topic
    data = message.value
    
    if topic == "voc_audio_raw":
        print("STT Data\n", data)
    else:
        print("Text Data\n", data)