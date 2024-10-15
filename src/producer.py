import json
import time

from kafka import KafkaProducer

bootstrap_server = ["kafka:9093"]


TOPIC_NAME = "additional"

prod = KafkaProducer(
    bootstrap_servers=bootstrap_server,
    acks='all',
    retries=5,
    api_version=(2, 0, 0)  # API 버전 명시적으로 설정
)

num = 0
for i in range(1, 101):
    num += i
    data = {'index': num}
    json_tf = json.dumps(data)
    prod.send(topic=TOPIC_NAME, value=json_tf.encode(encoding='utf-8'))
    # time.sleep(1)
    prod.flush()

prod.close()