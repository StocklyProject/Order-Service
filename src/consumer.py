import json
from kafka import KafkaProducer, KafkaConsumer

bootstrap_server = ["kafka:9093"]

TOPIC_NAME = "additional"
ODD_TOPIC = "odd"
EVEN_TOPIC = "even"

# KafkaConsumer 설정
consume = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=bootstrap_server,
    group_id='my-consumer-group',
    auto_offset_reset='earliest',
    api_version=(2, 0, 0)  # API 버전 명시
)

# KafkaProducer 설정
prod = KafkaProducer(
    bootstrap_servers=bootstrap_server,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 0, 0)  # API 버전 명시
)


# 짝수 여부 판단 함수
def is_even(n: int) -> bool:
    return n % 2 == 0


# Kafka 메시지 처리
for message in consume:
    try:
        # 메시지에서 인덱스 추출
        index = json.loads(message.value.decode())["index"]

        # 짝수/홀수에 따른 토픽 결정
        topic_trans = EVEN_TOPIC if is_even(index) else ODD_TOPIC

        # 데이터 전송
        data = {'index': index}
        prod.send(topic=topic_trans, value=data)

        print(f"Sent to {topic_trans}: {index} (Even: {is_even(index)})")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except KeyError:
        print("Error: 'index' key not found in the message.")
    except Exception as e:
        print(f"Error processing message: {e}")

# 리소스 정리
consume.close()
prod.flush()  # Flush 후 close
prod.close()
