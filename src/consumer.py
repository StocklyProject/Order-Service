from aiokafka import AIOKafkaConsumer
import json

async def async_kafka_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=['192.168.10.26:9094'],
        # bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        group_id=group_id,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        max_poll_interval_ms=300000,
        session_timeout_ms=45000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )
    await consumer.start()
    return consumer
