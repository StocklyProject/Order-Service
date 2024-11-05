from .consumer import async_kafka_consumer
import json
import asyncio

# SSE 비동기 이벤트 생성기
async def sse_event_generator(topic: str, group_id: str, symbol: str):
    consumer = await async_kafka_consumer(topic, group_id)
    try:
        async for message in consumer:
            # 메시지의 값을 JSON으로 파싱
            try:
                data = json.loads(message.value) if isinstance(message.value, str) else message.value
            except json.JSONDecodeError:
                continue

            # JSON으로 파싱된 데이터에서 symbol을 확인
            if isinstance(data, dict) and data.get("symbol") == symbol:
                yield f"data: {json.dumps(data)}\n\n"  # 클라이언트에 데이터 전송

            await asyncio.sleep(0.5)  # 메시지 간 대기 시간 설정
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()