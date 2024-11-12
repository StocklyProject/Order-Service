from .consumer import async_kafka_consumer
import json
import asyncio
from .database import get_redis
from fastapi import FastAPI, Depends, Request, HTTPException

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

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()

async def get_user_id_from_session(request: Request, redis=Depends(get_redis)):
    # 쿠키에서 session_id 가져오기
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID is missing")

    # Redis에서 session_id로 user_id 조회
    user_id = await redis.get(session_id)
    if not user_id:
        raise HTTPException(status_code=401, detail="Session has expired or is invalid")

    return user_id.decode('utf-8')  # Redis에서 가져온 값을 문자열로 반환