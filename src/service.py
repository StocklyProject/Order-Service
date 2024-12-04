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

async def get_user_from_session(session_id: str, redis):
    user_id_bytes = await redis.get(session_id)

    if user_id_bytes is None:
        raise HTTPException(status_code=403, detail="세션이 만료되었거나 유효하지 않습니다.")

    # 사용자 ID를 bytes에서 문자열로 변환 후 int로 변환
    user_id = int(user_id_bytes.decode('utf-8'))
    return int(user_id)


# 사용자 조회 함수
def get_user_by_id(user_id: int, db):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    cursor.close()

    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    return user

# 사용자 금액 충전 함수
def add_cash_to_user(user_id: int, amount: int, db):
    if amount <= 0:
        raise HTTPException(status_code=400, detail="충전 금액은 0보다 커야 합니다.")
    
    cursor = db.cursor()
    cursor.execute("UPDATE user SET cash = cash + %s WHERE id = %s", (amount, user_id))
    db.commit()
    cursor.close()

def reset_user_assets(user_id: int, db):
    """
    사용자의 자산(현금, 포트폴리오, 주문 상태)을 초기화합니다.
    """
    cursor = db.cursor()

    cursor.execute("""
        UPDATE stock_order
        SET is_deleted = TRUE
        WHERE user_id = %s
    """, (user_id,))

    cursor.execute("""
        UPDATE user
        SET cash = 0, total_stock = 0, total_roi = 0.0, total_asset = 0
        WHERE id = %s
    """, (user_id,))

    db.commit()
    cursor.close()