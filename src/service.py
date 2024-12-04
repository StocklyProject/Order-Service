from .consumer import async_kafka_consumer
import json
import asyncio
from fastapi import HTTPException
from datetime import datetime
from .consumer import async_kafka_consumer
from kafka import TopicPartition

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

    try:
        # cash 업데이트
        cursor.execute("UPDATE user_data SET cash = cash + %s WHERE id = %s", (amount, user_id))

        # total_asset 업데이트
        cursor.execute("""
            UPDATE user_data 
            SET total_asset = cash + total_stock
            WHERE id = %s
        """, (user_id,))

        # 변경사항 커밋
        db.commit()
    except Exception as e:
        db.rollback()  # 문제가 생기면 롤백
        raise HTTPException(status_code=500, detail="서버 오류로 인해 요청을 처리할 수 없습니다.") from e
    finally:
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
        UPDATE user_data
        SET cash = 0, total_stock = 0, total_roi = 0.0, total_asset = 0
        WHERE id = %s
    """, (user_id,))

    db.commit()
    cursor.close()



async def fetch_latest_data_for_symbol(symbol: str):
    """
    Kafka에서 특정 토픽의 특정 심볼에 해당하는 최신 데이터 한 개를 가져오는 비동기 함수.
    """
    consumer = await async_kafka_consumer('real_time_asking_prices', f"order_consumer_{datetime.now().strftime('%Y%m%d%H%M%S%f')}")
    try:
        async for msg in consumer:
            data = msg.value
            if data.get("symbol") == symbol:
                return data  

    finally:
        await consumer.stop()

    return None