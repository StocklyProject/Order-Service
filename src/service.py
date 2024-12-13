from .consumer import async_kafka_consumer
import json
import asyncio
from fastapi import HTTPException
from datetime import datetime, timedelta
from .consumer import async_kafka_consumer
from kafka import TopicPartition
from .logger import logger

async def sse_event_generator(topic: str, group_id: str, symbol: str):
    """ SSE 비동기 이벤트 생성기
    Args:
        topic (str): Kafka 토픽 이름
        group_id (str): Kafka 그룹 ID
        symbol (str): 필터링할 심볼 (종목) 이름
    """
    consumer = await async_kafka_consumer(topic, group_id)
    try:
        logger.info(f"Kafka 소비자 시작됨 (topic: {topic}, group_id: {group_id}, symbol: {symbol})")

        # Kafka 메시지 소비 루프
        async for message in consumer:
            try:
                # 메시지를 JSON으로 파싱
                if isinstance(message.value, (bytes, str)):
                    data = json.loads(
                        message.value.decode('utf-8') if isinstance(message.value, bytes) else message.value)
                else:
                    data = message.value

                # symbol에 맞는 데이터 필터링
                if isinstance(data, dict) and data.get("symbol") == symbol:
                    formatted_data = json.dumps(data)
                    yield f"data: {formatted_data}\n\n"  # 클라이언트에 데이터 전송
                    logger.debug(f"전송된 데이터: {formatted_data}")

            except json.JSONDecodeError as e:
                logger.warning(f"JSON 파싱 오류: {e} (message: {message.value})")
                continue
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {e}")
                continue

    except asyncio.CancelledError:
        logger.warning(f"SSE 연결 취소됨 (topic: {topic}, symbol: {symbol})")
        raise  # 취소 요청을 재전파하여 FastAPI에 정상적으로 알림
    except Exception as e:
        logger.error(f"소비자 비정상 종료: {e}")
    finally:
        # 커넥션 종료 보장
        if consumer is not None:
            try:
                await consumer.stop()
                logger.info(f"Kafka 소비자 종료됨 (topic: {topic}, group_id: {group_id})")
            except Exception as e:
                logger.error(f"Kafka 소비자 종료 중 오류 발생: {e}")

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
        logger.critical(f"Error adding cash to user: {e}")
        raise HTTPException(status_code=500, detail="서버 오류로 인해 요청을 처리할 수 없습니다") from e
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

def get_stocks(user_id: int, db):
    """
    사용자의 모든 주식 주문 내역을 조회합니다.
    날짜(created_at), 종목(회사 이름), 거래유형(type), 수량(quantity), 가격(price), 체결 여부(status)를 반환합니다.
    """
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            so.created_at AS date,
            c.name AS company_name,
            so.type AS order_type,
            so.quantity,
            so.price,
            so.status
        FROM stock_order so
        INNER JOIN company c ON so.company_id = c.id
        WHERE so.user_id = %s AND so.is_deleted = 0
        ORDER BY so.created_at DESC
    """, (user_id,))
    results = cursor.fetchall()
    cursor.close()
    return results


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


# 일일 종합 주식 수익률 업데이트 함수 
def update_daily_roi_for_all_users(db):
    cursor = db.cursor(dictionary=True)

    try:
        logger.critical("Starting portfolio ROI calculation.")

        # 1. 모든 사용자 데이터 조회
        cursor.execute("SELECT DISTINCT user_id FROM user_data WHERE is_deleted = FALSE")
        users = cursor.fetchall()
        logger.critical("Fetched users: %s", users)

        today = datetime.now().date()

        for user in users:
            user_id = user["user_id"]
            logger.critical("Processing user ID: %s", user_id)

            # 2.1 사용자 보유 주식 정보 가져오기
            cursor.execute("""
                SELECT company.symbol,
                       SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END) -
                       SUM(CASE WHEN so.type = '매도' THEN so.quantity ELSE 0 END) AS total_quantity,
                       SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) AS total_investment
                FROM stock_order so
                INNER JOIN company ON so.company_id = company.id
                WHERE so.user_id = %s AND so.is_deleted = FALSE
                GROUP BY company.symbol
            """, (user_id,))
            holdings = cursor.fetchall()
            logger.critical("Holdings for user %s: %s", user_id, holdings)

            if not holdings:
                logger.critical("No holdings found for user ID: %s", user_id)
                continue

            # 2.2 현재 주식 가격 가져오기
            cursor.execute("""
                SELECT symbol, close
                FROM stock
                WHERE date = %s AND is_daily = TRUE
            """, (today,))
            current_prices = {row["symbol"]: row["close"] for row in cursor.fetchall()}
            logger.critical("Current prices on %s: %s", today, current_prices)

            # 2.3 포트폴리오 수익률 계산
            total_investment = 0
            weighted_sum = 0
            for holding in holdings:
                symbol = holding["symbol"]
                quantity = holding["total_quantity"]
                total_investment_for_symbol = holding["total_investment"]

                current_price = current_prices.get(symbol, 0)
                if quantity > 0 and total_investment_for_symbol > 0:
                    # 종목 수익률 계산
                    roi = ((current_price - (total_investment_for_symbol / quantity)) / 
                           (total_investment_for_symbol / quantity)) * 100
                    roi = round(roi, 2)
                    weighted_sum += roi * total_investment_for_symbol
                    total_investment += total_investment_for_symbol
                    logger.critical(
                        "Symbol: %s, ROI: %s, Weighted Contribution: %s",
                        symbol, roi, roi * total_investment_for_symbol
                    )

            # 포트폴리오 수익률 계산
            if total_investment > 0:
                portfolio_roi = weighted_sum / total_investment
                portfolio_roi = round(portfolio_roi, 2)
            else:
                portfolio_roi = 0
            logger.critical("User ID: %s, Total Investment: %s, Portfolio ROI: %s", user_id, total_investment, portfolio_roi)

            # 2.4 cash 값을 별도로 조회
            cursor.execute("""
                SELECT cash FROM user_data WHERE user_id = %s LIMIT 1
            """, (user_id,))
            cash = cursor.fetchone()["cash"]

            # 2.5 같은 날짜인지 확인 후 업데이트 또는 삽입
            cursor.execute("""
                SELECT * FROM user_data
                WHERE user_id = %s AND DATE(created_at) = %s
            """, (user_id, today))
            existing_data = cursor.fetchone()

            if existing_data:
                # 같은 날짜면 업데이트
                cursor.execute("""
                    UPDATE user_data
                    SET total_roi = %s, total_asset = %s, total_stock = %s, 
                        cash = %s, is_daily = TRUE, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND DATE(created_at) = %s
                """, (portfolio_roi, total_investment, total_investment, cash, user_id, today))
                logger.critical("Updated record for user ID: %s, Date: %s", user_id, today)
            else:
                # 다른 날짜면 새 데이터 삽입
                cursor.execute("""
                    INSERT INTO user_data (user_id, total_roi, total_asset, total_stock, cash, is_daily, created_at)
                    VALUES (%s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                """, (user_id, portfolio_roi, total_investment, total_investment, cash))
                logger.critical("Inserted new record for user ID: %s, Date: %s", user_id, today)

        db.commit()
        logger.critical("Portfolio ROI calculation completed successfully.")

    except Exception as e:
        db.rollback()
        logger.critical("Error calculating portfolio ROI: %s", e)
    finally:
        cursor.close()


async def get_latest_roi_from_session(session_id: str, redis, db):
    user_id = await get_user_from_session(session_id, redis)
    try:
        cursor = db.cursor(dictionary=True)  # 🔥 dictionary=True로 변경하면 자동으로 dict로 반환됩니다.

        # 🔥 1️⃣ user_data 테이블에서 기본 정보 가져오기
        query = """
        SELECT 
            total_roi, 
            total_stock, -- 보유 주식의 총 시세 (현재 시장 가치)
            cash -- 보유 현금
        FROM 
            user_data 
        WHERE 
            user_id = %s 
            AND is_deleted = FALSE 
        ORDER BY updated_at DESC 
        LIMIT 1;
        """
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="해당 유저의 데이터를 찾을 수 없습니다.")

        # 🔥 DB로부터 받은 결과 매핑
        total_roi = float(result['total_roi']) if result['total_roi'] is not None else 0.0  # 수익률
        total_stock_value = float(result['total_stock']) if result['total_stock'] is not None else 0.0  # 현재 보유 중인 주식의 총 시세
        cash = float(result['cash']) if result['cash'] is not None else 0.0  # 보유 현금

        # 🔥 2️⃣ 보유 주식의 총 매수 금액 (total_investment) 계산
        try:
            cursor.execute("""
            SELECT 
                SUM((so.quantity - IFNULL(sq.sold_quantity, 0)) * so.price) AS total_investment 
            FROM stock_order so 
            LEFT JOIN (
                SELECT so2.company_id, SUM(so2.quantity) AS sold_quantity 
                FROM stock_order so2 
                WHERE so2.user_id = %s AND so2.type = '매도' AND so2.is_deleted = FALSE 
                GROUP BY so2.company_id
            ) sq ON so.company_id = sq.company_id 
            WHERE so.user_id = %s AND so.type = '매수' AND so.is_deleted = FALSE;
            """, (user_id, user_id))
            total_investment = cursor.fetchone()['total_investment']  # 🔥 dictionary로 받기 때문에 'total_investment'로 접근
            total_investment = float(total_investment) if total_investment is not None else 0.0  # 🔥 float 변환
        except Exception as e:
            logger.error("Failed to fetch total investment for User ID %s: %s", user_id, e)
            total_investment = 0.0

        # 🔥 3️⃣ 자산 차이(asset_difference) 계산
        asset_difference = float(total_stock_value) - float(total_investment)  # 주식 자산 - 투자 금액

        # 🔥 4️⃣ 총 자산(total_asset) 계산
        total_asset = float(cash) + float(total_stock_value)  # 총 자산 = 현금 + 주식의 현재 시세

        # 🔥 5️⃣ JSON 형태로 변환 (최종 반환 값)
        result_dict = {
            "roi": round(total_roi, 2),  # 수익률
            "cash": round(cash, 2),  # 보유 현금
            "total_investment": round(total_investment, 2),  # 총 투자 금액 (보유 주식의 매수 원가 총합)
            "total_stock_value": round(total_stock_value, 2),  # 주식의 총 시세 (현재 시장 가치)
            "asset_difference": round(asset_difference, 2),  # 자산 차이 (주식 자산 - 투자 금액)
            "total_asset": round(total_asset, 2)  # 총 자산 (현금 + 주식의 총 시세)
        }

        return result_dict

    except Exception as e:
        logger.error("Error fetching ROI for User ID %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail=f"DB 조회 중 오류가 발생했습니다: {str(e)}")

    finally:
        db.close()


# 매도 가능 주식 수량을 조회하는 함수
async def get_stock_orders(session_id: str, symbol: str, stock_type: str, redis, db):
    logger.critical(f"get_stock_orders called with session_id: {session_id}, symbol: {symbol}, stock_type: {stock_type}")
    try:
        user_id = await get_user_from_session(session_id, redis)
        logger.critical(f"User ID retrieved from session: {user_id}")
        
        cursor = db.cursor(dictionary=True)
        logger.critical("Database cursor created successfully")

        # 매수 가능한 수량 계산을 위해 종가 조회
        if stock_type == 'buy':
            price_query = """
                SELECT 
                    close 
                FROM 
                    stock 
                WHERE 
                    symbol = %s 
                    AND is_deleted = FALSE 
                ORDER BY 
                    date DESC 
                LIMIT 1
            """
            cursor.execute(price_query, (symbol,))
            stock_price_result = cursor.fetchone()
            logger.critical(f"Stock price query result: {stock_price_result}")

            if stock_price_result is None:
                logger.critical(f"No stock price found for symbol: {symbol}")
                raise HTTPException(status_code=404, detail="주식 가격 정보를 찾을 수 없습니다.")

            stock_price = int(stock_price_result.get('close', 0))
            logger.critical(f"Stock price for {symbol}: {stock_price}")

            # 사용자 자산 조회
            asset_query = """
                SELECT 
                    cash 
                FROM 
                    user_data 
                WHERE 
                    user_id = %s 
                    AND is_deleted = FALSE 
                LIMIT 1
            """
            cursor.execute(asset_query, (user_id,))
            user_data_result = cursor.fetchone()
            logger.critical(f"User asset query result: {user_data_result}")

            if user_data_result is None:
                logger.critical(f"No user data found for user_id: {user_id}")
                raise HTTPException(status_code=404, detail="사용자 자산 정보를 찾을 수 없습니다.")

            user_cash = int(user_data_result.get('cash', 0))
            logger.critical(f"User cash for user_id {user_id}: {user_cash}")

            # 매수 가능 수량 = 사용자 자산 / 최신 주식 가격
            buyable_quantity = user_cash // stock_price if stock_price > 0 else 0
            logger.critical(f"Calculated buyable quantity: {buyable_quantity}")

            return {
                "symbol": symbol,
                "stock_price": stock_price,
                "user_cash": user_cash,
                "buyable_quantity": buyable_quantity
            }

        # 매도 가능 수량 계산
        elif stock_type == 'sell':
            stock_order_query = """
                SELECT 
                    c.id AS company_id,
                    CAST(SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END) AS SIGNED) AS total_bought,
                    CAST(SUM(CASE WHEN so.type = '매도' THEN so.quantity ELSE 0 END) AS SIGNED) AS total_sold
                FROM 
                    company c
                INNER JOIN 
                    stock_order so 
                ON 
                    so.company_id = c.id
                WHERE 
                    c.symbol = %s 
                    AND c.is_deleted = FALSE 
                    AND so.user_id = %s 
                    AND so.is_deleted = FALSE
                GROUP BY 
                    c.id
            """
            cursor.execute(stock_order_query, (symbol, user_id))
            result = cursor.fetchone()
            logger.critical(f"Stock order query result: {result}")

            # 결과가 없으면 매도 가능 수량을 0으로 설정
            if result is None:
                logger.critical(f"No order data found for user_id: {user_id} and symbol: {symbol}")
                return {
                    "symbol": symbol,
                    "company_id": None,
                    "total_bought": 0,
                    "total_sold": 0,
                    "sellable_quantity": 0
                }

            total_bought = int(result.get('total_bought', 0))
            total_sold = int(result.get('total_sold', 0))
            company_id = result.get('company_id')
            logger.critical(f"Total bought: {total_bought}, Total sold: {total_sold}, Company ID: {company_id}")

            sellable_quantity = max(total_bought - total_sold, 0)
            logger.critical(f"Calculated sellable quantity: {sellable_quantity}")

            return {
                "symbol": symbol,
                "company_id": company_id,
                "total_bought": total_bought,
                "total_sold": total_sold,
                "sellable_quantity": sellable_quantity
            }
    except Exception as e:
        logger.critical(f"Error occurred while processing get_stock_orders: {e}")
        raise HTTPException(status_code=500, detail="DB 조회 중 오류가 발생했습니다.")
    finally:
        if cursor:
            cursor.close()
            logger.critical("Database cursor closed")
        if db:
            db.close()
            logger.critical("Database connection closed") 
