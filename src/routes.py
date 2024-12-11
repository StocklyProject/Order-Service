from datetime import datetime
import pytz
from fastapi import APIRouter, Request, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from .service import get_user_by_id, add_cash_to_user, reset_user_assets, fetch_latest_data_for_symbol, sse_event_generator, get_user_from_session, get_stocks, get_latest_roi_from_session, get_stock_orders
from .database import get_db_connection, get_redis
from .schemas import DepositRequest, OrderRequest
from .logger import logger
from .consumer import async_kafka_consumer
import json
from decimal import Decimal
import asyncio
import mysql.connector

KST = pytz.timezone('Asia/Seoul')

router = APIRouter(
    prefix="/api/v1/invests",
    tags=["invests"],
)

# 실시간 호가 조회
@router.get("/orderBook/{symbol}")
async def get_order_book_sse(symbol: str):
    topic = "real_time_asking_prices"
    now_kst = datetime.now()
    group_id = f"sse_consumer_group_{symbol}_{now_kst.strftime('%Y%m%d%H%M%S%f')}"
    return StreamingResponse(sse_event_generator(topic, group_id, symbol), media_type="text/event-stream")


# 자산 충전하기
@router.post("/deposit")
async def deposit_money(request: Request, body: DepositRequest, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    logger.critical("Received session ID: %s", session_id)
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id, redis)
    db = get_db_connection()
    try:
        user = get_user_by_id(user_id, db)
        add_cash_to_user(user_id, body.amount, db)
        return {"message": "충전이 완료되었습니다.", "user_id": user_id}
    finally:
        db.close()


# 자산 초기화
@router.post("/reset")
async def reset_assets(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    logger.critical("Received session ID: %s", session_id)
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    
    user_id = await get_user_from_session(session_id, redis)
    db = get_db_connection()
    
    try:
        reset_user_assets(user_id, db)
        return {"message": "자산이 초기화되었습니다.", "user_id": user_id}
    finally:
        db.close()


@router.post("/order")
async def process_order(
    request: Request,
    body: OrderRequest,
    redis=Depends(get_redis),
    db=Depends(get_db_connection),
):
    logger.info("Received order request: %s", body.dict())

    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 유효하지 않습니다.")

    # 사용자 ID 가져오기
    user_id = await get_user_from_session(session_id, redis)

    cursor = db.cursor(dictionary=True, buffered=True)  # 🔥 buffered=True 추가

    try:
        db.autocommit = False

        # 회사 정보 조회
        cursor.execute("SELECT id FROM company WHERE symbol = %s LIMIT 1", (body.symbol,))  # 🔥 LIMIT 1 추가
        company_data = cursor.fetchone()
        cursor.nextset()  # 🔥 추가된 nextset()으로 미소진 결과 집합 소진
        if not company_data:
            raise HTTPException(status_code=404, detail=f"심볼 '{body.symbol}'에 해당하는 회사가 없습니다.")
        company_id = company_data["id"]

        # 사용자 자산 정보 조회
        cursor.execute("SELECT cash FROM user_data WHERE user_id = %s LIMIT 1", (user_id,))  # 🔥 LIMIT 1 추가
        user_data = cursor.fetchone()
        cursor.nextset()
        if not user_data:
            raise HTTPException(status_code=404, detail="사용자 정보를 찾을 수 없습니다.")
        user_cash = user_data["cash"]

        # Kafka에서 실시간 데이터 가져오기
        latest_data = await fetch_latest_data_for_symbol(body.symbol)
        if not latest_data:
            raise HTTPException(status_code=404, detail=f"심볼 '{body.symbol}'에 대한 실시간 데이터가 없습니다.")

        # 호가 데이터 처리
        ask_prices = [
            {"price": float(latest_data[f"sell_price_{i}"]), "volume": int(latest_data[f"sell_volume_{i}"])}
            for i in range(3, 11) if f"sell_price_{i}" in latest_data and f"sell_volume_{i}" in latest_data
        ]
        bid_prices = [
            {"price": float(latest_data[f"buy_price_{i}"]), "volume": int(latest_data[f"buy_volume_{i}"])}
            for i in range(1, 9) if f"buy_price_{i}" in latest_data and f"buy_volume_{i}" in latest_data
        ]

        execution_price = None
        execution_quantity = None
        total_price = 0
        order_status = "미체결"

        # 매수/매도 처리
        if body.order_type == "매수":
            if body.price_type == "시장가":
                if not ask_prices:
                    raise HTTPException(status_code=400, detail="시장가 매수 주문에 유효한 매도 호가가 없습니다.")
                execution_price = ask_prices[0]["price"]
                execution_quantity = min(body.quantity, ask_prices[0]["volume"])
            elif body.price_type == "지정가":
                valid_asks = [ask for ask in ask_prices if ask["price"] <= body.price]
                if not valid_asks:
                    raise HTTPException(status_code=400, detail="지정가 매수 주문에 유효한 매도 호가가 없습니다.")
                execution_price = valid_asks[0]["price"]
                execution_quantity = min(body.quantity, valid_asks[0]["volume"])

            if execution_price and execution_quantity:
                total_price = execution_price * execution_quantity
                if total_price > user_cash:
                    raise HTTPException(status_code=400, detail="잔액이 부족하여 주문을 처리할 수 없습니다.")
                user_cash -= total_price
                order_status = "체결"

        elif body.order_type == "매도":
            cursor.execute(
                """
                SELECT 
                    IFNULL(SUM(CASE WHEN type = '매수' THEN quantity ELSE 0 END), 0) -
                    IFNULL(SUM(CASE WHEN type = '매도' THEN quantity ELSE 0 END), 0) AS total_quantity
                FROM stock_order
                WHERE user_id = %s AND company_id = %s AND is_deleted = 0
                """,
                (user_id, company_id),
            )
            user_stock_quantity = cursor.fetchone()["total_quantity"]
            cursor.nextset()

            if user_stock_quantity < body.quantity:
                raise HTTPException(status_code=400, detail="보유 주식 수량이 부족하여 매도할 수 없습니다.")

            if body.price_type == "시장가":
                if not bid_prices:
                    raise HTTPException(status_code=400, detail="시장가 매도 주문에 유효한 매수 호가가 없습니다.")
                execution_price = bid_prices[0]["price"]
                execution_quantity = min(body.quantity, bid_prices[0]["volume"])
            elif body.price_type == "지정가":
                valid_bids = [bid for bid in bid_prices if bid["price"] >= body.price]
                if not valid_bids:
                    raise HTTPException(status_code=400, detail="지정가 매도 주문에 유효한 매수 호가가 없습니다.")
                execution_price = valid_bids[0]["price"]
                execution_quantity = min(body.quantity, valid_bids[0]["volume"])

            if execution_price and execution_quantity:
                total_price = execution_price * execution_quantity
                user_cash += total_price
                order_status = "체결"

        # 주문 기록 저장
        cursor.execute(
            """
            INSERT INTO stock_order (user_id, company_id, type, price, quantity, total_price, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                company_id,
                body.order_type,
                execution_price,
                execution_quantity,
                total_price,
                order_status,
            ),
        )

        db.commit()

    except HTTPException as http_err:
        db.rollback()
        logger.error("HTTP error during order processing: %s", str(http_err.detail))
        raise http_err
    except Exception as e:
        db.rollback()
        logger.error("Error during order processing: %s", str(e))
        raise HTTPException(status_code=500, detail="주문 처리 중 오류가 발생했습니다.")
    finally:
        cursor.close()

    return {
        "status": order_status,
        "execution_price": execution_price,
        "execution_quantity": execution_quantity,
        "total_price": total_price,
    }


# 전체 주문 목록 조회
@router.get('/stocks')
async def get_all_stocks(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id, redis)
    db = get_db_connection()
    try:
        stocks = get_stocks(user_id, db)
        return {"message": "전체 주문 목록을 조회했습니다.", "data": stocks}
    finally:
        db.close()

@router.get("/roi/daily")
async def get_user_daily_roi(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id, redis)
    db = get_db_connection()
    try:
        cursor = db.cursor()
        cursor.execute("SELECT total_roi, created_at FROM user_data WHERE id = %s", (user_id,))
        rows = cursor.fetchall()  # 여러 행 가져오기

        # 데이터를 딕셔너리로 변환
        total_roi = [{"roi": row[0], "date": row[1].isoformat()} for row in rows]

        return {"message": "일일 수익률을 조회했습니다.", "total_roi": total_roi}
    finally:
        db.close()


@router.get('/roi/realtime/total')
async def get_realtime_total_roi(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    logger.critical("Session ID: %s", session_id)

    try:
        user_id = await get_user_from_session(session_id, redis)
    except Exception as e:
        logger.error("Failed to get user from session: %s", e)
        raise HTTPException(status_code=401, detail="사용자를 식별할 수 없습니다.")

    logger.critical("User ID: %s", user_id)

    topic = "real_time_stock_prices"
    group_id = f"real_time_total_roi_{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)
    logger.critical("Kafka Consumer initialized for topic: %s, group_id: %s", topic, group_id)

    async def event_stream():
        db = None
        cursor = None
        current_prices = {}

        try:
            db = get_db_connection()
            cursor = db.cursor(dictionary=True, buffered=True)  # 🔥 buffered=True 추가

            try:
                cursor.execute("SELECT cash FROM user_data WHERE user_id = %s LIMIT 1", (user_id,))  # 🔥 LIMIT 1 추가
                user_data = cursor.fetchone()
                cursor.nextset()  # 🔥 추가된 nextset()으로 미소진 결과 집합 소진
            except mysql.connector.errors.InternalError as e:
                logger.error("Failed to fetch user cash data: %s", e)
                raise e

            if not user_data:
                logger.warning("No user data found for User ID: %s", user_id)
                yield f"data: {json.dumps({'roi': 0, 'cash': 0, 'total_investment': 0, 'total_stock_value': 0, 'asset_difference': 0})}\n\n"
                return

            cash = float(user_data.get("cash", 0))

            try:
                cursor.execute("""
                    SELECT company.symbol, 
                        SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END) - 
                        SUM(CASE WHEN so.type = '매도' THEN so.quantity ELSE 0 END) AS total_quantity,
                        SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) / NULLIF(SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END), 0) AS average_buy_price
                    FROM stock_order so
                    INNER JOIN company ON so.company_id = company.id
                    WHERE so.user_id = %s AND so.is_deleted = FALSE
                    GROUP BY company.symbol
                """, (user_id,))
                holdings = cursor.fetchall()
            except mysql.connector.errors.InternalError as e:
                logger.error("Failed to fetch holdings for User ID %s: %s", user_id, e)
                raise e

            async for msg in consumer:
                try:
                    data = msg.value
                    symbol = data.get("symbol")
                    close_price = float(data.get("close", 0))

                    if symbol and close_price:
                        current_prices[symbol] = close_price

                    total_investment = sum(
                        float(holding['total_quantity']) * float(holding['average_buy_price'])
                        for holding in holdings
                    )

                    total_stock_value = sum(
                        float(current_prices.get(holding['symbol'], 0)) * float(holding['total_quantity'])
                        for holding in holdings
                    )

                    portfolio_roi = ((total_stock_value - total_investment) / total_investment * 100) if total_investment > 0 else 0
                    asset_difference = total_stock_value - total_investment

                    yield f"data: {json.dumps({'roi': round(portfolio_roi, 2), 'cash': round(cash, 2), 'total_investment': round(total_investment, 2), 'total_stock_value': round(total_stock_value, 2), 'asset_difference': round(asset_difference, 2), 'total_asset': round(cash + total_stock_value, 2)})}\n\n"

                except Exception as e:
                    logger.error("Error while processing Kafka message: %s", e)
                    continue

        except asyncio.CancelledError:
            logger.warning("SSE connection cancelled by client")
        except Exception as e:
            logger.error("Unexpected error in event_stream: %s", e)
        finally:
            try:
                if consumer:
                    await consumer.stop()
                if cursor:
                    cursor.close()  # 🔥 커서 닫기 추가
                if db:
                    db.close()
            except Exception as e:
                logger.error("Error while closing resources: %s", e)

    response = StreamingResponse(event_stream(), media_type="text/event-stream")
    return response

def decimal_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

async def event_stream(user_id, consumer):
    db = None
    cursor = None
    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT company.symbol, company.name,
                   SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END) -
                   SUM(CASE WHEN so.type = '매도' THEN so.quantity ELSE 0 END) AS total_quantity,
                   SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) AS total_investment
            FROM stock_order so
            INNER JOIN company ON so.company_id = company.id
            WHERE so.user_id = %s AND so.is_deleted = FALSE
            GROUP BY company.symbol, company.name
        """, (user_id,))
        holdings = cursor.fetchall()

        holdings_dict = {holding["symbol"]: holding for holding in holdings}

        cursor.execute("SELECT cash FROM user_data WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        cash = user_data["cash"] if user_data else 0

        last_prices = {}

        async for msg in consumer:
            data = msg.value
            symbol = data.get("symbol")
            current_price = data.get("close")

            if symbol and current_price:
                if last_prices.get(symbol) == current_price:
                    continue  # 🔥 중복 가격 필터링
                last_prices[symbol] = current_price

                holding = holdings_dict.get(symbol)
                if not holding:
                    continue

                name = holding["name"]
                quantity = float(holding["total_quantity"])
                total_investment_for_symbol = float(holding["total_investment"])
                total_stock_value = current_price * quantity
                avg_buy_price = total_investment_for_symbol / quantity if quantity > 0 else 0
                roi = ((current_price - avg_buy_price) / avg_buy_price) * 100 if avg_buy_price > 0 else 0
                price_difference = current_price - avg_buy_price

                yield f"data: {json.dumps({'symbol': symbol, 'name': name, 'volume': int(quantity), 'roi': round(roi, 2), 'cash': cash, 'total_investment': round(total_investment_for_symbol, 2), 'total_stock_value': round(total_stock_value, 2), 'price': f'{price_difference:.2f}'}, default=decimal_encoder)}\n\n"

    finally:
        if consumer:
            await consumer.stop()
        if cursor:
            cursor.close()
        if db:
            db.close()




@router.get('/roi/realtime')
async def get_realtime_roi(request: Request, redis=Depends(get_redis)):
    """
    실시간 주식 종목별 수익률 SSE 엔드포인트
    """
    session_id = request.cookies.get("session_id")
    if not session_id:
        logger.critical("Session ID not found.")
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    logger.critical("Session ID: %s", session_id)

    user_id = await get_user_from_session(session_id, redis)
    logger.critical("User ID: %s", user_id)

    # Kafka Consumer 초기화
    topic = "real_time_stock_prices"
    group_id = f"real_time_roi_{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)
    logger.critical("Kafka Consumer initialized for topic: %s, group_id: %s", topic, group_id)

    # StreamingResponse 생성
    response = StreamingResponse(
        event_stream(user_id, consumer), 
        media_type="text/event-stream"
    )

    # 요청한 출처(Origin)를 검사하여 허용된 출처만 Access-Control-Allow-Origin 헤더에 추가
    allowed_origins = ["https://stockly-frontend.vercel.app", "http://localhost:5173"]
    origin = request.headers.get("origin")
    
    if origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
    
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"

    return response

# 최신 보유 주식 수익률 조회
@router.get('/roi/latest')
async def get_latest_roi(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")

    user_id = await get_user_from_session(session_id, redis)  # Redis에서 사용자 ID 조회
    db = get_db_connection()

    try:
        cursor = db.cursor(dictionary=True)  # 딕셔너리 형식으로 결과 반환

        # 보유 중인 주식 정보 조회
        query = """
            SELECT
                co.symbol,
                co.name,
                SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END) - 
                SUM(CASE WHEN so.type = '매도' THEN so.quantity ELSE 0 END) AS volume,
                SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) / NULLIF(SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END), 0) AS purchase_price,
                s.close AS current_price,
                s.close - (SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) / NULLIF(SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END), 0)) AS price_difference,
                ROUND(((s.close - (SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) / NULLIF(SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END), 0))) / (SUM(CASE WHEN so.type = '매수' THEN so.quantity * so.price ELSE 0 END) / NULLIF(SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END), 0))) * 100, 2) AS roi,
                s.close * (SUM(CASE WHEN so.type = '매수' THEN so.quantity ELSE 0 END) - SUM(CASE WHEN so.type = '매도' THEN so.quantity ELSE 0 END)) AS total_stock_prices
            FROM stock_order AS so
            INNER JOIN company AS co ON so.company_id = co.id
            INNER JOIN stock AS s ON co.symbol = s.symbol
            WHERE so.user_id = %s AND so.is_deleted = FALSE
            AND s.date = (SELECT MAX(date) FROM stock WHERE stock.symbol = co.symbol)
            GROUP BY co.symbol, co.name, s.close
        """
        cursor.execute(query, (user_id,))
        results = cursor.fetchall()

        # 데이터가 없을 경우
        if not results:
            return {"message": "보유 중인 주식이 없습니다.", "data": []}

        # 데이터 반환
        return {"message": "종목별 최신 수익률을 조회했습니다.", "data": results}

    finally:
        db.close()


# 최신 종합 주식율 조회
@router.get('/roi/total/latest')
async def get_latest_roi(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    db = get_db_connection()
    latest_roi = await get_latest_roi_from_session(session_id, redis, db)
    return {"message": "최식 종합 주식율 조회.", "data": latest_roi}


# 매도/매수 가능 수량 조회
@router.get('/info')
async def get_stock_info(request: Request, symbol: str = Query, type: str = Query, redis=Depends(get_redis)):
    logger.critical(f"Request received for get_stock_info with symbol: {symbol}, type: {type}")
    if not symbol or not type:
        logger.critical("Missing required parameters: symbol or type")
        raise HTTPException(status_code=400, detail="symbol과 type이 필요합니다.")

    if type not in ['buy', 'sell']:
        logger.critical(f"Invalid type value: {type}")
        raise HTTPException(status_code=400, detail="잘못된 type 값입니다.")

    session_id = request.cookies.get("session_id")
    if not session_id:
        logger.critical("Missing session ID in request cookies")
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")

    try:
        db = get_db_connection()
        logger.critical("Database connection established successfully")
        stock_info = await get_stock_orders(session_id, symbol, type, redis, db)
        logger.critical(f"Successfully retrieved stock info: {stock_info}")
        return {"message": "주식 정보 조회", "data": stock_info}
    except Exception as e:
        logger.critical(f"Unexpected error in get_stock_info: {e}")
        raise HTTPException(status_code=500, detail="서버 내부 오류가 발생했습니다.")