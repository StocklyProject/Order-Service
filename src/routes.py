from datetime import datetime
import pytz
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import StreamingResponse
from .service import get_user_from_session, get_user_by_id, add_cash_to_user, reset_user_assets, fetch_latest_data_for_symbol, sse_event_generator, get_user_from_session, get_stocks
from .database import get_db_connection, get_redis
from .schemas import DepositRequest, OrderRequest
from .logger import logger
from .consumer import async_kafka_consumer
import json

KST = pytz.timezone('Asia/Seoul')

router = APIRouter(
    prefix="/api/v1/invests",
    tags=["invests"],
)

# 실시간 호가 조회
@router.get("/orderBook/{symbol}/")
async def get_order_book_sse(symbol: str):
    topic = "real_time_asking_prices"
    now_kst = datetime.now()
    group_id = f"sse_consumer_group_{symbol}_{now_kst.strftime('%Y%m%d%H%M%S%f')}"
    return StreamingResponse(sse_event_generator(topic, group_id, symbol), media_type="text/event-stream")


# 자산 충전하기
@router.post("/deposit/")
async def deposit_money(request: Request, body: DepositRequest, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
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
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    
    user_id = await get_user_from_session(session_id, redis)
    db = get_db_connection()
    
    try:
        reset_user_assets(user_id, db)
        return {"message": "자산이 초기화되었습니다.", "user_id": user_id}
    finally:
        db.close()



# 매수/매도 주문 처리
@router.post("/order/")
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

    cursor = db.cursor(dictionary=True)

    try:
        db.autocommit = False

        # 회사 정보 조회
        cursor.execute("SELECT id FROM company WHERE symbol = %s", (body.symbol,))
        company_data = cursor.fetchone()
        if not company_data:
            raise HTTPException(status_code=404, detail=f"심볼 '{body.symbol}'에 해당하는 회사가 없습니다.")
        company_id = company_data["id"]

        # 사용자 자산 정보 조회
        cursor.execute("SELECT cash, total_stock FROM user_data WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            raise HTTPException(status_code=404, detail="사용자 정보를 찾을 수 없습니다.")
        user_cash = user_data["cash"]
        user_total_stock = user_data["total_stock"]

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

        # 요청한 값 저장을 위한 변수
        requested_price = body.price
        requested_quantity = body.quantity

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
                user_total_stock += total_price
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
                user_total_stock -= total_price
                order_status = "체결"

        # 체결 실패 시에도 요청 값 저장
        if not execution_price or not execution_quantity:
            execution_price = requested_price
            execution_quantity = requested_quantity
            total_price = execution_price * execution_quantity

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

        # 체결 완료 시 사용자 데이터 갱신
        if order_status == "체결":
            cursor.execute(
                """
                UPDATE user_data
                SET cash = %s, total_stock = %s, total_asset = %s
                WHERE id = %s
                """,
                (user_cash, user_total_stock, user_cash + user_total_stock, user_id),
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
@router.get('/stocks/')
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

@router.get("/roi/daily/")
async def get_user_daily_roi(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id, redis)
    db = get_db_connection()
    try:
        cursor = db.cursor()
        cursor.execute("SELECT total_roi, created_at FROM user_data WHERE id = %s", (user_id,))
        total_roi = cursor.fetchone()
        return {"message": "일일 수익률을 조회했습니다.", "total_roi": total_roi}
    finally:
        db.close()


@router.get('/roi/realtime/total/')
async def get_realtime_total_roi(request: Request, redis=Depends(get_redis)):
    """
    실시간 종합 주식 수익률 SSE 엔드포인트
    """
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id, redis)

    # Kafka Consumer 초기화
    topic = "real_time_stock_prices"
    group_id = f"real_time_total_roi_{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)

    async def event_stream():
        try:
            db = get_db_connection()
            cursor = db.cursor(dictionary=True)

            # 사용자 보유 주식 정보 가져오기
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

            if not holdings:
                yield json.dumps({"message": "No holdings found.", "roi": 0, "cash": 0, "total_investment": 0, "total_stock_value": 0, "asset_difference": 0})
                return

            # 사용자 보유 현금 가져오기
            cursor.execute("SELECT cash FROM user_data WHERE id = %s", (user_id,))
            user_data = cursor.fetchone()
            cash = user_data["cash"] if user_data else 0

            # Kafka 메시지 처리
            async for msg in consumer:
                data = msg.value
                current_prices = {item["symbol"]: item["close"] for item in data.get("stocks", [])}

                # ROI 계산
                total_investment = 0
                total_stock_value = 0
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
                        total_stock_value += current_price * quantity

                # 포트폴리오 수익률 계산
                if total_investment > 0:
                    portfolio_roi = weighted_sum / total_investment
                    portfolio_roi = round(portfolio_roi, 2)
                else:
                    portfolio_roi = 0

                # 총자산에서 총주식 가치 차이를 계산
                total_asset = cash + total_stock_value
                asset_difference = total_asset - total_stock_value

                # SSE 이벤트 전송
                yield f"data: {json.dumps({'roi': portfolio_roi, 'cash': cash, 'total_investment': total_investment, 'total_stock_value': total_stock_value, 'asset_difference': asset_difference})}\n\n"

        finally:
            await consumer.stop()
            cursor.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")



@router.get('/roi/realtime/')
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

    async def event_stream():
        try:
            db = get_db_connection()
            cursor = db.cursor(dictionary=True)

            # 사용자 보유 주식 정보 가져오기
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
            logger.critical("Holdings for User ID %s: %s", user_id, holdings)

            if not holdings:
                logger.critical("No holdings found for User ID: %s", user_id)
                yield json.dumps({"message": "No holdings found.", "symbols": []})
                return

            # 사용자 보유 현금 가져오기
            cursor.execute("SELECT cash FROM user_data WHERE id = %s", (user_id,))
            user_data = cursor.fetchone()
            cash = user_data["cash"] if user_data else 0
            logger.critical("User ID %s, Cash: %s", user_id, cash)

            # Kafka 메시지 처리
            async for msg in consumer:
                logger.critical("Kafka Message Received: %s", msg.value)
                data = msg.value
                current_prices = {item["symbol"]: item["close"] for item in data.get("stocks", [])}
                logger.critical("Current Prices: %s", current_prices)

                symbol_results = []
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
                        total_stock_value = current_price * quantity
                        logger.critical(
                            "Symbol: %s, Quantity: %s, Total Investment: %s, Current Price: %s, ROI: %s, Stock Value: %s",
                            symbol, quantity, total_investment_for_symbol, current_price, roi, total_stock_value
                        )
                        symbol_results.append({
                            "symbol": symbol,
                            "roi": roi,
                            "cash": cash,
                            "total_investment": total_investment_for_symbol,
                            "total_stock_value": total_stock_value,
                        })

                # SSE 이벤트 전송 (심볼별 데이터)
                yield f"data: {json.dumps({'symbols': symbol_results})}\n\n"

        finally:
            await consumer.stop()
            cursor.close()

    logger.critical("Starting SSE stream for User ID: %s", user_id)
    return StreamingResponse(event_stream(), media_type="text/event-stream")

