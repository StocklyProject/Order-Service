from datetime import datetime
import pytz
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import StreamingResponse
from .service import get_user_from_session, get_user_by_id, add_cash_to_user, reset_user_assets, fetch_latest_data_for_symbol, sse_event_generator, get_user_from_session
from .database import get_db_connection, get_redis
from .schemas import DepositRequest, OrderRequest
from .logger import logger

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
@router.post("/order")
async def process_order(
    request: Request,
    body: OrderRequest,
    redis=Depends(get_redis),
    db=Depends(get_db_connection),
):
    logger.critical("Received order request: %s", body.dict())

    session_id = request.cookies.get("session_id")
    if not session_id:
        logger.critical("No session ID found in cookies.")
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")

    # 사용자 ID 가져오기
    user_id = await get_user_from_session(session_id, redis)
    logger.critical("User ID retrieved: %s", user_id)

    cursor = db.cursor(dictionary=True)

    # 트랜잭션 시작
    try:
        db.autocommit = False

        # company_id 조회
        logger.critical("Fetching company_id for symbol: %s", body.symbol)
        cursor.execute("SELECT id FROM company WHERE symbol = %s", (body.symbol,))
        company_data = cursor.fetchone()
        if not company_data:
            logger.error("Company not found for symbol: %s", body.symbol)
            raise HTTPException(status_code=404, detail=f"{body.symbol}에 대한 회사 정보가 없습니다.")
        company_id = company_data['id']
        logger.critical("Company ID retrieved: %s", company_id)

        # 사용자 자산 정보 조회
        cursor.execute("SELECT cash, total_asset, total_stock FROM user_data WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            logger.error("User not found with ID: %s", user_id)
            raise HTTPException(status_code=404, detail="사용자 정보를 찾을 수 없습니다.")
        user_cash = user_data['cash']
        user_total_asset = user_data['total_asset']
        user_total_stock = user_data['total_stock']
        logger.critical("User data retrieved: %s", user_data)

        # Kafka에서 특정 심볼의 최신 데이터 가져오기
        logger.critical("Fetching latest data for symbol: %s", body.symbol)
        latest_data = await fetch_latest_data_for_symbol(body.symbol)
        if not latest_data:
            logger.error("No real-time data found for symbol: %s", body.symbol)
            raise HTTPException(status_code=404, detail=f"{body.symbol}에 대한 실시간 데이터가 없습니다.")
        logger.critical("Latest data retrieved: %s", latest_data)

        # 실시간 호가 데이터 처리
        ask_prices = [
            {"price": float(latest_data[f"sell_price_{i}"]), "volume": int(latest_data[f"sell_volume_{i}"])}
            for i in range(3, 11)
            if f"sell_price_{i}" in latest_data and f"sell_volume_{i}" in latest_data
        ]
        bid_prices = [
            {"price": float(latest_data[f"buy_price_{i}"]), "volume": int(latest_data[f"buy_volume_{i}"])}
            for i in range(1, 9)
            if f"buy_price_{i}" in latest_data and f"buy_volume_{i}" in latest_data
        ]
        logger.critical("Processed ask prices: %s", ask_prices)
        logger.critical("Processed bid prices: %s", bid_prices)

        # 체결된 주문 데이터
        execution_price = None
        execution_quantity = None

        if body.order_type == "매수":
            logger.critical("Processing buy order.")

            if body.price_type == "시장가":
                if not ask_prices:
                    logger.warning("No ask prices available for market order.")
                    raise HTTPException(status_code=400, detail="No ask prices available")
                execution_price = ask_prices[0]["price"]
                execution_quantity = min(body.quantity, ask_prices[0]["volume"])
            elif body.price_type == "지정가":
                valid_asks = [ask for ask in ask_prices if ask["price"] <= body.price]
                if not valid_asks:
                    logger.warning("No matching ask prices for limit order.")
                    raise HTTPException(status_code=400, detail="No matching ask prices")
                execution_price = valid_asks[0]["price"]
                execution_quantity = min(body.quantity, valid_asks[0]["volume"])

            # 총 매수 금액 계산
            total_price = execution_price * execution_quantity

            # 사용자 자금 확인
            if total_price > user_cash:
                logger.warning("User does not have enough cash for the order.")
                raise HTTPException(status_code=400, detail="잔액이 부족합니다.")

            # 사용자 데이터 업데이트
            cursor.execute("""
                UPDATE user_data
                SET cash = cash - %s,
                    total_asset = total_asset + %s,
                    total_stock = total_stock + %s
                WHERE id = %s
            """, (total_price, total_price, total_price, user_id))

        elif body.order_type == "매도":
            logger.critical("Processing sell order.")

            # 데이터베이스에서 사용자의 보유 주식 수량 계산 (매수량 - 매도량)
            logger.critical("Fetching user's net stock holdings for company_id: %s", company_id)
            cursor.execute("""
                SELECT 
                    IFNULL(SUM(CASE WHEN type = '매수' THEN quantity ELSE 0 END), 0) -
                    IFNULL(SUM(CASE WHEN type = '매도' THEN quantity ELSE 0 END), 0) AS total_quantity
                FROM stock_order
                WHERE user_id = %s AND company_id = %s AND is_deleted = 0
            """, (user_id, company_id))
            user_stock_data = cursor.fetchone()
            user_stock_quantity = user_stock_data['total_quantity']
            logger.critical("User's net stock holdings: %s units.", user_stock_quantity)

            # 보유 주식 수량 확인
            if body.quantity > user_stock_quantity:
                logger.warning("Sell quantity exceeds user's holdings.")
                raise HTTPException(status_code=400, detail="매도 수량이 보유 수량을 초과합니다.")

            if body.price_type == "시장가":
                if not bid_prices:
                    logger.warning("No bid prices available for market order.")
                    raise HTTPException(status_code=400, detail="No bid prices available")
                execution_price = bid_prices[0]["price"]
                execution_quantity = min(body.quantity, bid_prices[0]["volume"])
            elif body.price_type == "지정가":
                valid_bids = [bid for bid in bid_prices if bid["price"] >= body.price]
                if not valid_bids:
                    logger.warning("No matching bid prices for limit order.")
                    raise HTTPException(status_code=400, detail="No matching bid prices")
                execution_price = valid_bids[0]["price"]
                execution_quantity = min(body.quantity, valid_bids[0]["volume"])

            # 총 매도 금액 계산
            total_price = execution_price * execution_quantity

            # 사용자 데이터 업데이트
            cursor.execute("""
                UPDATE user_data
                SET cash = cash + %s,
                    total_asset = total_asset - %s,
                    total_stock = total_stock - %s
                WHERE id = %s
            """, (total_price, total_price, total_price, user_id))

        # 주문 정보 저장
        cursor.execute("""
            INSERT INTO stock_order (user_id, company_id, type, price, quantity, total_price, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            company_id,
            body.order_type,
            execution_price,
            execution_quantity,
            total_price,
            "체결 완료"
        ))

        # 트랜잭션 커밋
        db.commit()

    except Exception as e:
        db.rollback()
        logger.error("Error during order processing: %s", str(e))
        raise HTTPException(status_code=500, detail="주문 처리 중 오류가 발생했습니다.")
    finally:
        cursor.close()

    # 체결 결과 반환
    return {
        "status": "주문이 체결되었습니다.",
        "execution_price": execution_price,
        "execution_quantity": execution_quantity,
        "total_price": total_price
    }
