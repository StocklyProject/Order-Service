from datetime import datetime
import pytz
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import StreamingResponse
from .service import sse_event_generator, get_user_from_session
from fastapi import Depends, Request
from .database import get_redis
from .service import get_user_from_session, get_user_by_id, add_cash_to_user, reset_user_assets
from .database import get_db_connection
from .schemas import DepositRequest

KST = pytz.timezone('Asia/Seoul')

router = APIRouter(
    prefix="/api/v1/invests",
    tags=["invests"],
)

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
        return {"message": "충전이 완료되었습니다.", "user_id": user_id, "new_balance": user["cash"]}
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

# 매수
@router.post("/buy")
async def buy_stock(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)

    return {"user_id": user_id}

# 매도
@router.post("/sell")
async def sell_stock(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)

    return {"user_id": user_id}