from datetime import datetime
import pytz
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import StreamingResponse
from .service import sse_event_generator, get_user_from_session
from fastapi import Depends, Request
from .database import get_redis

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
async def deposit_money(request: Request, amount: int, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)

    return {"user_id": user_id, "amount": amount}

# 자산 초기화하기 
@router.post("/reset")
async def reset_assets(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)

    return {"user_id": user_id}

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