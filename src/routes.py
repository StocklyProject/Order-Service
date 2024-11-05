from datetime import datetime
import pytz
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from .service import sse_event_generator

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