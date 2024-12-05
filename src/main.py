from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import asynccontextmanager
from . import routes as order_routes
from .database import get_db_connection
from datetime import datetime
from .service import update_daily_roi_for_all_users
from .logger import logger

# 수익률 업데이트 함수
def update_daily_roi():
    logger.critical(f"Starting daily ROI update at {datetime.now()}")
    db = get_db_connection()
    try:
        update_daily_roi_for_all_users(db)  # 주어진 함수 호출
        logger.critical(f"Daily ROI update completed at {datetime.now()}")
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 스케줄러 초기화
    scheduler = AsyncIOScheduler()
    scheduler.start()

    # 매일 오후 3시 10분에 실행되는 작업 추가
    # scheduler.add_job(
    #     update_daily_roi,
    #     CronTrigger(hour=15, minute=10, timezone="Asia/Seoul")  # 시간대 설정
    # )

    # 1분마다 실행되도록 작업 추가 (테스트용)
    scheduler.add_job(
        update_daily_roi,
        CronTrigger(minute="*", second=0, timezone="Asia/Seoul")  # 1분마다 실행
    )

    try:
        yield
    finally:
        scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

router = APIRouter(prefix="/api/v1")
app.include_router(order_routes.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def hello():
    return {"message": "order service consumer 메인페이지입니다"}