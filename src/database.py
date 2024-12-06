import mysql.connector
import os
import redis.asyncio as aioredis
from logger import logger

async def get_redis():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        logger.error("REDIS_URL 환경 변수가 설정되지 않았습니다.")
        raise ValueError("REDIS_URL 환경 변수를 설정하세요.")
    
    try:
        logger.critical(f"Redis 연결 시도: {redis_url}")
        redis = await aioredis.from_url(redis_url)
        logger.critical("Redis 연결 성공")
        return redis
    except Exception as e:
        logger.error(f"Redis 연결 실패: {str(e)}")
        raise


def get_db_connection():
    host = os.environ.get("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    return connection