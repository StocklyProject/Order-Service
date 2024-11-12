import mysql.connector
import os
import redis.asyncio as aioredis

async def get_redis():
    redis_url = os.getenv("REDIS_URL")
    redis = await aioredis.from_url(redis_url)
    return redis


def get_db_connection():
    host = os.environ.get("MYSQL_HOST"),
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