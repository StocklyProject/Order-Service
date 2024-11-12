FROM python:3.12

# 작업 디렉토리 설정
WORKDIR /Order-Service

# 시스템 패키지 업데이트 및 Git 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# requirements.txt 복사 후 필요한 패키지 설치
COPY ./src/requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip  # 캐시 삭제로 이미지 크기 최소화

# 서비스 소스 코드 복사
COPY ./src/ ./src

# 환경 변수로 Kafka 브로커 주소 설정 (디폴트는 로컬 Kafka 브로커)
ENV KAFKA_BROKER=kafka-broker.stockly.svc.cluster.local:9092

# 기본적으로 FastAPI 서버 실행, 포트 8001 사용
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8002"]