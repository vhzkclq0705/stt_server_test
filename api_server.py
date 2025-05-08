import boto3
import io
import json
import time
import uuid
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

import psutil
from kafka import KafkaProducer
from fastapi import FastAPI, File

# Kafka & S3 설정
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v:json.dumps(v).encode("utf-8")
)
s3 = boto3.client('s3')
bucket = 'wh04-voc-bucket'


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 서버 Lifecycle 관리 함수
    
    yield 이전 코드는 서버가 시작된 직후 실행,
    yield 이후 코드는 서버가 종료된 직후 실행
    """
    
    yield # 서버 종료 시 실행할 코드
    producer.close()
    s3.close()


# 서버 가동
app = FastAPI(lifespan=lifespan)


@app.post("/submit")
async def submit_data(
    json_file: bytes = File(...),
    audio_file: Optional[bytes] = File(None)
) -> dict:
    """
    json 파일과 음성 파일을 받아 STT 후, 통합하여 반환하는 API

    :param json_file: 고객의 정보가 담긴 json 데이터 (byte 형태)  
    :param audio_file: ARS 음성 데이터 (Optional[byte] 형태로, Nullable)

    :return: 고객 문의 데이터
    """
    start_time = time.time()

    data = json.loads(json_file.decode("utf-8"))
    json_data = data[0]
    
    # source_id 제거
    json_data.pop("source_id", None)
    
    # consulting_id, customer_id 생성
    consulting_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    
    json_data["consulting_id"] = consulting_id
    json_data["customer_id"] = customer_id
    
    timestamp = datetime.now().strftime("%Y%m%d")
    json_s3_key = f"sample/raw/api_raw/{timestamp}_{consulting_id}.json"
    
    if audio_file:
        json_data["source"] = "ARS"
        audio_s3_key = f"sample/raw/audio/{timestamp}_{consulting_id}.wav"
        
        # s3.upload_fileobj(io.BytesIO(audio_file), bucket, audio_s3_key)
        json_data["audio_s3_key"] = audio_s3_key
        producer.send("voc_audio_raw", json_data)
    else:
        producer.send("voc_text_ready", json_data)
    
    # s3.put_object(
    #     Bucket=bucket,
    #     Key=json_s3_key,
    #     Body=json.dumps(json_data, ensure_ascii=False).encode("utf-8")
    # )

    end_time = time.time()
    
    return {
        "message": "성공",
        "duration": end_time - start_time
    }


@app.get("/status")
async def get_server_status() -> dict:
    """
    현재 서버의 상태를 확인하는 함수

    :return: cpu, 메모리의 상태 데이터
    """
    memory = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)

    return {
        "cpu_percent": cpu,
        "memory_percent": memory.percent,
        "memory_used_mb": round(memory.used / 1024**2, 2),
        "memory_total_mb": round(memory.total / 1024**2, 2)
    }