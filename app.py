import asyncio
import io
import json
import time
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

import psutil
from fastapi import FastAPI, File, HTTPException
from faster_whisper import WhisperModel  # 시스템에 ffmpeg 필수로 설치

# 모델 정의
model = WhisperModel("base", compute_type="int8", device="cpu")

# STT 처리와, API 처리를 위한 멀티 쓰레딩
executor = ThreadPoolExecutor(max_workers=2)

# 처리 큐 설정
QUEUE_MAXSIZE = 10
task_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)


def transcribe_audio(audio_bytes: bytes) -> str:
    """
    음성 데이터를 텍스트로 변환하는 함수

    :param audio_bytes: 음성 데이터 (byte 형태)

    :return: 텍스트화된 음성 데이터
    """
    audio_stream = io.BytesIO(audio_bytes)
    segments, _ = model.transcribe(audio_stream, language="ko")
    return "".join([seg.text for seg in segments])


async def worker():
    """큐에 들어있는 작업을 처리하는 함수"""
    while True:
        audio_bytes, future = await task_queue.get()
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            executor, transcribe_audio, audio_bytes
        )
        future.set_result(result)
        task_queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    for _ in range(2):
        asyncio.create_task(worker())

    yield
    # 서버 종료 시 실행할 코드


# 서버 가동
app = FastAPI(lifespan=lifespan)


@app.post("/stt")
async def receive_stt(
    json_bytes: bytes = File(...),
    audio_bytes: bytes = File(...)
) -> dict:
    """
    json 파일과 음성 파일을 받아 STT 후, 통합하여 반환하는 API

    :param json_bytes: 고객의 정보가 담긴 json 데이터 (byte 형태)
    :param audio_bytes: ARS 음성 데이터 (byte 형태)

    :return: 고객 문의 데이터
    """
    start_time = time.time()

    loop = asyncio.get_event_loop()
    future = loop.create_future()

    try:
        # 최대 5초 동안 큐에 들어갈 수 있을지 기다림
        await asyncio.wait_for(
            task_queue.put((audio_bytes, future)), timeout=5.0
        )
    except asyncio.TimeoutError:
        # 5초가 지나면 재시도 안내 반환
        raise HTTPException(
            status_code=503,
            detail={
                "message": "요청이 많아 처리 지연 중입니다. 잠시 후 다시 시도해주세요.",
                "queue_status": f"{task_queue.qsize()}/{QUEUE_MAXSIZE}"
            },
            headers={"Retry-After": "10"}
        )

    consulting_content = await future
    json_data = json.loads(json_bytes.decode("utf-8"))

    end_time = time.time()

    return {
        "message": "파일 수신 성공",
        "duration": end_time - start_time,
        "source_id": json_data[0]["source_id"],
        "category": json_data[0]["consulting_category"],
        "consultation_content": consulting_content.strip(),
        "queue_status": f"{task_queue.qsize()}/{QUEUE_MAXSIZE}"
    }


@app.get("/status")
async def get_server_status() -> dict:
    """
    현재 서버의 상태를 확인하는 함수

    :return: 큐, cpu, 메모리의 상태 데이터
    """
    memory = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)

    return {
        "queue_size": task_queue.qsize(),
        "queue_maxsize": QUEUE_MAXSIZE,
        "available_slots": QUEUE_MAXSIZE - task_queue.qsize(),
        "cpu_percent": cpu,
        "memory_percent": memory.percent,
        "memory_used_mb": round(memory.used / 1024**2, 2),
        "memory_total_mb": round(memory.total / 1024**2, 2)
    }