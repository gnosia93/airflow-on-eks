# Wine Quality 추론 서버 이미지 (FastAPI + uvicorn).
# EKS Deployment로 실행되며 S3에서 모델/스케일러를 로드한다.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app/serve.py /app/serve.py

EXPOSE 8000

# uvicorn으로 FastAPI 앱 기동
CMD ["uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8000"]
