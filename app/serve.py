"""
모델 서빙(Model Serving) - FastAPI 추론 서버.

S3에서 승격된 모델(model.pkl)과 전처리 단계의 스케일러(scaler.pkl)를 로드하여,
Wine Quality 분류 추론을 제공한다. 이 서버는 EKS 위 Deployment로 실행되며
Service를 통해 노출된다. (SageMaker 미사용 - 요구사항 8.3)

엔드포인트:
    GET  /health   : 헬스체크 (모델 로드 여부)
    POST /predict  : 단건 추론

환경 변수:
    S3_BUCKET  : 모델/스케일러가 저장된 버킷 (필수)
    S3_PREFIX  : 접두사. 기본값 "wine-quality"
    MODEL_DATE : 로드할 모델 파티션 일자(YYYY-MM-DD). 미지정 시 오늘 날짜.
"""

import os
import pickle
from datetime import date

import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# 전처리 단계와 동일한 피처 순서 (wine_type 제외한 11개가 표준화 대상)
NUMERIC_COLS = [
    "fixed acidity",
    "volatile acidity",
    "citric acid",
    "residual sugar",
    "chlorides",
    "free sulfur dioxide",
    "total sulfur dioxide",
    "density",
    "pH",
    "sulphates",
    "alcohol",
]
FEATURE_ORDER = NUMERIC_COLS + ["wine_type"]

app = FastAPI(title="Wine Quality Inference", version="1.0.0")

# 프로세스 시작 시 1회 로드되는 전역 아티팩트
_model = None
_scaler = None


class WineFeatures(BaseModel):
    """추론 입력. wine_type은 'red' 또는 'white'."""

    fixed_acidity: float
    volatile_acidity: float
    citric_acid: float
    residual_sugar: float
    chlorides: float
    free_sulfur_dioxide: float
    total_sulfur_dioxide: float
    density: float
    pH: float
    sulphates: float
    alcohol: float
    wine_type: str = "red"


def _load_artifacts() -> None:
    """S3에서 model.pkl, scaler.pkl 을 로드한다."""
    global _model, _scaler

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET 환경 변수가 설정되지 않았습니다.")
    prefix = os.environ.get("S3_PREFIX", "wine-quality")
    model_date = os.environ.get("MODEL_DATE") or date.today().isoformat()

    s3 = boto3.client("s3")
    model_key = f"{prefix}/models/dt={model_date}/model.pkl"
    scaler_key = f"{prefix}/processed/dt={model_date}/scaler.pkl"

    print(f"[serve] loading model  s3://{bucket}/{model_key}")
    _model = pickle.loads(s3.get_object(Bucket=bucket, Key=model_key)["Body"].read())
    print(f"[serve] loading scaler s3://{bucket}/{scaler_key}")
    _scaler = pickle.loads(s3.get_object(Bucket=bucket, Key=scaler_key)["Body"].read())
    print("[serve] artifacts loaded")


@app.on_event("startup")
def startup() -> None:
    _load_artifacts()


@app.get("/health")
def health() -> dict:
    ready = _model is not None and _scaler is not None
    return {"status": "ok" if ready else "not_ready", "model_loaded": ready}


@app.post("/predict")
def predict(features: WineFeatures) -> dict:
    if _model is None or _scaler is None:
        raise HTTPException(status_code=503, detail="모델이 아직 로드되지 않았습니다.")

    # 전처리와 동일한 변환: 피처 순서 정렬 -> wine_type 인코딩 -> 수치형 표준화
    row = {
        "fixed acidity": features.fixed_acidity,
        "volatile acidity": features.volatile_acidity,
        "citric acid": features.citric_acid,
        "residual sugar": features.residual_sugar,
        "chlorides": features.chlorides,
        "free sulfur dioxide": features.free_sulfur_dioxide,
        "total sulfur dioxide": features.total_sulfur_dioxide,
        "density": features.density,
        "pH": features.pH,
        "sulphates": features.sulphates,
        "alcohol": features.alcohol,
        "wine_type": 1 if features.wine_type.lower() == "white" else 0,
    }
    df = pd.DataFrame([row])[FEATURE_ORDER]
    df[NUMERIC_COLS] = _scaler.transform(df[NUMERIC_COLS])

    proba = float(_model.predict_proba(df)[0, 1])
    label = int(proba >= 0.5)
    return {
        "label": label,
        "quality_good_probability": round(proba, 4),
        "prediction": "good" if label == 1 else "not_good",
    }
