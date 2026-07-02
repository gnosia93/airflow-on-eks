"""
모델 학습(Model Training) 스크립트.

전처리 단계가 S3에 저장한 train/test Parquet을 읽어 분류 모델을 학습하고,
테스트셋으로 평가한 뒤 학습된 모델(model.pkl)과 평가 지표(metrics.json)를 S3에 저장한다.
모든 연산은 EKS Pod 안에서 실행된다. (SageMaker 미사용 - 요구사항 8)

환경 변수:
    S3_BUCKET     : 데이터/모델이 저장될 S3 버킷 이름 (필수)
    S3_PREFIX     : 버킷 내 접두사(prefix). 기본값 "wine-quality"
    DATA_DATE     : 읽어올 processed 데이터의 파티션 일자(YYYY-MM-DD). 미지정 시 오늘 날짜.
    N_ESTIMATORS  : 트리 개수. 기본값 200
    MAX_DEPTH     : 트리 최대 깊이. 기본값 8
    LEARNING_RATE : 부스팅 학습률. 기본값 0.1
    RANDOM_SEED   : 재현성을 위한 시드. 기본값 42
"""

import io
import json
import os
import pickle
import sys
from datetime import date

import boto3
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

TARGET_COL = "label"


def _read_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    print(f"[train] reading s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def main() -> int:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("[train] ERROR: S3_BUCKET 환경 변수가 설정되지 않았습니다.", file=sys.stderr)
        return 1

    prefix = os.environ.get("S3_PREFIX", "wine-quality")
    run_date = os.environ.get("DATA_DATE") or date.today().isoformat()

    # 하이퍼파라미터 (튜닝 단계에서 이 값들을 탐색한다)
    n_estimators = int(os.environ.get("N_ESTIMATORS", "200"))
    max_depth = int(os.environ.get("MAX_DEPTH", "8"))
    learning_rate = float(os.environ.get("LEARNING_RATE", "0.1"))
    seed = int(os.environ.get("RANDOM_SEED", "42"))

    s3 = boto3.client("s3")
    base = f"{prefix}/processed/dt={run_date}"

    # 1) 데이터 로드
    train_df = _read_parquet_from_s3(s3, bucket, f"{base}/train.parquet")
    test_df = _read_parquet_from_s3(s3, bucket, f"{base}/test.parquet")

    x_train = train_df.drop(columns=[TARGET_COL])
    y_train = train_df[TARGET_COL]
    x_test = test_df.drop(columns=[TARGET_COL])
    y_test = test_df[TARGET_COL]
    print(f"[train] train={len(x_train)} test={len(x_test)} features={x_train.shape[1]}")

    # 2) 모델 학습
    print(
        f"[train] fitting GradientBoosting "
        f"(n_estimators={n_estimators}, max_depth={max_depth}, lr={learning_rate})"
    )
    model = GradientBoostingClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        random_state=seed,
    )
    model.fit(x_train, y_train)

    # 3) 평가
    pred = model.predict(x_test)
    proba = model.predict_proba(x_test)[:, 1]
    metrics = {
        "accuracy": round(float(accuracy_score(y_test, pred)), 4),
        "precision": round(float(precision_score(y_test, pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_test, pred, zero_division=0)), 4),
        "roc_auc": round(float(roc_auc_score(y_test, proba)), 4),
        "hyperparameters": {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
        },
    }
    print(f"[train] metrics: {json.dumps(metrics, ensure_ascii=False)}")

    # 4) 모델 및 지표 저장
    model_base = f"{prefix}/models/dt={run_date}"
    s3.put_object(
        Bucket=bucket, Key=f"{model_base}/model.pkl", Body=pickle.dumps(model)
    )
    s3.put_object(
        Bucket=bucket,
        Key=f"{model_base}/metrics.json",
        Body=json.dumps(metrics, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    print(f"[train] saved model -> s3://{bucket}/{model_base}/model.pkl")
    print(f"[train] saved metrics -> s3://{bucket}/{model_base}/metrics.json")
    print(f"[train] DONE. roc_auc={metrics['roc_auc']} f1={metrics['f1']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
