"""
하이퍼파라미터 튜닝 - 단일 시도(trial) 학습 스크립트.

하나의 하이퍼파라미터 조합으로 모델을 학습/평가한 뒤, 후보(candidate) 모델과 지표를
튜닝 전용 경로(tuning/.../trials/{TRIAL_ID}/)에 저장한다. 여러 조합이 각각 독립된
EKS Pod에서 병렬로 실행되며, 이후 select_best.py가 최적 시도를 선택한다.

환경 변수:
    S3_BUCKET     : S3 버킷 이름 (필수)
    S3_PREFIX     : 접두사. 기본값 "wine-quality"
    DATA_DATE     : processed 데이터 파티션 일자. 미지정 시 오늘 날짜.
    TRIAL_ID      : 시도 식별자 (필수, 예: "trial-0")
    N_ESTIMATORS  : 트리 개수
    MAX_DEPTH     : 트리 최대 깊이
    LEARNING_RATE : 학습률
    RANDOM_SEED   : 시드. 기본값 42
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
from sklearn.metrics import f1_score, roc_auc_score

TARGET_COL = "label"


def _read_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def main() -> int:
    bucket = os.environ.get("S3_BUCKET")
    trial_id = os.environ.get("TRIAL_ID")
    if not bucket or not trial_id:
        print("[tune] ERROR: S3_BUCKET, TRIAL_ID 는 필수입니다.", file=sys.stderr)
        return 1

    prefix = os.environ.get("S3_PREFIX", "wine-quality")
    run_date = os.environ.get("DATA_DATE") or date.today().isoformat()
    n_estimators = int(os.environ.get("N_ESTIMATORS", "200"))
    max_depth = int(os.environ.get("MAX_DEPTH", "8"))
    learning_rate = float(os.environ.get("LEARNING_RATE", "0.1"))
    seed = int(os.environ.get("RANDOM_SEED", "42"))

    s3 = boto3.client("s3")
    base = f"{prefix}/processed/dt={run_date}"

    train_df = _read_parquet_from_s3(s3, bucket, f"{base}/train.parquet")
    test_df = _read_parquet_from_s3(s3, bucket, f"{base}/test.parquet")
    x_train, y_train = train_df.drop(columns=[TARGET_COL]), train_df[TARGET_COL]
    x_test, y_test = test_df.drop(columns=[TARGET_COL]), test_df[TARGET_COL]

    print(
        f"[tune] {trial_id}: fitting "
        f"(n_estimators={n_estimators}, max_depth={max_depth}, lr={learning_rate})"
    )
    model = GradientBoostingClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        random_state=seed,
    )
    model.fit(x_train, y_train)

    proba = model.predict_proba(x_test)[:, 1]
    pred = model.predict(x_test)
    metrics = {
        "trial_id": trial_id,
        "roc_auc": round(float(roc_auc_score(y_test, proba)), 4),
        "f1": round(float(f1_score(y_test, pred, zero_division=0)), 4),
        "hyperparameters": {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
        },
    }
    print(f"[tune] {trial_id} metrics: {json.dumps(metrics, ensure_ascii=False)}")

    # 후보 모델 + 지표를 시도별 경로에 저장
    trial_base = f"{prefix}/tuning/dt={run_date}/trials/{trial_id}"
    s3.put_object(Bucket=bucket, Key=f"{trial_base}/model.pkl", Body=pickle.dumps(model))
    s3.put_object(
        Bucket=bucket,
        Key=f"{trial_base}/metrics.json",
        Body=json.dumps(metrics, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    print(f"[tune] {trial_id} DONE. saved -> s3://{bucket}/{trial_base}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
