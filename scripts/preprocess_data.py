"""
전처리(Preprocessing) 스크립트.

collect_data.py가 S3에 저장한 원천 데이터를 읽어 다음을 수행한다.
  1) 결측치/중복 제거
  2) 범주형 컬럼(wine_type) 인코딩
  3) 타깃 라벨 생성 (quality >= 7 이면 1: good, 아니면 0)
  4) 수치형 피처 표준화(StandardScaler)
  5) train/test 분할 후 S3에 Parquet으로 저장
  6) 스케일러(scaler.pkl)를 S3에 저장하여 서빙 단계에서 재사용

모든 연산은 EKS Pod 안에서 실행된다.

환경 변수:
    S3_BUCKET   : 데이터가 저장된 S3 버킷 이름 (필수)
    S3_PREFIX   : 버킷 내 접두사(prefix). 기본값 "wine-quality"
    DATA_DATE   : 읽어올 raw 데이터의 파티션 일자(YYYY-MM-DD). 미지정 시 오늘 날짜.
    TEST_SIZE   : 테스트셋 비율. 기본값 0.2
    RANDOM_SEED : 재현성을 위한 시드. 기본값 42
"""

import io
import os
import pickle
import sys
from datetime import date

import boto3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

TARGET_COL = "label"
QUALITY_THRESHOLD = 7


def _read_csv_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    print(f"[preprocess] reading s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def _write_parquet_to_s3(s3, df: pd.DataFrame, bucket: str, key: str) -> str:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    uri = f"s3://{bucket}/{key}"
    print(f"[preprocess] wrote {len(df)} rows -> {uri}")
    return uri


def main() -> int:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("[preprocess] ERROR: S3_BUCKET 환경 변수가 설정되지 않았습니다.", file=sys.stderr)
        return 1

    prefix = os.environ.get("S3_PREFIX", "wine-quality")
    run_date = os.environ.get("DATA_DATE") or date.today().isoformat()
    test_size = float(os.environ.get("TEST_SIZE", "0.2"))
    seed = int(os.environ.get("RANDOM_SEED", "42"))

    s3 = boto3.client("s3")

    # 1) 원천 데이터 로드
    raw_key = f"{prefix}/raw/dt={run_date}/wine_quality_raw.csv"
    df = _read_csv_from_s3(s3, bucket, raw_key)
    print(f"[preprocess] loaded raw: {len(df)} rows")

    # 2) 정제: 결측치/중복 제거
    before = len(df)
    df = df.dropna().drop_duplicates().reset_index(drop=True)
    print(f"[preprocess] cleaned: {before} -> {len(df)} rows")

    # 3) 타깃 라벨 생성
    df[TARGET_COL] = (df["quality"] >= QUALITY_THRESHOLD).astype(int)
    df = df.drop(columns=["quality"])
    pos_ratio = df[TARGET_COL].mean()
    print(f"[preprocess] positive(label=1) ratio: {pos_ratio:.3f}")

    # 4) 범주형 인코딩 (red=0, white=1)
    df["wine_type"] = (df["wine_type"] == "white").astype(int)

    # 5) 피처/타깃 분리 및 train/test 분할
    features = [c for c in df.columns if c != TARGET_COL]
    x = df[features]
    y = df[TARGET_COL]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=test_size, random_state=seed, stratify=y
    )

    # 6) 수치형 표준화 (wine_type 제외한 연속형 피처)
    numeric_cols = [c for c in features if c != "wine_type"]
    scaler = StandardScaler()
    x_train = x_train.copy()
    x_test = x_test.copy()
    x_train[numeric_cols] = scaler.fit_transform(x_train[numeric_cols])
    x_test[numeric_cols] = scaler.transform(x_test[numeric_cols])

    train_df = x_train.copy()
    train_df[TARGET_COL] = y_train.values
    test_df = x_test.copy()
    test_df[TARGET_COL] = y_test.values

    # 7) S3 저장 (processed 파티션)
    base = f"{prefix}/processed/dt={run_date}"
    _write_parquet_to_s3(s3, train_df, bucket, f"{base}/train.parquet")
    _write_parquet_to_s3(s3, test_df, bucket, f"{base}/test.parquet")

    # 8) 스케일러 직렬화 후 저장 (서빙 단계 재사용)
    scaler_key = f"{base}/scaler.pkl"
    s3.put_object(Bucket=bucket, Key=scaler_key, Body=pickle.dumps(scaler))
    print(f"[preprocess] wrote scaler -> s3://{bucket}/{scaler_key}")

    print(
        f"[preprocess] DONE. train={len(train_df)} test={len(test_df)} "
        f"features={len(features)} base=s3://{bucket}/{base}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
