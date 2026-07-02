"""
데이터 수집(Data Collection) 스크립트.

UCI Wine Quality 공개 데이터셋(red/white)을 내려받아 하나의 원천(raw) CSV로
합친 뒤 S3에 업로드한다. 이 스크립트는 EKS Pod 안에서 실행되며, S3 접근 권한은
IRSA(IAM Roles for Service Accounts)를 통해 Pod의 서비스 계정에 부여된다.

환경 변수:
    S3_BUCKET   : 산출물을 저장할 S3 버킷 이름 (필수)
    S3_PREFIX   : 버킷 내 접두사(prefix). 기본값 "wine-quality"
    DATA_DATE   : 파티션용 실행 일자(YYYY-MM-DD). 미지정 시 오늘 날짜.
"""

import io
import os
import sys
from datetime import date

import boto3
import pandas as pd

RED_URL = (
    "https://archive.ics.uci.edu/ml/machine-learning-databases/"
    "wine-quality/winequality-red.csv"
)
WHITE_URL = (
    "https://archive.ics.uci.edu/ml/machine-learning-databases/"
    "wine-quality/winequality-white.csv"
)


def _download(url: str, wine_type: str) -> pd.DataFrame:
    """공개 URL에서 CSV를 읽어 DataFrame으로 반환하고 wine_type 컬럼을 추가한다."""
    print(f"[collect] downloading {wine_type} wine data from {url}")
    # UCI 원본은 세미콜론(;) 구분자를 사용한다.
    df = pd.read_csv(url, sep=";")
    df["wine_type"] = wine_type
    print(f"[collect] {wine_type}: {len(df)} rows, {df.shape[1]} columns")
    return df


def main() -> int:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("[collect] ERROR: S3_BUCKET 환경 변수가 설정되지 않았습니다.", file=sys.stderr)
        return 1

    prefix = os.environ.get("S3_PREFIX", "wine-quality")
    run_date = os.environ.get("DATA_DATE") or date.today().isoformat()

    # 1) 두 데이터셋 수집 후 결합
    red = _download(RED_URL, "red")
    white = _download(WHITE_URL, "white")
    raw = pd.concat([red, white], ignore_index=True)
    print(f"[collect] combined raw dataset: {len(raw)} rows")

    # 2) 메모리 버퍼에 CSV로 직렬화
    buffer = io.StringIO()
    raw.to_csv(buffer, index=False)

    # 3) S3 업로드 (날짜 파티션 경로)
    key = f"{prefix}/raw/dt={run_date}/wine_quality_raw.csv"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue().encode("utf-8"))

    uri = f"s3://{bucket}/{key}"
    print(f"[collect] uploaded raw dataset -> {uri}")
    print(f"[collect] DONE. rows={len(raw)} uri={uri}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
