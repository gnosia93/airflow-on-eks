"""
하이퍼파라미터 튜닝 - 최적 시도 선택(select best) 스크립트.

모든 시도(trial)의 metrics.json을 읽어 roc_auc가 가장 높은 시도를 선택하고,
해당 후보 모델을 프로덕션 경로(models/dt=.../model.pkl)로 승격한다.
리더보드(leaderboard.json)와 최적 하이퍼파라미터(best_params.json)도 함께 저장한다.

환경 변수:
    S3_BUCKET  : S3 버킷 이름 (필수)
    S3_PREFIX  : 접두사. 기본값 "wine-quality"
    DATA_DATE  : 파티션 일자. 미지정 시 오늘 날짜.
    METRIC     : 선택 기준 지표. 기본값 "roc_auc"
"""

import json
import os
import sys
from datetime import date

import boto3

TARGET_COL = "label"


def main() -> int:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("[select] ERROR: S3_BUCKET 환경 변수가 필요합니다.", file=sys.stderr)
        return 1

    prefix = os.environ.get("S3_PREFIX", "wine-quality")
    run_date = os.environ.get("DATA_DATE") or date.today().isoformat()
    metric = os.environ.get("METRIC", "roc_auc")

    s3 = boto3.client("s3")
    trials_prefix = f"{prefix}/tuning/dt={run_date}/trials/"

    # 1) 모든 시도의 metrics.json 수집
    paginator = s3.get_paginator("list_objects_v2")
    leaderboard = []
    for page in paginator.paginate(Bucket=bucket, Prefix=trials_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("metrics.json"):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                leaderboard.append(json.loads(body))

    if not leaderboard:
        print(f"[select] ERROR: 시도 결과가 없습니다: s3://{bucket}/{trials_prefix}", file=sys.stderr)
        return 1

    # 2) 기준 지표로 정렬 (내림차순)
    leaderboard.sort(key=lambda m: m.get(metric, float("-inf")), reverse=True)
    best = leaderboard[0]
    print(f"[select] {len(leaderboard)} trials 평가. 기준={metric}")
    for rank, m in enumerate(leaderboard, 1):
        print(f"  #{rank} {m['trial_id']}: {metric}={m.get(metric)} params={m['hyperparameters']}")

    # 3) 최적 후보 모델을 프로덕션 경로로 승격 (S3 복사)
    best_trial = best["trial_id"]
    src_key = f"{prefix}/tuning/dt={run_date}/trials/{best_trial}/model.pkl"
    dst_key = f"{prefix}/models/dt={run_date}/model.pkl"
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
    )
    print(f"[select] promoted {best_trial} -> s3://{bucket}/{dst_key}")

    # 4) 리더보드 및 최적 파라미터 저장
    model_base = f"{prefix}/models/dt={run_date}"
    s3.put_object(
        Bucket=bucket,
        Key=f"{model_base}/leaderboard.json",
        Body=json.dumps(leaderboard, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    s3.put_object(
        Bucket=bucket,
        Key=f"{model_base}/best_params.json",
        Body=json.dumps(best, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    print(
        f"[select] DONE. best={best_trial} {metric}={best.get(metric)} "
        f"params={best['hyperparameters']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
