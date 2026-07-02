"""
하이퍼파라미터 튜닝 Airflow DAG.

여러 하이퍼파라미터 조합을 Airflow 동적 태스크 매핑(dynamic task mapping)으로
병렬 실행한다. 각 조합(trial)은 독립된 EKS Pod에서 학습되고, 모든 시도가 끝나면
select_best 태스크가 최적 모델을 선택해 프로덕션 경로로 승격한다.

    tune_train (병렬, N개 Pod)  ──►  select_best (Pod)

각 태스크는 KubernetesPodOperator로 EKS 위에서 실행된다. (SageMaker 미사용 - 요구사항 8)

사전 준비:
    - data-collection-preprocessing 랩이 완료되어 processed 데이터가 S3에 있어야 한다.
    - scripts/tune_train.py, scripts/select_best.py 가 wine-mlops 이미지에 포함되어야 한다.
"""

from __future__ import annotations

import itertools
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
IMAGE = Variable.get("mlops_image", default_var="<ACCOUNT_ID>.dkr.ecr.ap-northeast-2.amazonaws.com/wine-mlops:latest")
S3_BUCKET = Variable.get("mlops_s3_bucket", default_var="my-mlops-workshop-bucket")
S3_PREFIX = "wine-quality"
SERVICE_ACCOUNT = "airflow-worker"
NAMESPACE = "airflow"

# ---------------------------------------------------------------------------
# 하이퍼파라미터 탐색 공간 (grid search)
# ---------------------------------------------------------------------------
SEARCH_SPACE = {
    "n_estimators": [100, 300],
    "max_depth": [3, 6],
    "learning_rate": [0.05, 0.1],
}


def _build_trial_env_vars() -> list[list[k8s.V1EnvVar]]:
    """탐색 공간의 모든 조합을 각 trial의 env_vars 리스트로 변환한다."""
    keys = list(SEARCH_SPACE.keys())
    combos = list(itertools.product(*[SEARCH_SPACE[k] for k in keys]))
    env_lists: list[list[k8s.V1EnvVar]] = []
    for i, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        env_lists.append(
            [
                k8s.V1EnvVar(name="S3_BUCKET", value=S3_BUCKET),
                k8s.V1EnvVar(name="S3_PREFIX", value=S3_PREFIX),
                k8s.V1EnvVar(name="DATA_DATE", value="{{ ds }}"),
                k8s.V1EnvVar(name="TRIAL_ID", value=f"trial-{i}"),
                k8s.V1EnvVar(name="N_ESTIMATORS", value=str(params["n_estimators"])),
                k8s.V1EnvVar(name="MAX_DEPTH", value=str(params["max_depth"])),
                k8s.V1EnvVar(name="LEARNING_RATE", value=str(params["learning_rate"])),
            ]
        )
    return env_lists


TRIAL_ENV_VARS = _build_trial_env_vars()

POD_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "1", "memory": "2Gi"},
    limits={"cpu": "2", "memory": "4Gi"},
)

default_args = {
    "owner": "mlops-workshop",
    "retries": 1,
}

with DAG(
    dag_id="hyperparameter_tuning",
    description="Wine Quality 하이퍼파라미터 튜닝 (EKS Pod 병렬 실행)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "workshop", "tuning"],
) as dag:

    # 동적 태스크 매핑: 조합 수만큼 tune-train Pod를 병렬 생성
    tune_train = KubernetesPodOperator.partial(
        task_id="tune_train",
        name="tune-train",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=IMAGE,
        cmds=["python", "/app/scripts/tune_train.py"],
        container_resources=POD_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    ).expand(env_vars=TRIAL_ENV_VARS)

    # 모든 시도 완료 후 최적 모델 선택 및 승격
    select_best = KubernetesPodOperator(
        task_id="select_best",
        name="select-best",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=IMAGE,
        cmds=["python", "/app/scripts/select_best.py"],
        env_vars=[
            k8s.V1EnvVar(name="S3_BUCKET", value=S3_BUCKET),
            k8s.V1EnvVar(name="S3_PREFIX", value=S3_PREFIX),
            k8s.V1EnvVar(name="DATA_DATE", value="{{ ds }}"),
            k8s.V1EnvVar(name="METRIC", value="roc_auc"),
        ],
        container_resources=POD_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    tune_train >> select_best
