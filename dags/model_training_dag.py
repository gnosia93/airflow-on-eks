"""
모델 학습 Airflow DAG.

전처리 산출물을 입력으로 받아 모델을 학습하는 단계를 오케스트레이션한다.
학습 태스크는 KubernetesPodOperator로 EKS Pod에서 실행된다.

이 랩에서는 학습 워크로드를 GPU 노드 그룹(gpu-workers)에 스케줄하는 패턴을 보여준다.
GradientBoosting 자체는 CPU 연산이지만, 워크숍에서는 GPU 노드에 태스크를 배치하는
nodeSelector / tolerations 구성을 학습하는 것이 목적이다. GPU가 불필요하면
USE_GPU_NODE 상수를 False로 바꾸면 일반 워커 노드에서 실행된다.

사전 준비:
    - data-collection-preprocessing 랩이 완료되어 processed 데이터가 S3에 있어야 한다.
    - collect/preprocess와 동일한 이미지를 재사용하되, scripts/train_model.py가 포함되어야 한다.
"""

from __future__ import annotations

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

# 학습 태스크를 GPU 노드 그룹에 스케줄할지 여부
USE_GPU_NODE = True

COMMON_ENV = [
    k8s.V1EnvVar(name="S3_BUCKET", value=S3_BUCKET),
    k8s.V1EnvVar(name="S3_PREFIX", value=S3_PREFIX),
    k8s.V1EnvVar(name="DATA_DATE", value="{{ ds }}"),
    # 하이퍼파라미터 (튜닝 랩에서 이 값들을 탐색)
    k8s.V1EnvVar(name="N_ESTIMATORS", value="200"),
    k8s.V1EnvVar(name="MAX_DEPTH", value="8"),
    k8s.V1EnvVar(name="LEARNING_RATE", value="0.1"),
]

# GPU 노드 그룹(cluster.yaml의 gpu-workers)에 스케줄하기 위한 설정.
# 해당 노드 그룹은 label workload=gpu 와 taint nvidia.com/gpu=true:NoSchedule 을 가진다.
NODE_SELECTOR = {"workload": "gpu"} if USE_GPU_NODE else {"workload": "general"}
TOLERATIONS = (
    [
        k8s.V1Toleration(
            key="nvidia.com/gpu", operator="Equal", value="true", effect="NoSchedule"
        )
    ]
    if USE_GPU_NODE
    else []
)

TRAIN_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "1", "memory": "2Gi"},
    limits={"cpu": "2", "memory": "4Gi"},
)

default_args = {
    "owner": "mlops-workshop",
    "retries": 1,
}

with DAG(
    dag_id="model_training",
    description="Wine Quality 모델 학습 (EKS Pod 실행)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "workshop", "training"],
) as dag:

    train_model = KubernetesPodOperator(
        task_id="train_model",
        name="train-model",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=IMAGE,
        cmds=["python", "/app/scripts/train_model.py"],
        env_vars=COMMON_ENV,
        container_resources=TRAIN_RESOURCES,
        node_selector=NODE_SELECTOR,
        tolerations=TOLERATIONS,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )
