"""
데이터 수집 + 전처리 Airflow DAG.

MLOps 파이프라인의 앞단 두 단계를 오케스트레이션한다.

    collect_data  ->  preprocess_data

각 태스크는 KubernetesPodOperator로 EKS 클러스터 위에 독립된 Pod를 띄워 실행한다.
Pod는 IRSA가 매핑된 서비스 계정(airflow-worker)으로 실행되어 S3에 접근한다.
(SageMaker를 사용하지 않고 모든 연산을 EKS Pod에서 수행 - 요구사항 8)

사전 준비:
    - docker/ 디렉터리 이미지를 빌드해 ECR에 푸시하고 아래 IMAGE 값을 교체한다.
    - Airflow Variable 또는 환경으로 S3 버킷 이름을 주입한다 (여기서는 Variable 사용).
    - S3 접근용 IRSA 서비스 계정(airflow-worker)이 airflow 네임스페이스에 존재해야 한다.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# ---------------------------------------------------------------------------
# 설정: 실습 환경에 맞게 교체
# ---------------------------------------------------------------------------
# ECR 이미지 URI (예: 123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/wine-mlops:latest)
IMAGE = Variable.get("mlops_image", default_var="<ACCOUNT_ID>.dkr.ecr.ap-northeast-2.amazonaws.com/wine-mlops:latest")
# 산출물 저장 S3 버킷
S3_BUCKET = Variable.get("mlops_s3_bucket", default_var="my-mlops-workshop-bucket")
S3_PREFIX = "wine-quality"
# IRSA가 매핑된 서비스 계정 (airflow 네임스페이스)
SERVICE_ACCOUNT = "airflow-worker"
NAMESPACE = "airflow"

# 태스크 Pod에 공통으로 주입할 환경 변수.
# DATA_DATE는 Airflow 실행일(ds)을 사용하여 파티션을 일관되게 유지한다.
COMMON_ENV = [
    k8s.V1EnvVar(name="S3_BUCKET", value=S3_BUCKET),
    k8s.V1EnvVar(name="S3_PREFIX", value=S3_PREFIX),
    k8s.V1EnvVar(name="DATA_DATE", value="{{ ds }}"),
]

# Pod 리소스 요청/제한 (전처리는 메모리를 조금 더 준다)
POD_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
)

default_args = {
    "owner": "mlops-workshop",
    "retries": 1,
}

with DAG(
    dag_id="data_collection_preprocessing",
    description="Wine Quality 데이터 수집 및 전처리 (EKS Pod 실행)",
    default_args=default_args,
    schedule=None,  # 수동 트리거 (워크숍 실습용)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "workshop", "data"],
) as dag:

    collect_data = KubernetesPodOperator(
        task_id="collect_data",
        name="collect-data",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=IMAGE,
        cmds=["python", "/app/scripts/collect_data.py"],
        env_vars=COMMON_ENV,
        container_resources=POD_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,  # 완료 후 Pod 정리
        in_cluster=True,
    )

    preprocess_data = KubernetesPodOperator(
        task_id="preprocess_data",
        name="preprocess-data",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=IMAGE,
        cmds=["python", "/app/scripts/preprocess_data.py"],
        env_vars=COMMON_ENV + [
            k8s.V1EnvVar(name="TEST_SIZE", value="0.2"),
            k8s.V1EnvVar(name="RANDOM_SEED", value="42"),
        ],
        container_resources=POD_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # 실행 순서 의존성: 수집 -> 전처리
    collect_data >> preprocess_data
