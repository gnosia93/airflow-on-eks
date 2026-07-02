"""
모델 서빙 배포 Airflow DAG.

승격된 모델을 로드하는 추론 서버(FastAPI)를 EKS에 배포/갱신한다.

    deploy_serving (Pod: kubectl apply) ──► verify_endpoint (Pod: /health 호출)

deploy_serving 태스크는 bitnami/kubectl 이미지를 실행하는 Pod에서 매니페스트를 apply하고
rollout 완료를 기다린다. 이를 위해 airflow-worker 서비스 계정에 serving-deployer Role이
바인딩되어 있어야 한다(k8s/rbac.yaml).

튜닝/학습 랩이 models/dt=.../model.pkl 을 갱신한 뒤 이 DAG를 실행하면, Deployment가
새 모델을 로드한 상태로 롤아웃된다. (SageMaker 미사용 - 요구사항 8.3)
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
SERVING_IMAGE = Variable.get("serving_image", default_var="<ACCOUNT_ID>.dkr.ecr.ap-northeast-2.amazonaws.com/wine-serving:latest")
S3_BUCKET = Variable.get("mlops_s3_bucket", default_var="my-mlops-workshop-bucket")
KUBECTL_IMAGE = "bitnami/kubectl:1.30"
SERVICE_ACCOUNT = "airflow-worker"
NAMESPACE = "airflow"

# Deployment/Service 매니페스트 (kubectl apply -f - 로 stdin 적용).
# MODEL_DATE 는 Airflow 실행일(ds)을 사용해 해당 파티션의 모델을 로드한다.
SERVING_MANIFEST = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: wine-serving
  namespace: {NAMESPACE}
  labels:
    app: wine-serving
spec:
  replicas: 2
  selector:
    matchLabels:
      app: wine-serving
  template:
    metadata:
      labels:
        app: wine-serving
    spec:
      serviceAccountName: {SERVICE_ACCOUNT}
      containers:
        - name: serving
          image: {SERVING_IMAGE}
          ports:
            - containerPort: 8000
          env:
            - name: S3_BUCKET
              value: "{S3_BUCKET}"
            - name: S3_PREFIX
              value: "wine-quality"
            - name: MODEL_DATE
              value: "{{{{ ds }}}}"
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 20
            periodSeconds: 15
---
apiVersion: v1
kind: Service
metadata:
  name: wine-serving
  namespace: {NAMESPACE}
  labels:
    app: wine-serving
spec:
  type: LoadBalancer
  selector:
    app: wine-serving
  ports:
    - name: http
      port: 80
      targetPort: 8000
"""

# 매니페스트를 apply 하고 롤아웃 완료를 대기.
# 새 모델 로드를 확실히 하기 위해 rollout restart 후 상태를 확인한다.
DEPLOY_SCRIPT = f"""set -e
cat <<'MANIFEST' | kubectl apply -f -
{SERVING_MANIFEST}
MANIFEST
kubectl -n {NAMESPACE} rollout restart deployment/wine-serving
kubectl -n {NAMESPACE} rollout status deployment/wine-serving --timeout=180s
"""

# 클러스터 내부 DNS로 /health 를 호출해 준비 상태 검증.
VERIFY_SCRIPT = f"""set -e
echo "checking wine-serving /health ..."
for i in $(seq 1 12); do
  code=$(wget -q -O - "http://wine-serving.{NAMESPACE}.svc.cluster.local/health" || true)
  echo "attempt $i: $code"
  case "$code" in
    *'"model_loaded": true'*|*'"model_loaded":true'*) echo "OK: serving ready"; exit 0 ;;
  esac
  sleep 10
done
echo "ERROR: serving not ready in time"; exit 1
"""

default_args = {
    "owner": "mlops-workshop",
    "retries": 1,
}

with DAG(
    dag_id="model_serving",
    description="Wine Quality 추론 엔드포인트 EKS 배포",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "workshop", "serving"],
) as dag:

    deploy_serving = KubernetesPodOperator(
        task_id="deploy_serving",
        name="deploy-serving",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=KUBECTL_IMAGE,
        cmds=["/bin/sh", "-c"],
        arguments=[DEPLOY_SCRIPT],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    verify_endpoint = KubernetesPodOperator(
        task_id="verify_endpoint",
        name="verify-endpoint",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=KUBECTL_IMAGE,
        cmds=["/bin/sh", "-c"],
        arguments=[VERIFY_SCRIPT],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    deploy_serving >> verify_endpoint
