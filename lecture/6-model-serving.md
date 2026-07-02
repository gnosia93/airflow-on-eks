
# Lab: 모델 서빙 (EKS 추론 엔드포인트)

학습/튜닝 랩이 승격한 모델을 로드하는 FastAPI 추론 서버를 EKS 위에 배포한다.
Deployment + Service로 노출하며, 배포는 Airflow DAG로 오케스트레이션한다.
추론 엔드포인트는 전적으로 EKS 위에서 동작한다(SageMaker 미사용 - 요구사항 8.3).

## 학습 목표

- S3의 모델/스케일러를 로드하는 추론 서버(FastAPI)를 컨테이너화한다.
- EKS Deployment/Service로 추론 엔드포인트를 배포하고 readiness/liveness 프로브를 구성한다.
- Airflow DAG로 배포/롤아웃/검증을 자동화한다.

## 파이프라인 흐름

```
(models/dt=.../model.pkl)                deploy_serving          verify_endpoint
(processed/.../scaler.pkl)   ──►   (Pod: kubectl apply +   ──►   (Pod: /health 확인)
                                    rollout restart)
                                          │
                                          ▼
                                   wine-serving Deployment(2 replicas) + Service(LB)
```

## 사전 요구사항

- [모델 학습 랩](../model-training/README.md) 또는 [튜닝 랩](../hyperparameter-tuning/README.md)이 완료되어
  `models/dt=.../model.pkl` 과 `processed/.../scaler.pkl` 이 S3에 있어야 한다.
- 셸 환경 변수:

```bash
export AWS_REGION=ap-northeast-2
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export S3_BUCKET=my-mlops-workshop-bucket
export MODEL_DATE=$(date +%F)   # 모델을 학습한 파티션 일자
```

---

## Step 1. 추론 이미지 빌드 및 ECR 푸시

```bash
aws ecr create-repository --repository-name wine-serving --region "${AWS_REGION}" || true

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin \
    "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# 빌드 컨텍스트는 예제 루트 (Dockerfile은 docker/)
docker build -t wine-serving:latest -f docker/Dockerfile .
docker tag wine-serving:latest \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-serving:latest"
docker push "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-serving:latest"
```

## Step 2. RBAC 적용

Airflow 워커가 Deployment/Service를 배포할 수 있도록 권한을 부여한다.

```bash
kubectl apply -f k8s/rbac.yaml
```

## Step 3. 배포 방법 선택

### 방법 A. Airflow DAG로 배포 (권장, 요구사항 7)

Airflow Variable에 서빙 이미지 URI를 등록한다.

```bash
kubectl exec -n airflow deploy/airflow-scheduler -- \
  airflow variables set serving_image \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-serving:latest"
```

`dags/model_serving_dag.py`를 DAG 저장소에 push(git-sync)하거나 dags 경로에 복사한 뒤,
Airflow UI에서 `model_serving` DAG를 활성화하고 **Trigger DAG**를 클릭한다.
DAG는 매니페스트를 apply → 롤아웃 → `/health` 검증까지 자동 수행한다.

### 방법 B. kubectl로 직접 배포 (수동 확인용)

```bash
sed -e "s|__IMAGE__|${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-serving:latest|" \
    -e "s|__S3_BUCKET__|${S3_BUCKET}|" \
    -e "s|__MODEL_DATE__|${MODEL_DATE}|" \
    k8s/serving.yaml | kubectl apply -f -
```

---

## Validation (검증)

### 1. Pod / 롤아웃 상태

```bash
kubectl get pods -n airflow -l app=wine-serving
kubectl rollout status deployment/wine-serving -n airflow
```

기대 결과: `wine-serving` Pod 2개가 `Running` 및 `READY 1/1`.

### 2. 엔드포인트 주소 확인

```bash
kubectl get svc -n airflow wine-serving
```

`EXTERNAL-IP`(ELB 주소)가 프로비저닝될 때까지 대기. 로컬 확인은 포트포워딩:

```bash
kubectl port-forward -n airflow svc/wine-serving 8080:80
```

### 3. 헬스체크

```bash
curl -s http://localhost:8080/health
```

기대 결과:

```json
{"status": "ok", "model_loaded": true}
```

### 4. 추론 요청

```bash
curl -s -X POST http://localhost:8080/predict \
  -H "Content-Type: application/json" \
  -d '{
    "fixed_acidity": 7.4, "volatile_acidity": 0.7, "citric_acid": 0.0,
    "residual_sugar": 1.9, "chlorides": 0.076, "free_sulfur_dioxide": 11,
    "total_sulfur_dioxide": 34, "density": 0.9978, "pH": 3.51,
    "sulphates": 0.56, "alcohol": 9.4, "wine_type": "red"
  }'
```

기대 결과(예시):

```json
{"label": 0, "quality_good_probability": 0.12, "prediction": "not_good"}
```

---

## 문제 해결

| 증상 | 원인 / 대응 |
|------|-------------|
| Pod `CrashLoopBackOff`, 로그에 `NoSuchKey` | `MODEL_DATE` 파티션에 모델/스케일러 없음. 학습·전처리 파티션 일자 확인 |
| Pod `Running`이나 `READY 0/1` | readiness 프로브 실패 = 모델 로드 전. 로그 `kubectl logs -n airflow <pod>` 확인 |
| `deploy_serving` 태스크 `Forbidden` | RBAC 미적용. `kubectl apply -f k8s/rbac.yaml` 재확인 |
| `/predict` 500 오류 | 입력 피처 누락/형식 오류. 요청 JSON 필드명을 스키마와 대조 |
| EXTERNAL-IP `<pending>` 지속 | 서브넷 LB 태그 문제. 포트포워딩으로 우회 |

---

## 정리

```bash
kubectl delete -f k8s/rbac.yaml
kubectl delete deployment,svc wine-serving -n airflow
```

---

## 파이프라인 완성

이로써 데이터 수집 → 전처리 → 학습 → 튜닝 → 서빙까지 전체 MLOps 파이프라인이 모두 EKS 위에서
동작한다. 각 단계를 하나의 통합 DAG로 잇고 싶다면, 각 랩의 태스크를
`collect >> analyze >> preprocess >> train >> tune >> deploy_serving` 순서로 결합한 마스터 DAG를 구성한다.
