
# Lab: 모델 학습 (EKS Pod 실행)

전처리 랩이 만든 `train.parquet` / `test.parquet`를 입력으로 받아 분류 모델을 학습하고,
평가 지표와 함께 S3에 저장한다. 학습 연산은 EKS Pod에서 실행되며, GPU 노드 그룹에
태스크를 배치하는 패턴을 함께 실습한다. SageMaker는 사용하지 않는다.

## 학습 목표

- 전처리 산출물을 읽어 EKS Pod에서 모델을 학습한다.
- 학습된 모델(`model.pkl`)과 평가 지표(`metrics.json`)를 S3에 저장하는 패턴을 익힌다.
- `KubernetesPodOperator`의 `node_selector` / `tolerations`로 학습 태스크를 GPU 노드 그룹에 스케줄한다.

## 파이프라인 흐름

```
(preprocess 산출물)               train_model (Pod, GPU 노드)
 s3://.../processed/       ──►          │
 train.parquet, test.parquet           ▼
                                  s3://.../models/dt=.../
                                  model.pkl, metrics.json
```

## 사전 요구사항

- [데이터 수집·전처리 랩](../data-collection-preprocessing/README.md)이 완료되어 processed 데이터가 S3에 있어야 한다.
- EKS 클러스터에 GPU 노드 그룹(`gpu-workers`)이 정의되어 있어야 한다([EKS 프로비저닝 랩](../eks-provisioning/README.md)의 `cluster.yaml` 참고).
- 셸 환경 변수:

```bash
export AWS_REGION=ap-northeast-2
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export S3_BUCKET=my-mlops-workshop-bucket
export CLUSTER_NAME=airflow-mlops
```

---

## Step 1. 학습 스크립트를 이미지에 포함

이 랩은 수집/전처리와 동일한 `wine-mlops` 이미지를 재사용한다. `scripts/train_model.py`를
이미지에 추가한 뒤 다시 빌드/푸시한다.

```bash
# data-collection-preprocessing 예제의 scripts 디렉터리로 학습 스크립트 복사
cp scripts/train_model.py ../data-collection-preprocessing/scripts/

# 이미지 재빌드 & 푸시 (data-collection-preprocessing 디렉터리에서)
cd ../data-collection-preprocessing
docker build -t wine-mlops:latest -f docker/Dockerfile .
docker tag wine-mlops:latest \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"
docker push "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"
cd -
```

> 학습 스크립트는 추가 의존성이 없다(scikit-learn은 이미 이미지에 포함). 그래서 `requirements.txt` 변경 없이 재빌드만 하면 된다.

## Step 2. GPU 노드 스케줄링 준비

GPU 노드에서 학습하려면 NVIDIA device plugin이 필요하다(GPU 리소스를 스케줄러에 노출).

```bash
kubectl apply -f \
  https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.15.0/deployments/static/nvidia-device-plugin.yml
```

> GPU 노드 그룹은 `desiredCapacity: 0`이라, 학습 태스크가 스케줄되면 Cluster Autoscaler가
> 노드를 기동한다. Autoscaler가 없다면 노드 그룹을 임시로 1로 올린다:
> ```bash
> eksctl scale nodegroup --cluster "${CLUSTER_NAME}" --name gpu-workers \
>   --nodes 1 --nodes-min 0 --nodes-max 2 --region "${AWS_REGION}"
> ```
> GPU가 필요 없다면 `dags/model_training_dag.py`의 `USE_GPU_NODE = False`로 두고 일반 노드에서 실행한다.

## Step 3. DAG 배포 및 실행

`dags/model_training_dag.py`를 DAG 저장소에 push(git-sync)하거나 dags 경로에 복사한다.
Airflow UI에서 `model_training` DAG를 활성화하고 **Trigger DAG**를 클릭한다.

---

## Validation (검증)

### 1. 태스크 / Pod 실행 확인

```bash
# GPU 노드가 기동되는지 (USE_GPU_NODE=True인 경우)
kubectl get nodes -l workload=gpu

# 학습 Pod 상태
kubectl get pods -n airflow -w | grep train-model
```

### 2. 모델 산출물 확인

```bash
aws s3 ls "s3://${S3_BUCKET}/wine-quality/models/" --recursive
```

기대 결과: `model.pkl`, `metrics.json`이 존재.

### 3. 평가 지표 확인

```bash
aws s3 cp "s3://${S3_BUCKET}/wine-quality/models/dt=$(date +%F)/metrics.json" - | cat
```

기대 결과(예시): ROC-AUC가 0.8 이상.

```json
{
  "accuracy": 0.86,
  "precision": 0.71,
  "recall": 0.55,
  "f1": 0.62,
  "roc_auc": 0.89,
  "hyperparameters": { "n_estimators": 200, "max_depth": 8, "learning_rate": 0.1 }
}
```

### 4. Airflow 로그 확인

태스크 로그 마지막 줄:

```
[train] DONE. roc_auc=0.89 f1=0.62
```

---

## 문제 해결

| 증상 | 원인 / 대응 |
|------|-------------|
| 학습 Pod가 `Pending` 지속 | GPU 노드 미기동. NVIDIA device plugin 설치 여부와 노드 스케일 확인. Autoscaler 없으면 Step 2의 수동 스케일 |
| `NoSuchKey` (S3) | processed 데이터 없음. 전처리 랩을 먼저 실행하고 `DATA_DATE` 파티션이 일치하는지 확인 |
| `AccessDenied` (S3) | IRSA 미적용. `airflow-worker` 서비스 계정 애노테이션 확인 |
| toleration 미적용으로 스케줄 실패 | DAG의 `tolerations`가 노드 taint(`nvidia.com/gpu=true:NoSchedule`)와 일치하는지 확인 |

---

## 다음 단계

학습이 끝나면 하이퍼파라미터 튜닝 랩으로 이동해, 이 학습 스크립트의 하이퍼파라미터
(`N_ESTIMATORS`, `MAX_DEPTH`, `LEARNING_RATE`)를 병렬 태스크로 탐색하고 최적 모델을 선택한다.
그 후 모델 서빙 랩에서 `model.pkl`과 전처리의 `scaler.pkl`을 함께 로드해 EKS 위에 추론 엔드포인트를 배포한다.
