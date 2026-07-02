## 데이터 수집 및 전처리 ##

Wine Quality 공개 데이터셋을 수집하고 전처리하는 MLOps 파이프라인 앞단을 실습한다.
모든 연산은 EKS 클러스터의 Pod에서 실행되며, 중간 산출물은 S3에 저장된다. 


### 1. 파이프라인 흐름 ###

```
collect_data (Pod)  ──►  preprocess_data (Pod)
     │                          │
     ▼                          ▼
 s3://.../raw/            s3://.../processed/
 wine_quality_raw.csv     train.parquet, test.parquet, scaler.pkl
```

### 디렉터리 구조 ###

```
data-collection-preprocessing/
├── dags/
│   └── data_collection_preprocessing_dag.py   # Airflow DAG
├── scripts/
│   ├── collect_data.py                         # 수집 스크립트
│   └── preprocess_data.py                      # 전처리 스크립트
├── docker/
│   ├── Dockerfile
│   └── requirements.txt
└── README.md
```

### Step 1. S3 버킷 생성 ###

```bash
export AWS_REGION=ap-northeast-2
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export S3_BUCKET=my-mlops-workshop-bucket   # 고유한 이름으로 교체
export CLUSTER_NAME=airflow-mlops
```

```bash
aws s3 mb "s3://${S3_BUCKET}" --region "${AWS_REGION}"
```

### Step 2. 이미지 빌드 후 ECR 푸시 ###

```bash
# ECR 리포지토리 생성
aws ecr create-repository --repository-name wine-mlops --region "${AWS_REGION}" || true

# 로그인
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin \
    "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# 빌드 (컨텍스트는 예제 루트, Dockerfile은 docker/)
docker build -t wine-mlops:latest -f docker/Dockerfile .

# 태그 및 푸시
docker tag wine-mlops:latest \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"
docker push "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"
```

### Step 3. IRSA 서비스 계정 생성 (S3 접근 권한) ###

Pod가 S3에 접근하려면 서비스 계정에 IAM 역할을 매핑한다.

```bash
eksctl create iamserviceaccount \
  --name airflow-worker \
  --namespace airflow \
  --cluster "${CLUSTER_NAME}" \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --approve \
  --override-existing-serviceaccounts
```

> 운영 환경에서는 `AmazonS3FullAccess` 대신 해당 버킷에만 접근하는 최소 권한 정책을 사용한다.

### Step 4. Airflow Variable 설정 ###

DAG가 참조하는 이미지 URI와 버킷 이름을 주입한다.

```bash
kubectl exec -n airflow deploy/airflow-scheduler -- \
  airflow variables set mlops_image \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"

kubectl exec -n airflow deploy/airflow-scheduler -- \
  airflow variables set mlops_s3_bucket "${S3_BUCKET}"
```

### Step 5. DAG 배포 ###

git-sync를 쓰는 경우 `dags/data_collection_preprocessing_dag.py`를 DAG 저장소에 push하면 자동 반영된다.
수동 복사 방식이라면 스케줄러/워커의 dags 경로에 파일을 넣는다.

Airflow UI에서 `data_collection_preprocessing` DAG를 활성화한 뒤 **Trigger DAG**를 클릭한다.

### Validation (검증) ###

1. **DAG 실행 상태 확인** — Airflow UI Grid 뷰에서 두 태스크가 모두 초록색(success)인지 확인.

2. **Pod 실행 확인** (실행 중 관찰):
   ```bash
   kubectl get pods -n airflow -w | grep -E "collect-data|preprocess-data"
   ```

3. **S3 산출물 확인**:
   ```bash
   aws s3 ls "s3://${S3_BUCKET}/wine-quality/raw/" --recursive
   aws s3 ls "s3://${S3_BUCKET}/wine-quality/processed/" --recursive
   ```
   기대 결과: `wine_quality_raw.csv`, `train.parquet`, `test.parquet`, `scaler.pkl` 이 존재.

4. **태스크 로그 확인** — 전처리 태스크 로그 마지막 줄에 다음과 유사한 출력:
   ```
   [preprocess] DONE. train=... test=... features=12 base=s3://.../processed/dt=...
   ```

### 문제 해결 ###

| 증상 | 원인 / 대응 |
|------|-------------|
| Pod가 `CreateContainerError` | ECR 이미지 URI 오타 또는 노드의 ECR pull 권한 누락. 노드 IAM 역할에 `AmazonEC2ContainerRegistryReadOnly` 확인 |
| `AccessDenied` (S3) | IRSA 미적용. `kubectl describe sa airflow-worker -n airflow`로 `eks.amazonaws.com/role-arn` 애노테이션 확인 |
| `collect_data` 네트워크 오류 | 노드가 프라이빗 서브넷이면 NAT Gateway 필요 (UCI 외부 다운로드) |


