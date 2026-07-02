# Lab: EKS 위에 Apache Airflow 설치

Amazon EKS 클러스터 위에 공식 Helm Chart로 Apache Airflow를 설치하고, MLOps 파이프라인을
오케스트레이션할 수 있는 상태까지 만든다. 이 랩이 끝나면 Airflow 웹 UI에 접속하고, 태스크를
EKS Pod로 실행할 수 있는 환경이 준비된다.

## 학습 목표

- 공식 Airflow Helm Chart를 사용해 EKS에 Airflow를 배포한다.
- `KubernetesExecutor` 기반 구성과 각 구성요소(Scheduler, Webserver, Worker, Metadata DB)의 역할을 이해한다.
- git-sync로 DAG를 배포하고, IRSA로 워커 Pod에 AWS 접근 권한을 부여한다.
- 설치 검증과 대표적인 문제 해결 절차를 수행한다.

## 사전 요구사항

| 도구 | 최소 버전 | 확인 명령 |
|------|-----------|-----------|
| AWS CLI | 2.x | `aws --version` |
| kubectl | 1.28+ | `kubectl version --client` |
| eksctl | 0.180+ | `eksctl version` |
| Helm | 3.12+ | `helm version` |

- EKS 클러스터가 이미 생성되어 있고 `kubectl`이 해당 클러스터를 가리켜야 한다.
- `--with-oidc` 옵션으로 클러스터에 OIDC 공급자가 활성화되어 있어야 한다(IRSA용).

```bash
export AWS_REGION=ap-northeast-2
export CLUSTER_NAME=airflow-mlops

# 컨텍스트 연결 확인
kubectl config current-context
kubectl get nodes
```

기대 결과: 노드가 `Ready` 상태로 출력된다.

---

## Step 1. 네임스페이스 및 스토리지 준비

Airflow의 메타데이터 DB와 로그는 PersistentVolume을 사용하므로 EBS CSI Driver가 필요하다.

```bash
# 네임스페이스 생성
kubectl create namespace airflow

# EBS CSI Driver 애드온 설치 (이미 있으면 무시됨)
eksctl create addon --name aws-ebs-csi-driver \
  --cluster "${CLUSTER_NAME}" --force

# 기본 StorageClass 확인
kubectl get storageclass
```

기대 결과: `gp2` 또는 `gp3` StorageClass가 존재한다.

---

## Step 2. IRSA 서비스 계정 생성

Airflow 워커/스케줄러가 만드는 태스크 Pod가 S3/ECR 등에 접근하려면 서비스 계정에 IAM 역할을 매핑한다.

```bash
eksctl create iamserviceaccount \
  --name airflow-worker \
  --namespace airflow \
  --cluster "${CLUSTER_NAME}" \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --approve \
  --override-existing-serviceaccounts
```

> 운영 환경에서는 `AmazonS3FullAccess` 대신 특정 버킷만 허용하는 최소 권한 정책을 사용한다.

확인:

```bash
kubectl describe sa airflow-worker -n airflow | grep role-arn
```

기대 결과: `eks.amazonaws.com/role-arn` 애노테이션에 IAM 역할 ARN이 표시된다.

---

## Step 3. Helm 저장소 추가

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

---

## Step 4. Airflow 설치

같은 디렉터리의 `values.yaml`을 사용한다. 설치 전 `values.yaml`의 `dags.gitSync.repo`를
본인의 DAG 저장소로 교체한다.

```bash
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  -f values.yaml \
  --timeout 10m
```

설치에는 수 분이 걸린다(이미지 pull + DB 마이그레이션).

---

## Validation (검증)

### 1. Pod 상태 확인

```bash
kubectl get pods -n airflow
```

기대 결과: 아래 Pod들이 모두 `Running`(또는 `Completed`) 상태.

```
NAME                                 READY   STATUS    RESTARTS   AGE
airflow-scheduler-...                2/2     Running   0          3m
airflow-webserver-...                1/1     Running   0          3m
airflow-triggerer-...                2/2     Running   0          3m
airflow-postgresql-0                 1/1     Running   0          3m
airflow-statsd-...                   1/1     Running   0          3m
```

### 2. 웹 UI 접속

LoadBalancer 방식(values.yaml 기본):

```bash
kubectl get svc -n airflow airflow-webserver
```

`EXTERNAL-IP`(ELB 주소)가 프로비저닝되면 브라우저에서 `http://<EXTERNAL-IP>:8080` 접속.

LoadBalancer가 없거나 로컬에서 빠르게 확인하려면 포트포워딩:

```bash
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
```

브라우저에서 `http://localhost:8080` 접속 후 `admin / admin`으로 로그인.
기대 결과: Airflow DAG 목록 화면이 표시된다.

### 3. git-sync 동작 확인

```bash
kubectl logs -n airflow deploy/airflow-scheduler -c git-sync -f
```

기대 결과: 저장소를 clone/pull한 로그가 보이고, UI의 DAG 목록에 저장소의 DAG가 나타난다.

---

## 문제 해결

| 증상 | 원인 / 대응 |
|------|-------------|
| Pod가 `Pending` 상태로 멈춤 | 노드 리소스 부족 또는 PVC 바인딩 실패. `kubectl describe pod <pod> -n airflow`로 이벤트 확인. EBS CSI Driver / StorageClass 존재 여부 확인 |
| `airflow-postgresql-0` CrashLoop | PVC 프로비저닝 실패. `kubectl get pvc -n airflow`로 상태 확인, StorageClass가 없으면 Step 1 재확인 |
| 웹서버 EXTERNAL-IP가 `<pending>` 지속 | 서브넷에 LoadBalancer 태그 누락 또는 퍼블릭 서브넷 부재. 포트포워딩으로 우회하거나 서브넷 태그 확인 |
| DAG가 UI에 안 보임 | `values.yaml`의 `dags.gitSync.repo`/`branch`/`subPath` 확인. git-sync 컨테이너 로그 확인 |
| 태스크 Pod가 `AccessDenied`(S3) | IRSA 미적용. Step 2의 서비스 계정 애노테이션 재확인 |

### 로그 확인 명령 모음

```bash
# 스케줄러 로그
kubectl logs -n airflow deploy/airflow-scheduler -c scheduler --tail=100

# 웹서버 로그
kubectl logs -n airflow deploy/airflow-webserver --tail=100

# 특정 Pod의 이벤트
kubectl describe pod <pod-name> -n airflow
```

---

## 구성 변경 및 삭제

설정을 바꾼 뒤 재적용:

```bash
helm upgrade airflow apache-airflow/airflow -n airflow -f values.yaml
```

Airflow 제거(클러스터는 유지):

```bash
helm uninstall airflow -n airflow
kubectl delete namespace airflow
```

---

## 다음 단계

Airflow가 준비되면 [데이터 수집 및 전처리 랩](../data-collection-preprocessing/README.md)으로 이동해
`KubernetesPodOperator`로 첫 파이프라인 단계를 EKS Pod에서 실행한다.

