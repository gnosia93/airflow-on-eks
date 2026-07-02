
# Lab: Amazon EKS 클러스터 프로비저닝

MLOps 워크숍 실습의 기반이 되는 Amazon EKS 클러스터를 생성한다. 이 클러스터 위에서
이후 모든 워크로드(Airflow, 데이터 파이프라인, 모델 학습/서빙)를 실행한다.

## 학습 목표

- `eksctl`로 EKS 클러스터와 노드 그룹을 선언적으로 프로비저닝한다.
- 일반 워크로드용 CPU 노드 그룹과 학습용 GPU 노드 그룹의 역할을 구분한다.
- IRSA를 위한 OIDC 공급자를 활성화한다.
- 클러스터 노드 상태를 검증한다.

## 사전 요구사항

| 도구 | 최소 버전 | 확인 명령 |
|------|-----------|-----------|
| AWS CLI | 2.x | `aws --version` |
| eksctl | 0.180+ | `eksctl version` |
| kubectl | 1.28+ | `kubectl version --client` |

- AWS 자격증명이 구성되어 있어야 한다: `aws sts get-caller-identity`
- 필요한 IAM 권한: EKS/EC2/CloudFormation/IAM 리소스 생성 권한 (실습은 관리자 권한 권장).

```bash
export AWS_REGION=ap-northeast-2
export CLUSTER_NAME=airflow-mlops

aws sts get-caller-identity
```

기대 결과: 계정 ID, 사용자/역할 ARN이 출력된다.

---

## 클러스터 구성 개요

같은 디렉터리의 `cluster.yaml`이 클러스터를 정의한다.

| 노드 그룹 | 인스턴스 | 노드 수 (min/desired/max) | 용도 |
|-----------|----------|---------------------------|------|
| `workers` | m5.xlarge (4 vCPU/16GiB) | 2 / 3 / 5 | Airflow, 수집·분석·전처리 태스크 |
| `gpu-workers` | g4dn.xlarge (T4 GPU) | 0 / 0 / 2 | 모델 학습·튜닝 (필요 시에만 기동) |

- GPU 노드 그룹은 `desiredCapacity: 0`으로 두어 평소 비용이 들지 않는다. 학습 태스크가 GPU를 요청하면 스케일업된다.
- GPU 노드에는 `nvidia.com/gpu` taint가 있어 일반 Pod가 스케줄되지 않는다. GPU 태스크는 toleration을 명시해야 한다.
- `iam.withOIDC: true`로 IRSA를 위한 OIDC 공급자를 활성화한다.

---

## Step 1. 클러스터 생성

```bash
eksctl create cluster -f cluster.yaml
```

- 소요 시간: 약 15~20분 (CloudFormation 스택 2개 이상 생성).
- 완료되면 `kubectl` 컨텍스트가 자동으로 새 클러스터로 설정된다.

진행 상황을 별도 터미널에서 확인:

```bash
eksctl get cluster --region "${AWS_REGION}"
```

---

## Step 2. kubeconfig 갱신 (필요 시)

자동 설정이 안 됐거나 다른 환경에서 접속할 때:

```bash
aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${AWS_REGION}"
kubectl config current-context
```

---

## Validation (검증)

### 1. 노드 상태 확인

```bash
kubectl get nodes -o wide
```

기대 결과: `workers` 노드 그룹의 노드 3개가 `Ready` 상태.

```
NAME                          STATUS   ROLES    AGE   VERSION
ip-192-168-xx-xx.ap-...       Ready    <none>   5m    v1.30.x
ip-192-168-yy-yy.ap-...       Ready    <none>   5m    v1.30.x
ip-192-168-zz-zz.ap-...       Ready    <none>   5m    v1.30.x
```

> GPU 노드 그룹은 `desiredCapacity: 0`이므로 이 시점에는 노드가 없다. 정상이다.

### 2. 노드 그룹 확인

```bash
eksctl get nodegroup --cluster "${CLUSTER_NAME}" --region "${AWS_REGION}"
```

기대 결과: `workers`(3 노드)와 `gpu-workers`(0 노드) 두 그룹이 출력된다.

### 3. OIDC 공급자 확인

```bash
aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" \
  --query "cluster.identity.oidc.issuer" --output text
```

기대 결과: `https://oidc.eks.ap-northeast-2.amazonaws.com/id/...` 형태의 URL.

### 4. 시스템 Pod 확인

```bash
kubectl get pods -n kube-system
```

기대 결과: `coredns`, `aws-node`, `kube-proxy` Pod가 모두 `Running`.

---

## 문제 해결

| 증상 | 원인 / 대응 |
|------|-------------|
| `Error: ... AlreadyExistsException` | 동일 이름의 CloudFormation 스택 잔존. `eksctl delete cluster --name ${CLUSTER_NAME}` 후 재시도 |
| 노드가 `NotReady` 지속 | CNI(aws-node) 문제 가능. `kubectl describe node`와 `kubectl logs -n kube-system -l k8s-app=aws-node` 확인 |
| `UnauthorizedOperation` / `AccessDenied` | 실행 IAM 주체 권한 부족. EKS/EC2/IAM/CloudFormation 권한 확인 |
| 서브넷/VPC 한도 초과 | 리전의 VPC·EIP 한도 초과. 미사용 VPC 정리 또는 한도 증설 요청 |
| GPU 태스크가 `Pending` | Cluster Autoscaler 미설치 또는 NVIDIA device plugin 미설치. 학습 랩에서 별도 설치 |

CloudFormation 콘솔 또는 CLI로 스택 이벤트를 보면 실패 원인을 정확히 알 수 있다:

```bash
aws cloudformation describe-stack-events \
  --stack-name "eksctl-${CLUSTER_NAME}-cluster" \
  --region "${AWS_REGION}" --max-items 20
```

---

## 리소스 정리 (워크숍 종료 시)

클러스터와 모든 노드 그룹을 삭제한다.

```bash
eksctl delete cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}"
```

확인:

```bash
eksctl get cluster --region "${AWS_REGION}"
```

기대 결과: `airflow-mlops` 클러스터가 목록에 없다.

> 클러스터를 지워도 S3 버킷, ECR 리포지토리는 남는다. 해당 리소스는 각 랩의 정리 절차 또는 전체 Cleanup 섹션에서 별도로 삭제한다.

---

## 다음 단계

클러스터가 `Ready` 되면 [Airflow 설치 랩](../airflow-installation/README.md)으로 이동한다.
