# Lab: 하이퍼파라미터 튜닝 (EKS Pod 병렬 실행)

여러 하이퍼파라미터 조합을 EKS Pod에서 병렬로 학습하고, 최적 모델을 자동으로 선택해
프로덕션 경로로 승격한다. Airflow의 동적 태스크 매핑(dynamic task mapping)을 활용한다.
SageMaker는 사용하지 않는다.

## 학습 목표

- Airflow 동적 태스크 매핑(`.expand()`)으로 N개의 학습 시도를 병렬 Pod로 실행한다.
- 그리드 서치로 하이퍼파라미터 공간을 탐색하고, 시도별 결과를 S3에 저장한다.
- 팬인(fan-in) 태스크로 최적 모델을 선택·승격하는 패턴을 익힌다.

## 파이프라인 흐름

```
              ┌─ tune_train[trial-0] (Pod) ─┐
(processed)   ├─ tune_train[trial-1] (Pod) ─┤
 train/test ──┼─ tune_train[trial-2] (Pod) ─┼──► select_best (Pod) ──► models/dt=.../
              ├─ ...                        ─┤        (팬인)            model.pkl (승격)
              └─ tune_train[trial-N] (Pod) ─┘                          best_params.json
                                                                       leaderboard.json
```

`SEARCH_SPACE`(n_estimators×max_depth×learning_rate)의 모든 조합이 각각 하나의 Pod로 병렬 실행된다.
기본 설정은 2×2×2 = **8개 조합**이다.

## 사전 요구사항

- [데이터 수집·전처리 랩](../data-collection-preprocessing/README.md)이 완료되어 processed 데이터가 S3에 있어야 한다.
- 셸 환경 변수:

```bash
export AWS_REGION=ap-northeast-2
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export S3_BUCKET=my-mlops-workshop-bucket
```

---

## Step 1. 튜닝 스크립트를 이미지에 포함

수집/전처리와 동일한 `wine-mlops` 이미지를 재사용한다. 두 스크립트를 이미지에 추가 후 재빌드/푸시한다.

```bash
cp scripts/tune_train.py scripts/select_best.py \
   ../data-collection-preprocessing/scripts/

cd ../data-collection-preprocessing
docker build -t wine-mlops:latest -f docker/Dockerfile .
docker tag wine-mlops:latest \
  "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"
docker push "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/wine-mlops:latest"
cd -
```

> 추가 의존성이 없어(scikit-learn 포함) `requirements.txt` 변경 없이 재빌드만 하면 된다.

## Step 2. 병렬 실행 한도 확인 (선택)

8개 Pod가 동시에 뜨면 `workers` 노드 그룹(m5.xlarge x3)의 리소스를 사용한다.
동시 실행 수를 제한하려면 DAG의 `tune_train`에 `.expand(...)` 뒤로 max_active_tis_per_dag를
조정하거나, 노드 그룹을 스케일업한다. 워크숍 기본값(8 trials)은 3노드로 충분하다.

## Step 3. DAG 배포 및 실행

`dags/hyperparameter_tuning_dag.py`를 DAG 저장소에 push(git-sync)하거나 dags 경로에 복사한다.
Airflow UI에서 `hyperparameter_tuning` DAG를 활성화하고 **Trigger DAG**를 클릭한다.

---

## Validation (검증)

### 1. 매핑된 태스크 병렬 실행 확인

Airflow UI의 Grid 뷰에서 `tune_train`이 여러 개의 매핑 인스턴스(`[0] [1] ... [7]`)로
펼쳐져 병렬 실행되는지 확인한다.

```bash
kubectl get pods -n airflow | grep tune-train
```

기대 결과: 여러 개의 `tune-train-...` Pod가 동시에 실행/완료.

### 2. 시도별 결과 확인

```bash
aws s3 ls "s3://${S3_BUCKET}/wine-quality/tuning/" --recursive
```

기대 결과: 각 `trials/trial-N/` 아래 `model.pkl`, `metrics.json` 존재.

### 3. 최적 모델 승격 및 리더보드 확인

```bash
# 최적 하이퍼파라미터
aws s3 cp "s3://${S3_BUCKET}/wine-quality/models/dt=$(date +%F)/best_params.json" - | cat

# 리더보드 (모든 시도 순위)
aws s3 cp "s3://${S3_BUCKET}/wine-quality/models/dt=$(date +%F)/leaderboard.json" - | cat
```

기대 결과: 승격된 `model.pkl`과 함께, roc_auc 기준 정렬된 리더보드가 표시된다.

### 4. select_best 로그 확인

태스크 로그 마지막 줄:

```
[select] DONE. best=trial-3 roc_auc=0.91 params={'n_estimators': 300, 'max_depth': 6, 'learning_rate': 0.1}
```

---

## 문제 해결

| 증상 | 원인 / 대응 |
|------|-------------|
| 매핑 태스크가 순차 실행 | 병렬 슬롯 부족. Airflow `parallelism` / `max_active_tasks` 설정, 노드 리소스 확인 |
| 일부 trial Pod `Pending` | 노드 리소스 부족. `workers` 노드 그룹 스케일업 또는 동시 실행 수 제한 |
| `select_best`가 `시도 결과가 없습니다` | tune_train이 모두 실패했거나 `DATA_DATE` 파티션 불일치. 시도 로그와 파티션 확인 |
| `NoSuchKey` (processed) | 전처리 랩을 먼저 실행 |

---

## 다음 단계

승격된 `model.pkl`(models/dt=.../)과 전처리의 `scaler.pkl`을 함께 로드하여,
모델 서빙 랩에서 EKS 위에 추론 엔드포인트(FastAPI + Deployment/Service)를 배포한다.

