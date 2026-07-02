# airflow-on-eks

* [1. EKS 설치](https://github.com/gnosia93/airflow-on-eks/blob/main/lecture/1-eks-create.md)
* [2. airflow 설치](https://github.com/gnosia93/airflow-on-eks/blob/main/lecture/2-airflow-install.md)
* [3. 데이터 수집 및 전처리](https://github.com/gnosia93/airflow-on-eks/blob/main/lecture/3-preprocessing.md)
* [4. 모델학습](https://github.com/gnosia93/airflow-on-eks/blob/main/lecture/4-training.md)
* 5. 모델튜닝
* 6. 서빙
* [7. DAG 합치기](https://github.com/gnosia93/airflow-on-eks/blob/main/lecture/7-dag-combine.md)

MLOps 관점에서 중요한 포인트
워크샵 전체 플로우(수집 → 분석 → 전처리 → 훈련 → 튜닝 → 서빙)를 EKS 기반으로 돌리려면 설치 단계에서 미리 챙겨야 할 것들이 있어요.

* IRSA 설정: Airflow worker Pod가 S3(데이터 저장), ECR(모델 이미지) 등에 접근하려면 서비스 계정에 IAM 역할을 매핑해야 합니다.
* KubernetesPodOperator: 훈련/튜닝처럼 무거운 작업은 이 오퍼레이터로 별도 Pod(필요시 GPU 노드)에서 실행하는 게 핵심 패턴입니다.
* GPU 노드 그룹: 모델 훈련 실습이 있다면 g4dn 같은 GPU 노드 그룹을 별도로 추가하고 taint/toleration으로 격리하세요.
