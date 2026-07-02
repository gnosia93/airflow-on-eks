

* collect_data.py
 — UCI Wine Quality(red/white) 공개 데이터를 받아 합친 뒤 S3 raw/ 파티션에 업로드
* preprocess_data.py
 — 결측/중복 제거 → 라벨 생성(quality≥7) → 범주형 인코딩 → 표준화 → train/test 분할 → S3 processed/에 Parquet + scaler.pkl 저장
* data_collection_preprocessing_dag.py
 — collect_data >> preprocess_data 순서로 두 단계를 KubernetesPodOperator로 EKS Pod에서 실행하는 DAG

* docker/Dockerfile + requirements.txt — 태스크 실행용 컨테이너 이미지
* README.md — S3 생성, ECR 빌드/푸시, IRSA 서비스 계정, Variable 설정, 검증/트러블슈팅까지 담은 랩 가이드
