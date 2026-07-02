# 데이터 수집/전처리 태스크를 실행하는 컨테이너 이미지.
# 이 이미지는 ECR에 푸시된 뒤, Airflow의 KubernetesPodOperator가 EKS 위에서 Pod로 실행한다.
FROM python:3.11-slim

# 파이썬 출력 버퍼링 비활성화 (Airflow 로그 실시간 확인용)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 스크립트 복사 (빌드 컨텍스트 기준 상위 scripts 디렉터리)
COPY scripts/ /app/scripts/

# 기본 진입점 없음 - KubernetesPodOperator가 cmds/arguments로 실행 스크립트를 지정한다.
