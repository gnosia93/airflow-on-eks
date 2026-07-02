## Airflow 설치 ##

공식 차트 저장소 추가
```
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

네임스페이스 생성
```
kubectl create namespace airflow
```

values.yaml을 만들어 executor와 리소스를 지정합니다. MLOps 파이프라인이라면 KubernetesExecutor를 추천합니다. 각 태스크가 독립된 Pod로 떠서 리소스 격리와 확장에 유리합니다.
```
# values.yaml
executor: "KubernetesExecutor"

# 웹서버 접근용 (실습에서는 LoadBalancer, 운영은 Ingress+ALB 권장)
webserver:
  service:
    type: LoadBalancer

# 메타데이터 DB - 실습은 내장 PostgreSQL, 운영은 외부 RDS 권장
postgresql:
  enabled: true

# DAG를 어떻게 넣을지 (아래 4번 참고)
dags:
  gitSync:
    enabled: true
    repo: https://github.com/<your-org>/<your-dags-repo>.git
    branch: main
    subPath: "dags"
```

설치:
```
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  -f values.yaml \
  --timeout 10m
```

4. 접속 확인
```
kubectl get pods -n airflow
kubectl get svc -n airflow airflow-webserver
```
LoadBalancer의 EXTERNAL-IP로 접속 후 기본 계정 admin/admin으로 로그인합니다.

kubectl get pods -n airflow
kubectl get svc -n airflow airflow-webserver
LoadBalancer의 EXTERNAL-IP로 접속 후 기본 계정 admin/admin으로 로그인합니다.
