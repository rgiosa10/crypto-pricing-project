#!/bing/bash

cd dsa-airflow

# download the docker-compose.yaml and set the .env
#curl -Lf 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml' > docker-compose.yaml
echo "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

## MOUNT YOUR DATA AND CREDENTIALS DIRECTORY
# Update the volumes section to include these two drectories:
#   volumes:
#     - ./dags:/opt/airflow/dags
#     - ./logs:/opt/airflow/logs
#     - ./plugins:/opt/airflow/plugins
#     - ./data:/data
#     - ***YOUR_GOOGLE_CREDS_FOLDER***:/google_creds
#
#
# Update the environtment section wth a path to the credentials you just mounted:
#   environment:
#     &airflow-common-env
#     AIRFLOW__CORE__EXECUTOR: CeleryExecutor
#     AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
#     AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
#     AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
#     AIRFLOW__CORE__FERNET_KEY: ''
#     AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
#     AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
#     AIRFLOW__API__AUTH_BACKEND: 'airflow.api.auth.backend.basic_auth'
#     GOOGLE_APPLICATION_CREDENTIALS: /google_creds/***YOUR_CREDS***.json
#
#
# NOTE: Run the airflow-init before running `docker-compose up`:
docker-compose up airflow-init
#