#/bin/bash

# this script will setup the environment and install all necessary components 

# install/upgrade virtualenv
python3.7 -m pip install --upgrade virtualenv

# create and run a python3.7 virtual env
python3.7 -m venv venv
source venv/bin/activate
# install/upgrade pip
python3.7 -m pip install --upgrade pip setuptools wheel

# install Airflow in the virtual env
AIRFLOW_VERSION=2.3.2
PYTHON_VERSION=3.7
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# pip install pypi packages
pip install -r requirements.txt