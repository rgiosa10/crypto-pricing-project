# Who's Bitcoin price prediction do you agree with Google Vertex AI or ChatGPT?

#### By [Ruben Giosa](https://www.linkedin.com/in/rubengiosa/)

#### This repo showcases ETL pipeline for cleaning up historical Bitcoin price data, and webscraping new data in order to get some fun predictions using Vertex AI and ChatGPT. 

<img src="imgs/team_week3_readme.png" alt="sample of work" width="750"/>

<br>

## Technologies Used

* Python
* Jupyter
* Airflow
* BigQuery
* Looker Studio
* ChatGPT API
* Pandas
* Git
* Markdown
* `.gitignore`
* `requirements.txt`
  
</br>

## Datasets Used

1. [Bitcoin Stock Data](https://www.kaggle.com/datasets/deepakvedantam/bitcoin-stock-data)
2. [Investing.com Bitcoin historical data](https://www.investing.com/crypto/bitcoin/historical-data)
3. [Crypto pricing from Yahoo! Finance](https://finance.yahoo.com/crypto/)


</br>

## Description

[Ruben](https://www.linkedin.com/in/rubengiosa/) created a ETL pipeline leveraging Airflow to orchestrate profiling, cleaning, transformations, and loading of data into BigQuery on the below data: 
  * 

The DAG is triggered once the data files are detected in the data directory using a `FileSensor`. Once the transformations are completed these are complied into three `Parquet` files. Upon completion, the dataset is created in BigQuery, where the the stocks and M2 Supply files are then loaded as tables. A `BigQueryTableExistenceSensor` is then used to ensure that the `m2_supply` table is loaded, which then kicks-off the final step of the loading of the `gas` table to BigQuery. Ruben also owned and authored the `README.md`. Below is the DAG of the above pipeline:

<img src="imgs/rg_dag.png" alt="Airflow dag" width="750"/>


<br>

## Data Visualizations:
Once the datasets were cleaned and consolidated, I created data visualizations and analysis (using Looker Studio).

Below is a line graph that was put together by [Ruben](https://www.linkedin.com/in/rubengiosa/) that allows a user to look at the highest tech stock and Bitcoin prices by year (click on image of chart to use dashboard), which leverages the data from the `stocks` table:


<br>


## Setup/Installation Requirements

* Go to https://github.com/rgiosa10/crypto-pricing-project.git to find the specific repository for this website.
* Then open your terminal. I recommend going to your Desktop directory:
    ```bash
    cd Desktop
    ```
* Then clone the repository by inputting: 
  ```bash
  git clone https://github.com/rgiosa10/crypto-pricing-project.git
  ```
* Go to the new directory or open the directory folder on your desktop:
  ```bash
  cd team-week3
  ```
* Once in the directory you will need to set up a virtual environment in your terminal:
  ```bash
  python3.7 -m venv venv
  ```
* Then activate the environment:
  ```bash
  source venv/bin/activate
  ```
* Install the necessary items with requirements.txt:
  ```bash
    pip install -r requirements.txt
  ```
* Download the necessary csv files listed in the Datasets Used section
* With your virtual environment now enabled with proper requirements, open the directory:
  ```bash
  code .
  ```
* Upon launch please update the Google Cloud client and project details to configure it to load to your project

* Once VS Code is open, then run the setup file:
  ```bash
  ./setup.sh
  ```

    The contents of the `setup.sh` include the below to install:

    1. Relevant version of python
    2. Create virtual env
    3. Installing Airflow in virtual env
    4. Requirements.txt

    ```bash
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
    ```

* Then run the airflow setup file:

  ```bash
  ./airflow_setup.sh
  ```
    
    The contents of the `airflow_setup.sh` include the below to:

    1. Creating ./logs and ./plugins directories in the dsa-airflow directory 
    2. Download the `docker_compose.yaml` 
    3. Create the .env 
    4. Initialize airflow
    
```bash
    #!/bin/bash
    # Move into the dsa-airflow directory and make subdirs
    cd dsa-airflow

    # download the docker-compose.yaml and set the .env
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
    echo "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env


    # initialize airflow 
    docker-compose up airflow-init
```

* Once airflow has been initialized, use the below command line tool that allows you to initialize the rest of the Docker containers:
        ```bash
        docker-compose up
        ```

* You will need to create a file connection for the `data/` folder. To do so go to the airflow GUI and click Admin -> Connections and then create a new connection with the below config and click save:

    <img src="imgs/conn_setup.png" alt="connection setup" width="640"/>


* You will need to create a cloud connection for the `BigQueryTableExistenceSensor` folder to work:
    * Connection Id: google-cloud-default
    * Connection Type: Google BigQuery

* Once this is all setup, in the Airflow GUI 1) enable your DAG and 2) trigger it to run. From there go to your VS Code and run the below command from inside the data directory:

    ```bash
    ./get_data.sh
    ```
This will download the CSV file to your local filesystem in the data folder, which will trigger the file sensor and start the DAG.

* Once setups have been completed, you will want to be using the below commands to manage airflow and docker:
    
    1. In order to shut down hit `^Ctrl C` to stop Airflow on the local host and then run the below to stop the containers and remove old volumes:
        ```bash
        docker-compose down --volumes --remove-orphans 
        ```
    2. Use the below command line tool if you want to re-initialize the rest of the Docker containers:
        ```bash
        docker-compose up
        ```

</br>

## Known Bugs

* No known bugs

<br>

## License

MIT License

Copyright (c) 2022 Ruben Giosa, Philip Kendal, Chloe (Yen Chi) Le

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

</br>
