FROM quay.io/astronomer/astro-runtime:6.0.2

COPY requirements.txt .

COPY resources/salary.csv /usr/local/airflow/salary.csv

RUN pip install --no-cache-dir -q -r requirements.txt
