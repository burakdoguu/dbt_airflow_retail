FROM quay.io/astronomer/astro-runtime:8.4.0



FROM apache/airflow:2.4.2

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
ENV PATH $PATH:$JAVA_HOME/bin

USER airflow

COPY requirements.txt .

RUN python -m venv dbt_venv && . dbt_venv/bin/activate

RUN pip install --no-cache-dir -r requirements.txt
