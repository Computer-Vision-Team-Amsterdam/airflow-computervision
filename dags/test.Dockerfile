FROM python:3.7.13-bullseye

RUN pip install \
    azure-identity==1.10.0 \
    azure-keyvault-secrets==4.5.1

WORKDIR /opt

# creating the file to write XComs to
RUN mkdir -p airflow/xcom
RUN echo "" > airflow/xcom/return.json
COPY test.py /opt

