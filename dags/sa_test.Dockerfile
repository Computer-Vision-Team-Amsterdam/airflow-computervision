#  az acr build -t sa_test:latest -f sa_test.Dockerfile -r cvtweuacrogidgmnhwma3zq .
FROM python:3.7.13-bullseye

RUN pip install \
    azure-identity==1.10.0 \
    azure-keyvault-secrets==4.5.1 \
    azure-storage-blob==12.13.1

WORKDIR /opt


COPY sa_test.py /opt