FROM python:3.7.13-bullseye

RUN pip install \
    azure-identity==1.10.0 \
    azure-keyvault-secrets==4.5.1

WORKDIR /opt
COPY test.py /opt

RUN useradd appuser
# needed in this case to get access look through the folders
RUN chown -R appuser /opt
RUN chmod 755 /opt
USER appuser