FROM bitnami/spark:latest


WORKDIR /opt/spark

USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/

COPY ../requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

# set back to non-root for best practice)
USER 1001