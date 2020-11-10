FROM python:3.8
COPY requirements.txt /app/

WORKDIR /app
RUN pip install -r requirements.txt
RUN pip freeze

RUN mkdir -p /app/dataset/
COPY . /app/dataset/

ENV DATASET_DIR=/data
ENTRYPOINT ["/app/dataset/entrypoint.sh"]
