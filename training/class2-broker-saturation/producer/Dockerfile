FROM python:3.10

WORKDIR /app
COPY . /app

RUN pip install kafka-python flask

CMD ["python3", "-u", "producer.py"]
