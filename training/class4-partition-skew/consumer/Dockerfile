FROM python:3.10

WORKDIR /app
COPY . /app

RUN pip install kafka-python

CMD ["python3", "-u", "consumer.py"]
