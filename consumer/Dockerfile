FROM python:3.10

RUN pip install kafka-python

WORKDIR /app
COPY consumer.py .

CMD ["python3", "-u", "consumer.py"]