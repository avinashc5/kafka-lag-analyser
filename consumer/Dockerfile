FROM python:3.10

WORKDIR /app
COPY consumer.py .

RUN pip install kafka-python

CMD ["python3", "-u", "consumer.py"]