FROM python:3.10

RUN pip install kafka-python

WORKDIR /app
COPY producer.py .

CMD ["python3", "-u", "producer.py"]