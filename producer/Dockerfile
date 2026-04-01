FROM python:3.10

WORKDIR /app
COPY producer.py .

RUN pip install kafka-python

CMD ["python3", "-u", "producer.py"]