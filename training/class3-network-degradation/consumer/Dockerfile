FROM python:3.10
WORKDIR /app
COPY . /app
RUN apt-get update && apt-get install -y iproute2 && pip install kafka-python
CMD ["python3", "-u", "consumer.py"]