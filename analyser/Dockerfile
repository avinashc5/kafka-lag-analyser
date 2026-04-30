FROM python:3.10

RUN pip install kafka-python numpy matplotlib pandas xgboost scikit-learn

WORKDIR /app
COPY analyser.py .

CMD ["python3", "-u", "analyser.py"]