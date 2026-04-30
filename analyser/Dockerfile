FROM python:3.13-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY scraper/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install fastapi uvicorn

COPY analyser/ /app/analyser/
COPY ml/ /app/ml/

EXPOSE 8000

CMD ["uvicorn", "analyser.analyser:app", "--host", "0.0.0.0", "--port", "8000"]
