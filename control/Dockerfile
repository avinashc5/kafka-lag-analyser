FROM python:3.13-slim

WORKDIR /app

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir fastapi uvicorn python-multipart

COPY control/ /app/control/

EXPOSE 8001

CMD ["uvicorn", "control.control:app", "--host", "0.0.0.0", "--port", "8001"]
