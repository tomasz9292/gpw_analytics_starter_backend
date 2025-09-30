FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY api /app/api
EXPOSE 8001
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8001"]
