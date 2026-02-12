FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app_v4_comprehensive.py .

EXPOSE 5000

CMD ["python", "app_v4_comprehensive.py"]
