FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # For pdfplumber/Pillow
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    # For matplotlib (Agg backend)
    libfreetype6-dev \
    # OCR (optional but included for completeness)
    tesseract-ocr \
    tesseract-ocr-deu \
    # Cleanup
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir pytesseract

# Copy app
COPY app_v5_comprehensive.py ./app.py

# Create cache directory
RUN mkdir -p /tmp/isfp_cache
RUN mkdir -p /tmp/isfp_templates

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')" || exit 1

# Run with gunicorn (4 workers, 5-min timeout for large PDFs)
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "4", \
     "--timeout", "300", \
     "--max-requests", "1000", \
     "--max-requests-jitter", "50", \
     "app:app"]
