FROM python:3.12-slim

# ffmpeg lets yt-dlp merge separate audio/video streams when TikTok
# serves them separately. Koyeb version deliberately skipped it;
# on Fly we have a real Docker image, so bake it in.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

# Fly http_service internal_port is 8080. The app itself serves
# /health and /webhook via Flask/gunicorn, exactly as on Koyeb.
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "180"]
