FROM python:3.12.13-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml setup.py MANIFEST.in ./
COPY cpp ./cpp
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
COPY pairs.yml ./pairs.yml
EXPOSE 8000 9100
CMD ["pm", "stack", "--host", "0.0.0.0", "--port", "8000"]
