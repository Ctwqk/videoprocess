FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY feature_aggregator ./feature_aggregator
COPY schemas ./schemas

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir .

EXPOSE 8080

CMD ["uvicorn", "feature_aggregator.main:app", "--host", "0.0.0.0", "--port", "8080"]
