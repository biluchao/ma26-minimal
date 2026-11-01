FROM python:3.11-slim AS base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM base AS test
COPY . .
RUN python tests/unit/test_core.py \
 && python tests/unit/test_modules.py \
 && python tests/integration/test_backtest.py

FROM base AS runtime
COPY src/ ./src/
COPY config/ ./config/
RUN mkdir -p snapshots_live traces && chmod 700 snapshots_live traces
VOLUME ["/app/snapshots_live", "/app/traces"]
HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.path.insert(0,'src'); \
from core.event_bus import EventBus; print('ok')" || exit 1
ENTRYPOINT ["python", "src/bootstrap.py"]
CMD ["--mode", "live", "--config", "config/live.yaml"]

