# deploy/Dockerfile
# MA26-ATR 量化交易系统 v4.1.0 — 容器化部署
#
# 构建说明:
#   从项目根目录执行:
#   docker build -f deploy/Dockerfile -t ma26-atr:latest .
#
# 运行示例:
#   docker run -d \
#     --name ma26-atr-paper \
#     -e BINANCE__STRATEGY__MA_PERIOD=26 \
#     -e BINANCE__RISK__DAILY_LOSS_LIMIT=0.03 \
#     -v $(pwd)/config:/app/config \
#     -v $(pwd)/logs:/app/logs \
#     -v $(pwd)/data:/app/data \
#     ma26-atr:latest -m paper -c config/paper.yaml
#
# 多模式:
#   backtest: -m backtest -c config/backtest.yaml
#   live:     -m default -c config/default.yaml

# ═══════════════════════════════════════════════════════════════
# 阶段 1: 构建（编译依赖）
# ═══════════════════════════════════════════════════════════════
FROM python:3.11-slim AS builder

# 禁止 Python 生成 .pyc 文件，确保输出无缓冲
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安装编译工具（用于 numpy/pandas/ta-lib 等含 C 扩展的包）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    make \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 创建独立虚拟环境
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 拷贝依赖清单并安装
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt


# ═══════════════════════════════════════════════════════════════
# 阶段 2: 运行（最小镜像）
# ═══════════════════════════════════════════════════════════════
FROM python:3.11-slim AS runtime

LABEL maintainer="quant_team" \
      description="MA26-ATR Quantitative Trading System v4.1.0" \
      org.opencontainers.image.source="https://github.com/your-org/ma26-atr"

# 环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    CONFIG_DIR=/app/config \
    LOG_DIR=/app/logs \
    DATA_DIR=/app/data \
    PATH="/opt/venv/bin:$PATH"

# 安装运行时库（OpenMP 用于 numpy 并行，certificates 用于 HTTPS 请求）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    ca-certificates \
    tzdata \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -snf /usr/share/zoneinfo/UTC /etc/localtime && echo "UTC" > /etc/timezone

# 从 builder 复制虚拟环境
COPY --from=builder /opt/venv /opt/venv

# 创建非 root 用户（安全最佳实践）
RUN useradd -m -r -s /bin/bash -u 1000 trader && \
    mkdir -p /app /app/logs /app/data /app/config && \
    chown -R trader:trader /app

# 设置工作目录
WORKDIR /app

# 拷贝项目文件（只拷贝运行时必需的文件）
COPY src /app/src
COPY config /app/config
COPY bootstrap.py /app/bootstrap.py
COPY tests /app/tests

# 设置权限
RUN chown -R trader:trader /app

# 切换至非 root 用户
USER trader

# 声明数据卷
VOLUME ["/app/logs", "/app/data", "/app/config"]

# 预留监控/Webhook 端口（如启用 Prometheus 或管理 API）
EXPOSE 8080

# 健康检查（可选 — 如果系统实现了 HTTP /health 端点则取消注释）
# HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
#   CMD python -c 'import urllib.request; urllib.request.urlopen("http://localhost:8080/health")' || exit 1

# 默认启动参数（可被 docker run 覆盖）
# 使用 ENTRYPOINT + CMD 模式，允许追加参数
ENTRYPOINT ["python", "bootstrap.py"]
CMD ["-m", "paper", "-c", "config/paper.yaml"]
