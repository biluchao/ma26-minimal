# deploy/Makefile
# MA26-ATR 量化交易系统 v4.1.0 — 构建与部署
#
# 用法:
#   make build             构建 Docker 镜像
#   make run-backtest      运行回测模式
#   make run-paper         运行模拟盘模式
#   make run-live          运行实盘模式
#   make run-local         本地直接运行（不依赖 Docker）
#   make test              运行测试套件
#   make clean             清理容器和镜像
#   make logs              查看运行中的容器日志
#   make shell             进入容器 shell 调试
#   make push              推送镜像到远程仓库

.PHONY: help build run-backtest run-paper run-live run-local test test-verbose clean logs shell push

# ═══════════════════════════════════════════════════════════════
# 变量定义
# ═══════════════════════════════════════════════════════════════

IMAGE_NAME       ?= ma26-atr
IMAGE_TAG        ?= 4.1.0
FULL_IMAGE       := $(IMAGE_NAME):$(IMAGE_TAG)
DOCKER_REGISTRY  ?= your-registry.com/quant

CONTAINER_PREFIX ?= ma26-atr
CONFIG_DIR       ?= $(PWD)/config
LOG_DIR          ?= $(PWD)/logs
DATA_DIR         ?= $(PWD)/data

PYTHON           ?= python3
PIP              ?= pip3
DOCKER           ?= docker

# ═══════════════════════════════════════════════════════════════
# 帮助
# ═══════════════════════════════════════════════════════════════

help:
	@echo "╔══════════════════════════════════════════════════════════╗"
	@echo "║   MA26-ATR 量化交易系统 v4.1.0 — 构建与部署工具          ║"
	@echo "╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "构建:"
	@echo "  make build              构建 Docker 镜像"
	@echo ""
	@echo "运行 (Docker):"
	@echo "  make run-backtest       回测模式 (config/backtest.yaml)"
	@echo "  make run-paper          模拟盘模式 (config/paper.yaml)"
	@echo "  make run-live           实盘模式 (config/default.yaml)"
	@echo ""
	@echo "运行 (本地):"
	@echo "  make run-local          本地 Python 直接运行"
	@echo "  make run-local-backtest 本地回测"
	@echo ""
	@echo "测试:"
	@echo "  make test               运行全部测试"
	@echo "  make test-verbose       详细输出测试"
	@echo "  make test-core          仅运行 core 测试"
	@echo "  make test-integration   仅运行集成测试"
	@echo ""
	@echo "维护:"
	@echo "  make logs               查看容器日志"
	@echo "  make shell              进入容器 shell"
	@echo "  make clean              清理容器和镜像"
	@echo "  make push               推送镜像到仓库"
	@echo ""
	@echo "变量覆盖:"
	@echo "  make build IMAGE_TAG=4.2.0"
	@echo "  make run-paper CONFIG_DIR=/path/to/config"

# ═══════════════════════════════════════════════════════════════
# 构建
# ═══════════════════════════════════════════════════════════════

build:
	@echo "🔨 构建镜像 $(FULL_IMAGE) ..."
	$(DOCKER) build -f deploy/Dockerfile -t $(FULL_IMAGE) .
	@echo "✅ 构建完成: $(FULL_IMAGE)"

# ═══════════════════════════════════════════════════════════════
# 运行 (Docker)
# ═══════════════════════════════════════════════════════════════

run-backtest: build
	@echo "📊 启动回测模式..."
	$(DOCKER) run --rm \
		--name $(CONTAINER_PREFIX)-backtest \
		-v $(CONFIG_DIR):/app/config \
		-v $(LOG_DIR):/app/logs \
		-v $(DATA_DIR):/app/data \
		$(FULL_IMAGE) -m backtest -c config/backtest.yaml

run-paper: build
	@echo "📝 启动模拟盘模式..."
	$(DOCKER) run -d \
		--name $(CONTAINER_PREFIX)-paper \
		--restart unless-stopped \
		-v $(CONFIG_DIR):/app/config \
		-v $(LOG_DIR):/app/logs \
		-v $(DATA_DIR):/app/data \
		$(FULL_IMAGE) -m paper -c config/paper.yaml
	@echo "✅ 模拟盘已启动 (容器名: $(CONTAINER_PREFIX)-paper)"
	@echo "   查看日志: make logs"

run-live: build
	@echo "🚀 启动实盘模式..."
	$(DOCKER) run -d \
		--name $(CONTAINER_PREFIX)-live \
		--restart unless-stopped \
		-v $(CONFIG_DIR):/app/config \
		-v $(LOG_DIR):/app/logs \
		-v $(DATA_DIR):/app/data \
		$(FULL_IMAGE) -m default -c config/default.yaml
	@echo "✅ 实盘已启动 (容器名: $(CONTAINER_PREFIX)-live)"

# ═══════════════════════════════════════════════════════════════
# 运行 (本地 Python)
# ═══════════════════════════════════════════════════════════════

run-local:
	@echo "🏃 本地运行..."
	$(PYTHON) bootstrap.py -m paper -c config/paper.yaml

run-local-backtest:
	@echo "🏃 本地回测..."
	$(PYTHON) bootstrap.py -m backtest -c config/backtest.yaml

# ═══════════════════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════════════════

test:
	@echo "🧪 运行测试套件..."
	$(PYTHON) -m pytest tests/ -v --tb=short

test-verbose:
	@echo "🧪 运行测试套件 (详细模式)..."
	$(PYTHON) -m pytest tests/ -vv --tb=long -s

test-core:
	@echo "🧪 运行 Core 测试..."
	$(PYTHON) -m pytest tests/test_core.py -v

test-modules:
	@echo "🧪 运行 Modules 测试..."
	$(PYTHON) -m pytest tests/test_modules.py -v

test-integration:
	@echo "🧪 运行集成测试..."
	$(PYTHON) -m pytest tests/test_integration.py -v

test-docker: build
	@echo "🧪 Docker 内运行测试..."
	$(DOCKER) run --rm \
		-v $(PWD)/tests:/app/tests \
		$(FULL_IMAGE) python -m pytest tests/ -v

# ═══════════════════════════════════════════════════════════════
# 维护
# ═══════════════════════════════════════════════════════════════

logs:
	@echo "📋 查看日志 ($(CONTAINER_PREFIX)-paper)..."
	$(DOCKER) logs -f $(CONTAINER_PREFIX)-paper 2>&1

logs-live:
	@echo "📋 查看实盘日志..."
	$(DOCKER) logs -f $(CONTAINER_PREFIX)-live 2>&1

shell:
	@echo "🐚 进入容器 shell..."
	$(DOCKER) run --rm -it \
		-v $(CONFIG_DIR):/app/config \
		-v $(LOG_DIR):/app/logs \
		-v $(DATA_DIR):/app/data \
		--entrypoint /bin/bash \
		$(FULL_IMAGE)

clean:
	@echo "🧹 清理容器和镜像..."
	$(DOCKER) rm -f $(CONTAINER_PREFIX)-paper $(CONTAINER_PREFIX)-live $(CONTAINER_PREFIX)-backtest 2>/dev/null || true
	$(DOCKER) rmi $(FULL_IMAGE) 2>/dev/null || true
	@echo "✅ 清理完成"

clean-all: clean
	@echo "🧹 深度清理..."
	$(DOCKER) system prune -f
	rm -rf $(LOG_DIR)/* $(DATA_DIR)/*

# ═══════════════════════════════════════════════════════════════
# 依赖管理
# ═══════════════════════════════════════════════════════════════

deps:
	@echo "📦 安装依赖..."
	$(PIP) install -r requirements.txt

deps-update:
	@echo "📦 更新依赖..."
	$(PIP) install --upgrade -r requirements.txt

# ═══════════════════════════════════════════════════════════════
# 推送
# ═══════════════════════════════════════════════════════════════

push:
	@echo "📤 推送镜像到 $(DOCKER_REGISTRY)/$(FULL_IMAGE) ..."
	$(DOCKER) tag $(FULL_IMAGE) $(DOCKER_REGISTRY)/$(FULL_IMAGE)
	$(DOCKER) push $(DOCKER_REGISTRY)/$(FULL_IMAGE)

push-latest: push
	$(DOCKER) tag $(FULL_IMAGE) $(DOCKER_REGISTRY)/$(IMAGE_NAME):latest
	$(DOCKER) push $(DOCKER_REGISTRY)/$(IMAGE_NAME):latest
