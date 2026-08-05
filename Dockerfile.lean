FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV HF_ENDPOINT=https://hf-mirror.com
ENV PADDLE_PDX_MODEL_SOURCE=BOS
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

# apt 源切换为清华镜像（本机访问 deb.debian.org 极慢）
RUN sed -i 's@deb.debian.org@mirrors.tuna.tsinghua.edu.cn@g; s@security.debian.org@mirrors.tuna.tsinghua.edu.cn@g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# torch 固定 2.5.1：其 CUDA 依赖为 cu12 系，与 paddle 3.3.0 的 cu13 精确 pin 不冲突
# （torch 2.13 会与 paddle 争抢 nvidia-cudnn-cu13 版本导致装不上）。
# --no-deps + 手动补齐 CPU 运行所需小依赖；无 CUDA 时嵌入服务自动回退 CPU 推理。
RUN pip install --no-cache-dir torch==2.5.1 --no-deps \
        -i https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install --no-cache-dir filelock typing-extensions sympy networkx jinja2 fsspec \
        -i https://pypi.tuna.tsinghua.edu.cn/simple

# 应用依赖：PyPI 走清华镜像（快），paddle 官方包走 BOS 索引
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --extra-index-url https://www.paddlepaddle.org.cn/packages/stable/cu130/

# GPU 版 Paddle 最后强制安装，确保依赖解析过程中不会把 CPU 版覆盖回来
RUN pip install --no-cache-dir --force-reinstall \
    -i https://www.paddlepaddle.org.cn/packages/stable/cu130/ \
    paddlepaddle-gpu==3.3.0

# BGE 向量模型真实权重已预先放入 models/bge-small-zh-v1.5/，随 COPY . 一并打入镜像
COPY . /app
WORKDIR /app

EXPOSE 8080
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
