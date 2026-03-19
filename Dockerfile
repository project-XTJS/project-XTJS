ARG PADDLE_BASE_IMAGE=nvidia/cuda:13.0.0-cudnn-devel-ubuntu24.04
ARG PADDLE_VERSION=3.3.0
ARG PADDLE_OCR_VERSION=3.4.0
ARG PADDLE_INDEX_URL=https://www.paddlepaddle.org.cn/packages/stable/cu130/

FROM ${PADDLE_BASE_IMAGE}
ARG PADDLE_VERSION
ARG PADDLE_OCR_VERSION
ARG PADDLE_INDEX_URL
LABEL authors="Stan1ey"

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PADDLE_PDX_MODEL_SOURCE=BOS \
    PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        build-essential \
        curl \
        ca-certificates \
        git \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 \
        libgomp1 \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv "${VIRTUAL_ENV}"
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel uv
RUN python -m pip install --no-cache-dir "paddlepaddle-gpu==${PADDLE_VERSION}" -i "${PADDLE_INDEX_URL}"
RUN python -m pip install --no-cache-dir "paddleocr[doc-parser]==${PADDLE_OCR_VERSION}"

COPY pyproject.toml uv.lock ./
RUN uv sync --active --frozen --inexact --no-install-project

COPY . .
RUN uv sync --active --frozen --inexact

EXPOSE 8080

CMD ["uv", "run", "--no-sync", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
