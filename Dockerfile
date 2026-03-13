ARG PADDLE_BASE_IMAGE=nvidia/cuda:13.0.0-cudnn-devel-ubuntu24.04
ARG PADDLE_VERSION=3.3.0
ARG PADDLE_INDEX_URL=https://www.paddlepaddle.org.cn/packages/stable/cu130/
FROM ${PADDLE_BASE_IMAGE}
LABEL authors="Stan1ey"

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgl1 \
        libglib2.0-0 \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv "${VIRTUAL_ENV}"
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel uv
RUN python -m pip install --no-cache-dir paddlepaddle-gpu==${PADDLE_VERSION} -i ${PADDLE_INDEX_URL}

COPY pyproject.toml uv.lock ./
RUN uv sync --active --frozen --inexact --no-install-project

COPY . .
RUN uv sync --active --frozen --inexact

EXPOSE 8080

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
