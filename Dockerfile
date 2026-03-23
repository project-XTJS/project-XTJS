ARG PADDLE_BASE_IMAGE=nvidia/cuda:13.0.0-cudnn-devel-ubuntu24.04
ARG PADDLE_VERSION=3.3.0
ARG PADDLE_INDEX_URL=https://www.paddlepaddle.org.cn/packages/stable/cu130/
ARG PADDLE_TRUSTED_HOST=www.paddlepaddle.org.cn
ARG PADDLE_OCR_PACKAGE_VERSION=3.3.0
ARG PADDLE_OCR_DEPENDENCY_GROUP=doc-parser
ARG INSTALL_HPI_DEPS=false
FROM ${PADDLE_BASE_IMAGE}
ARG PADDLE_VERSION
ARG PADDLE_INDEX_URL
ARG PADDLE_TRUSTED_HOST
ARG PADDLE_OCR_PACKAGE_VERSION
ARG PADDLE_OCR_DEPENDENCY_GROUP
ARG INSTALL_HPI_DEPS
LABEL authors="Stan1ey"

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV PADDLE_PDX_MODEL_SOURCE=BOS

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

COPY wheels/ /tmp/wheels/

RUN python3 -m venv "${VIRTUAL_ENV}"
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN set -eux; \
    local_wheel="$(find /tmp/wheels -maxdepth 1 -type f -name 'paddlepaddle_gpu-*.whl' | head -n 1)"; \
    if [ -n "${local_wheel}" ]; then \
        python -m pip install --no-cache-dir \
            httpx \
            "numpy>=1.21" \
            "protobuf>=3.20.2" \
            Pillow \
            opt_einsum==3.3.0 \
            networkx \
            typing_extensions \
            "safetensors>=0.6.0"; \
        python -m pip install --no-cache-dir --no-deps "${local_wheel}"; \
    else \
        python -m pip install --no-cache-dir \
            --trusted-host "${PADDLE_TRUSTED_HOST}" \
            "paddlepaddle-gpu==${PADDLE_VERSION}" \
            -i "${PADDLE_INDEX_URL}"; \
    fi
RUN python -m pip install --no-cache-dir "paddleocr[${PADDLE_OCR_DEPENDENCY_GROUP}]==${PADDLE_OCR_PACKAGE_VERSION}"
RUN if [ "${INSTALL_HPI_DEPS}" = "true" ]; then python -m paddleocr install_hpi_deps gpu; fi

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["/app/.venv/bin/python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
