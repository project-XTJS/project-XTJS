FROM ccr-2vdh3abv-pub.cnc.bj.baidubce.com/paddlepaddle/paddle:3.0.0-gpu-cuda12.6-cudnn9.5-trt10.5
LABEL authors="Stan1ey"

WORKDIR /app

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy

RUN python -m venv --system-site-packages "${VIRTUAL_ENV}"
RUN python -m pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --active --frozen --inexact --no-install-project

COPY . .
RUN uv sync --active --frozen --inexact

EXPOSE 8080

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
