FROM python:3.12-slim
LABEL authors="Stan1ey"

# 安装必要的系统依赖

WORKDIR /app
# 先安装paddle
RUN pip install paddlepaddle-gpu -i https://mirror.baidu.com/pypi/simple
# 安装uv
RUN pip install --no-cache-dir uv

# 复制依赖文件
COPY pyproject.toml uv.lock ./

# 使用uv安装依赖
RUN uv sync

# 复制应用代码
COPY . .

# 暴露端口
EXPOSE 8080

# 启动应用
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]