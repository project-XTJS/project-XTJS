FROM python:3.12-slim
LABEL authors="Stan1ey"

# 安装必要的系统依赖
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 安装uv
RUN pip install --no-cache-dir uv

# 复制依赖文件
COPY requirements.txt pyproject.toml uv.lock ./

# 使用uv安装依赖
RUN uv pip install --index-url https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# 复制应用代码
COPY . .

# 暴露端口
EXPOSE 8080

# 启动应用
CMD ["uv", "run", "app.main.py"]
