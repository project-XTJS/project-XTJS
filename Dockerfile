# 1. 使用极其精简的 Python 3.13 官方镜像作为底座
FROM python:3.13-slim

# 2. 设置环境变量：防止 Python 乱写缓存，强制使用无缓冲的标准输出
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# 3. 安装 Linux 系统底层的依赖库 (OpenCV 和 Paddle 必须用到这些 C++ 库)
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# 4. 设定工作目录
WORKDIR /app

# 5. 先复制依赖清单，利用 Docker 缓存机制加速构建
COPY requirements.txt .

# 6. 🌟 核心：先强行安装适配 CUDA 13.0 的 PaddlePaddle-GPU！
RUN pip install --no-cache-dir paddlepaddle-gpu==3.3.0 -i https://www.paddlepaddle.org.cn/packages/stable/cu130/

# 7. 安装项目其余的常规依赖
RUN pip install --no-cache-dir -r requirements.txt

# 8. 把你本地所有的代码全盘复制到容器里
COPY . .

# 9. 暴露 FastAPI 运行的 8080 端口
EXPOSE 8080

# 10. 容器启动时的默认命令
CMD ["python", "run.py"]