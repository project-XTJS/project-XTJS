import os
from datetime import datetime, timedelta

from minio import Minio
from minio.error import S3Error

# 配置minio
class MinioConfig:
    ENDPOINT = "127.0.0.1:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    BUCKET_NAME = "tendering-files"
    SECURE = False    # 本地测试用http，生产环境使用True(https)

# 招投标文件允许的格式
ALLOWED_EXTENSIONS = {"pdf", "docx", "doc"}
# 文件大小限制：500MB（招投标文件通常较大，可调整）
MAX_FILE_SIZE = 500 * 1024 * 1024

# MinIO 客户端初始化
# 全局MinIO客户端实例（初始化一次即可）
minio_client = Minio(
    endpoint=MinioConfig.ENDPOINT,
    access_key=MinioConfig.ACCESS_KEY,
    secret_key=MinioConfig.SECRET_KEY,
    secure=MinioConfig.SECURE
)


# ===================== 工具函数（文件上传/删除/校验） =====================
def check_file_valid(file) -> bool:
    """
    校验上传的招投标文件是否合法（格式+大小）
    :param file: FastAPI的UploadFile对象
    :return: 合法返回True，否则False
    """
    # 1. 校验文件大小
    if file.size > MAX_FILE_SIZE:
        return False
    # 2. 校验文件格式（取后缀名，忽略大小写）
    filename = file.filename
    if not filename:
        return False
    ext = os.path.splitext(filename)[1].lower().lstrip(".")
    if ext not in ALLOWED_EXTENSIONS:
        return False
    return True


def upload_tendering_file(file, object_name: str = None) -> str:
    """
    上传招投标文件到MinIO
    :param file: UploadFile对象（FastAPI接收的文件）
    :param object_name: 存储到MinIO的自定义文件名（默认用原文件名）
    :return: 文件的临时访问URL（7天有效期）
    :raise: 校验失败/上传失败时抛出异常
    """
    # 前置校验
    if not check_file_valid(file):
        raise ValueError(
            f"文件不符合要求！仅支持{ALLOWED_EXTENSIONS}格式，且大小不超过{MAX_FILE_SIZE / 1024 / 1024}MB"
        )

    # 生成存储对象名（避免重名，拼接时间戳）
    if not object_name:
        # 1. 获取当前时间戳（格式：20260305143025，精确到秒，避免重名）
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        # 2. 拆分原文件名的“名称”和“扩展名”
        filename = file.filename
        name, ext = os.path.splitext(filename)  # 例："标书.pdf" → ("标书", ".pdf")
        # 3. 拼接：原名称 + _ + 时间戳 + 扩展名
        object_name = f"{name}_{timestamp}{ext}"

    try:
        # 检查Bucket是否存在，不存在则自动创建
        if not minio_client.bucket_exists(MinioConfig.BUCKET_NAME):
            minio_client.make_bucket(MinioConfig.BUCKET_NAME)

        # 上传文件到MinIO
        minio_client.put_object(
            bucket_name=MinioConfig.BUCKET_NAME,
            object_name=object_name,
            data=file.file,  # 文件流
            length=file.size,  # 文件大小
            content_type=file.content_type  # 文件MIME类型
        )

        # 生成带有效期的访问URL（7天，可根据需求调整）
        file_url = minio_client.presigned_get_object(
            MinioConfig.BUCKET_NAME,
            object_name,
            expires=timedelta(days=7)
        )
        return file_url

    except S3Error as e:
        raise Exception(f"MinIO文件上传失败：{e.message}")


def delete_tendering_file(object_name: str) -> bool:
    """
    删除MinIO中的招投标文件
    :param object_name: MinIO中存储的文件名
    :return: 删除成功返回True
    """
    try:
        minio_client.remove_object(MinioConfig.BUCKET_NAME, object_name)
        return True
    except S3Error as e:
        raise Exception(f"MinIO文件删除失败：{e.message}")