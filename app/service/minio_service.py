import os
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fastapi import UploadFile
from minio import Minio
from minio.error import S3Error

from app.config.minio import MinioConfig


class MinioService:
    """MinIO 业务服务：负责上传、删除、对象名处理。"""

    def __init__(self) -> None:
        # 客户端在服务初始化时创建，供路由层复用。
        self.client = Minio(
            endpoint=MinioConfig.ENDPOINT,
            access_key=MinioConfig.ACCESS_KEY,
            secret_key=MinioConfig.SECRET_KEY,
            secure=MinioConfig.SECURE,
        )

    @staticmethod
    def _get_file_size(file: UploadFile) -> int:
        """通过文件指针计算上传文件大小。"""
        try:
            file_obj = file.file
            file_obj.seek(0, os.SEEK_END)
            size = file_obj.tell()
            file_obj.seek(0)
            return size
        except Exception:
            return 0

    def validate_upload_file(self, file: UploadFile) -> None:
        """统一校验文件大小、文件名与后缀合法性。"""
        size = self._get_file_size(file)
        if size <= 0 or size > MinioConfig.MAX_FILE_SIZE:
            raise ValueError(
                f"文件大小必须在 1B 到 {MinioConfig.MAX_FILE_SIZE // 1024 // 1024}MB 之间。"
            )

        if not file.filename:
            raise ValueError("文件名不能为空。")

        extension = os.path.splitext(file.filename)[1].lower().lstrip(".")
        if extension not in MinioConfig.ALLOWED_EXTENSIONS:
            raise ValueError(f"不支持的文件类型：{extension}")

    @staticmethod
    def generate_object_name(filename: str, object_name: str | None = None) -> str:
        """生成对象名；未指定时以时间戳避免重名。"""
        if object_name:
            return object_name
        basename = os.path.basename(filename or "file")
        name, ext = os.path.splitext(basename)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        unique_suffix = uuid4().hex[:8]
        return f"{name}_{timestamp}_{unique_suffix}{ext}"

    @staticmethod
    def build_file_url(object_name: str) -> str:
        """构建可持久化 file_url（不含签名参数）。"""
        if not object_name or not object_name.strip():
            raise ValueError("对象名不能为空。")
        normalized_object_name = object_name.lstrip("/")
        return f"minio://{MinioConfig.BUCKET_NAME}/{normalized_object_name}"

    @staticmethod
    def is_presigned_url(file_url: str) -> bool:
        """判断地址是否为预签名 URL。"""
        if not file_url:
            return False
        parsed = urlparse(file_url)
        if parsed.scheme not in {"http", "https"}:
            return False
        query = parse_qs(parsed.query)
        signature_fields = {
            "X-Amz-Signature",
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-Expires",
        }
        return any(field in query for field in signature_fields)

    def _object_exists(self, object_name: str) -> bool:
        """检查对象是否已存在于桶中。"""
        try:
            self.client.stat_object(MinioConfig.BUCKET_NAME, object_name)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise RuntimeError(f"检查 MinIO 对象是否存在失败：{exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"检查 MinIO 对象是否存在时发生异常：{exc}") from exc

    def _resolve_upload_object_name(self, filename: str, object_name: str | None) -> str:
        """解析上传对象名并避免与已有对象冲突。"""
        if object_name:
            if self._object_exists(object_name):
                raise ValueError(f"对象名已存在：{object_name}")
            return object_name

        for _ in range(5):
            generated = self.generate_object_name(filename)
            if not self._object_exists(generated):
                return generated
        raise RuntimeError("生成唯一对象名失败，请重试。")

    def ensure_bucket(self) -> None:
        """确保业务桶存在，不存在时自动创建。"""
        try:
            if not self.client.bucket_exists(MinioConfig.BUCKET_NAME):
                self.client.make_bucket(MinioConfig.BUCKET_NAME)
        except S3Error as exc:
            raise RuntimeError(f"检查或创建 MinIO 存储桶失败：{exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"MinIO 存储桶操作异常：{exc}") from exc

    def upload_file(self, file: UploadFile, object_name: str | None = None) -> dict:
        """上传文件并返回对象名、file_url、预签名 URL、文件大小。"""
        self.validate_upload_file(file)
        size = self._get_file_size(file)
        if size <= 0:
            raise ValueError("无法读取上传文件大小。")

        try:
            self.ensure_bucket()
            object_name = self._resolve_upload_object_name(file.filename, object_name)
            self.client.put_object(
                bucket_name=MinioConfig.BUCKET_NAME,
                object_name=object_name,
                data=file.file,
                length=size,
                content_type=file.content_type,
            )
            presigned_url = self.get_presigned_url(object_name)
            file_url = self.build_file_url(object_name)
            return {
                "object_name": object_name,
                "file_url": file_url,
                "presigned_url": presigned_url,
                "size": size,
            }
        except ValueError:
            raise
        except RuntimeError:
            raise
        except S3Error as exc:
            raise RuntimeError(f"MinIO 上传失败：{exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"MinIO 上传过程中发生异常：{exc}") from exc

    def get_presigned_url(self, object_name: str) -> str:
        """按对象名生成预签名访问 URL。"""
        if not object_name or not object_name.strip():
            raise ValueError("对象名不能为空。")

        try:
            return self.client.presigned_get_object(
                MinioConfig.BUCKET_NAME,
                object_name,
                expires=timedelta(days=MinioConfig.URL_EXPIRE_DAYS),
            )
        except S3Error as exc:
            raise RuntimeError(f"生成 MinIO 预签名 URL 失败：{exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"生成 MinIO 预签名 URL 时发生异常：{exc}") from exc

    def delete_file(self, object_name: str) -> None:
        """按对象名删除文件。"""
        if not object_name or not object_name.strip():
            raise ValueError("对象名不能为空。")

        try:
            self.client.remove_object(MinioConfig.BUCKET_NAME, object_name)
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                raise ValueError(f"对象不存在：{object_name}") from exc
            raise RuntimeError(f"MinIO 删除失败：{exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"MinIO 删除过程中发生异常：{exc}") from exc

    @staticmethod
    def object_name_from_presigned_url(file_url: str) -> str:
        """从预签名 URL 反解对象名，供删除流程使用。"""
        parsed = urlparse(file_url)
        path = parsed.path.lstrip("/")
        parts = path.split("/", 1)
        if len(parts) != 2:
            raise ValueError("无效的 MinIO 文件 URL。")
        return parts[1]

    @staticmethod
    def object_name_from_file_url(file_url: str) -> str:
        """从 file_url（minio://bucket/object）反解对象名。"""
        parsed = urlparse(file_url)
        if parsed.scheme != "minio":
            raise ValueError("无效的 MinIO 存储地址。")
        if parsed.netloc != MinioConfig.BUCKET_NAME:
            raise ValueError("存储地址中的桶名与当前配置不一致。")

        object_name = parsed.path.lstrip("/")
        if not object_name:
            raise ValueError("存储地址中缺少对象名。")
        return object_name
