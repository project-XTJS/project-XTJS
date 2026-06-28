# -*- coding: utf-8 -*-
"""
MinIO 对象存储服务模块。

封装 MinIO 客户端操作，提供文件上传、删除、预签名 URL、
对象存在性检查、URL 解析等能力，并包含审计日志记录。
"""

import gzip
import io
import json
import os
import re
import logging
from datetime import datetime, timedelta
from mimetypes import guess_type
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fastapi import UploadFile
from minio import Minio
from minio.error import S3Error

from app.config.settings import settings

logger = logging.getLogger(__name__)


class MinioService:
    """MinIO 对象操作服务层，封装桶管理、文件上传、预签名及辅助解析。"""

    # 不支持的 Word 文件扩展名
    UNSUPPORTED_WORD_EXTENSIONS = {"doc", "docx"}

    def __init__(self) -> None:
        """初始化 MinIO 客户端，从全局配置读取连接信息和桶名。"""
        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self.bucket_name = settings.MINIO_BUCKET_NAME

    def _audit(
        self,
        action: str,
        status: str,
        object_name: str | None = None,
        detail: str | None = None,
    ) -> None:
        """记录 MinIO 操作审计日志。"""
        logger.info(
            "minio_audit action=%s status=%s bucket=%s object_name=%s endpoint=%s detail=%s",
            action,
            status,
            self.bucket_name,
            object_name or "",
            settings.MINIO_ENDPOINT,
            detail or "",
        )

    @staticmethod
    def _get_file_size(file: UploadFile) -> int:
        """尝试获取上传文件的大小（字节），失败时返回 0。"""
        try:
            file_obj = file.file
            file_obj.seek(0, os.SEEK_END)
            size = file_obj.tell()
            file_obj.seek(0)
            return size
        except Exception:
            return 0

    def validate_upload_file(self, file: UploadFile) -> None:
        """校验上传文件的类型、大小，不允许 Word 及不支持的类型。"""
        size = self._get_file_size(file)
        if size <= 0 or size > settings.MINIO_MAX_FILE_SIZE:
            max_mb = settings.MINIO_MAX_FILE_SIZE // 1024 // 1024
            raise ValueError(f"File size must be between 1B and {max_mb}MB")

        if not file.filename:
            raise ValueError("File name cannot be empty")

        extension = os.path.splitext(file.filename)[1].lower().lstrip(".")
        if extension in self.UNSUPPORTED_WORD_EXTENSIONS:
            raise ValueError("Word files are not supported")
        if extension not in settings.minio_allowed_extensions:
            raise ValueError(f"Unsupported file type: {extension}")

    @staticmethod
    def generate_object_name(filename: str, object_name: str | None = None) -> str:
        """生成唯一对象名：基于原文件名、时间戳和随机后缀。"""
        if object_name:
            return object_name
        basename = os.path.basename(filename or "file")
        name, ext = os.path.splitext(basename)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        unique_suffix = uuid4().hex[:8]
        return f"{name}_{timestamp}_{unique_suffix}{ext}"

    @staticmethod
    def _safe_segment(name: object, *, maxlen: int = 100) -> str:
        """把项目名/公司名/文件名安全化为对象键的一段（去非法字符、限长）。"""
        text = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", str(name or "").strip())
        text = text.strip(". ")
        return text[:maxlen] or "unnamed"

    @staticmethod
    def build_project_object_key(
        project: object,
        *,
        role: str,
        company: object | None = None,
        filename: object | None = None,
        kind: str = "original",
    ) -> str:
        """构建分级对象键。

        role: tender / business_bid / technical_bid / result
        kind: "original"(原件) / "json"(JSON 识别结果)
          原件:   <项目>/招标文件/<file>            <项目>/投标文件/<公司>/{商务标|技术标}_<file>
          JSON:   <项目>/JSON识别/招标文件/<file>.json.gz
                  <项目>/JSON识别/投标文件/<公司>/{商务标|技术标}_<file>.json.gz
                  <项目>/JSON识别/result.json.gz   (role=result)
        """
        proj = MinioService._safe_segment(project)
        parts: list[str] = [proj]
        if kind == "json":
            parts.append("JSON识别")
            if str(role or "").strip() == "result":
                return "/".join(parts + ["result.json.gz"])
        role = str(role or "").strip()
        fname = MinioService._safe_segment(filename or "file", maxlen=140)
        if kind == "json":
            fname = f"{fname}.json.gz"
        elif kind == "original":
            # 原件是物理文件，键需唯一以防同项目/公司同名文件互相覆盖（沿用时间戳+随机后缀）。
            stem, ext = os.path.splitext(fname)
            fname = f"{stem}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid4().hex[:8]}{ext}"
        if role == "tender":
            parts += ["招标文件", fname]
        elif role in ("business_bid", "technical_bid"):
            label = "商务标" if role == "business_bid" else "技术标"
            parts += ["投标文件", MinioService._safe_segment(company or "未知公司"), f"{label}_{fname}"]
        else:
            parts += [role or "其它", fname]
        return "/".join(parts)

    @staticmethod
    def build_file_url(object_name: str, bucket_name: str | None = None) -> str:
        """构建系统内部使用的 minio:// URL。"""
        if not object_name or not object_name.strip():
            raise ValueError("Object name cannot be empty")

        resolved_bucket_name = (bucket_name or settings.MINIO_BUCKET_NAME).strip()
        if not resolved_bucket_name:
            raise ValueError("Bucket name cannot be empty")

        normalized_object_name = object_name.lstrip("/")
        return f"minio://{resolved_bucket_name}/{normalized_object_name}"

    @staticmethod
    def guess_content_type(object_name: str, default: str = "application/octet-stream") -> str:
        """根据对象名推断 MIME 类型。"""
        guessed, _ = guess_type(str(object_name or "").strip())
        return guessed or default

    @staticmethod
    def is_presigned_url(file_url: str) -> bool:
        """判断给定的 URL 是否为 MinIO 预签名 URL。"""
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
        """检查 MinIO 中对象是否存在。"""
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise RuntimeError(f"Failed to check object existence: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Error while checking object existence: {exc}") from exc

    def _resolve_upload_object_name(self, filename: str, object_name: str | None) -> str:
        """确定上传对象名：若传入且不冲突则直接使用，否则生成唯一名。"""
        if object_name:
            if self._object_exists(object_name):
                raise ValueError(f"Object already exists: {object_name}")
            return object_name

        for _ in range(5):
            generated = self.generate_object_name(filename)
            if not self._object_exists(generated):
                return generated
        raise RuntimeError("Failed to generate a unique object name")

    def ensure_bucket(self) -> None:
        """确保 MinIO 桶存在，不存在则创建。"""
        if not self.bucket_name or not self.bucket_name.strip():
            raise RuntimeError("Fixed MinIO bucket name is empty")

        try:
            exists = self.client.bucket_exists(self.bucket_name)
            if exists:
                self._audit(action="ensure_bucket", status="exists")
                return

            self.client.make_bucket(self.bucket_name)
            self._audit(action="ensure_bucket", status="created")
        except S3Error as exc:
            self._audit(
                action="ensure_bucket",
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"Failed to check/create MinIO bucket: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="ensure_bucket",
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO bucket operation error: {exc}") from exc

    def upload_file(self, file: UploadFile, object_name: str | None = None) -> dict:
        """上传文件至 MinIO，返回对象名、桶名、内部 URL 和预签名 URL。"""
        self.validate_upload_file(file)
        size = self._get_file_size(file)
        if size <= 0:
            raise ValueError("Failed to read upload file size")

        try:
            self.ensure_bucket()
            object_name = self._resolve_upload_object_name(file.filename, object_name)
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=file.file,
                length=size,
                content_type=file.content_type,
            )
            presigned_url = self.get_presigned_url(object_name)
            file_url = self.build_file_url(object_name, self.bucket_name)
            self._audit(action="upload_file", status="success", object_name=object_name)
            return {
                "object_name": object_name,
                "bucket_name": self.bucket_name,
                "file_url": file_url,
                "presigned_url": presigned_url,
                "size": size,
            }
        except ValueError:
            raise
        except RuntimeError:
            raise
        except S3Error as exc:
            self._audit(
                action="upload_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO upload failed: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="upload_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO upload error: {exc}") from exc

    def upload_bytes(
        self,
        data: bytes,
        *,
        filename: str,
        content_type: str = "application/octet-stream",
        object_name: str | None = None,
        if_exists: str = "error",
    ) -> dict:
        """Upload an in-memory payload to MinIO and return object metadata plus URLs.

        if_exists 控制目标对象已存在时的行为：
          - "error"（默认）：抛 "Object already exists"，避免覆盖用户文件；
          - "reuse"：直接复用已存在对象（如预览图等确定性缓存路径），不再重复上传；
          - "overwrite"：覆盖写入。
        """
        size = len(data or b"")
        if size <= 0:
            raise ValueError("Upload content cannot be empty")
        if size > settings.MINIO_MAX_FILE_SIZE:
            max_mb = settings.MINIO_MAX_FILE_SIZE // 1024 // 1024
            raise ValueError(f"File size must be between 1B and {max_mb}MB")
        if not filename or not filename.strip():
            raise ValueError("File name cannot be empty")

        try:
            self.ensure_bucket()
            if object_name and if_exists in ("reuse", "overwrite") and self._object_exists(object_name):
                if if_exists == "reuse":
                    # 幂等复用：缓存类对象（如预览图）已存在则直接复用，避免 "Object already exists" 报错。
                    presigned_url = self.get_presigned_url(object_name)
                    file_url = self.build_file_url(object_name, self.bucket_name)
                    self._audit(action="upload_bytes", status="reused", object_name=object_name)
                    return {
                        "object_name": object_name,
                        "bucket_name": self.bucket_name,
                        "file_url": file_url,
                        "presigned_url": presigned_url,
                        "size": size,
                        "content_type": content_type,
                        "reused": True,
                    }
                # if_exists == "overwrite": 沿用该对象名直接覆盖写入
            else:
                object_name = self._resolve_upload_object_name(filename, object_name)
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=io.BytesIO(data),
                length=size,
                content_type=content_type,
            )
            presigned_url = self.get_presigned_url(object_name)
            file_url = self.build_file_url(object_name, self.bucket_name)
            self._audit(action="upload_bytes", status="success", object_name=object_name)
            return {
                "object_name": object_name,
                "bucket_name": self.bucket_name,
                "file_url": file_url,
                "presigned_url": presigned_url,
                "size": size,
                "content_type": content_type,
            }
        except ValueError:
            raise
        except RuntimeError:
            raise
        except S3Error as exc:
            self._audit(
                action="upload_bytes",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO upload failed: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="upload_bytes",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO upload error: {exc}") from exc

    def get_presigned_url(self, object_name: str, bucket_name: str | None = None) -> str:
        """生成指定对象的预签名下载 URL，默认过期天数由配置决定。"""
        if not object_name or not object_name.strip():
            raise ValueError("Object name cannot be empty")

        try:
            resolved_bucket_name = str(bucket_name or self.bucket_name or "").strip()
            if not resolved_bucket_name:
                raise ValueError("Bucket name cannot be empty")
            if resolved_bucket_name == self.bucket_name:
                self.ensure_bucket()
            expires_days = max(1, min(int(settings.MINIO_PRESIGNED_EXPIRES_DAYS), 7))
            presigned_url = self.client.presigned_get_object(
                resolved_bucket_name,
                object_name,
                expires=timedelta(days=expires_days),
            )
            self._audit(action="get_presigned_url", status="success", object_name=object_name)
            return presigned_url
        except S3Error as exc:
            self._audit(
                action="get_presigned_url",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"Failed to generate presigned URL: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="get_presigned_url",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"Error while generating presigned URL: {exc}") from exc

    def delete_file(self, object_name: str) -> None:
        """删除 MinIO 中的指定对象，不存在时抛出 ValueError。"""
        if not object_name or not object_name.strip():
            raise ValueError("Object name cannot be empty")

        try:
            self.ensure_bucket()
            self.client.remove_object(self.bucket_name, object_name)
            self._audit(action="delete_file", status="success", object_name=object_name)
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                self._audit(
                    action="delete_file",
                    status="not_found",
                    object_name=object_name,
                    detail=f"{type(exc).__name__}: {exc}",
                )
                raise ValueError(f"Object not found: {object_name}") from exc
            self._audit(
                action="delete_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO delete failed: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="delete_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO delete error: {exc}") from exc

    def get_object_bytes(
        self,
        object_name: str,
        bucket_name: str | None = None,
    ) -> tuple[bytes, str]:
        """下载对象并返回字节内容和 MIME 类型。"""
        resolved_object_name = str(object_name or "").strip()
        if not resolved_object_name:
            raise ValueError("Object name cannot be empty")

        resolved_bucket_name = str(bucket_name or self.bucket_name or "").strip()
        if not resolved_bucket_name:
            raise ValueError("Bucket name cannot be empty")

        response = None
        try:
            if resolved_bucket_name == self.bucket_name:
                self.ensure_bucket()
            response = self.client.get_object(resolved_bucket_name, resolved_object_name)
            data = response.read()
            content_type = (
                response.headers.get("Content-Type")
                or self.guess_content_type(resolved_object_name)
            )
            self._audit(
                action="get_object_bytes",
                status="success",
                object_name=resolved_object_name,
            )
            return data, content_type
        except ValueError:
            raise
        except S3Error as exc:
            self._audit(
                action="get_object_bytes",
                status="failed",
                object_name=resolved_object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO get object failed: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="get_object_bytes",
                status="failed",
                object_name=resolved_object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO get object error: {exc}") from exc
        finally:
            if response is not None:
                try:
                    response.close()
                    response.release_conn()
                except Exception:
                    pass

    def put_json_gz(self, object_name: str, obj: object) -> dict:
        """把 JSON 对象 gzip 压缩后写入 MinIO（覆盖写），用于把大 JSON 移出数据库。"""
        if not object_name or not str(object_name).strip():
            raise ValueError("Object name cannot be empty")
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        data = gzip.compress(raw, compresslevel=6)
        try:
            self.ensure_bucket()
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=io.BytesIO(data),
                length=len(data),
                content_type="application/gzip",
            )
            self._audit(action="put_json_gz", status="success", object_name=object_name)
            return {
                "object_name": object_name,
                "bucket_name": self.bucket_name,
                "file_url": self.build_file_url(object_name, self.bucket_name),
                "size": len(data),
                "raw_size": len(raw),
            }
        except S3Error as exc:
            self._audit(action="put_json_gz", status="failed", object_name=object_name, detail=str(exc))
            raise RuntimeError(f"MinIO put_json_gz failed: {exc}") from exc

    def get_json_gz(self, object_name: str, bucket_name: str | None = None):
        """读取 gzip JSON 对象并反序列化；对象不存在返回 None。"""
        resolved_object = str(object_name or "").strip()
        if not resolved_object:
            return None
        resolved_bucket = str(bucket_name or self.bucket_name or "").strip()
        if resolved_bucket == self.bucket_name and not self._object_exists(resolved_object):
            return None
        data, _ = self.get_object_bytes(resolved_object, bucket_name)
        if not data:
            return None
        try:
            raw = gzip.decompress(data)
        except (OSError, EOFError):
            raw = data  # 兼容历史未压缩 JSON
        return json.loads(raw.decode("utf-8"))

    # 预签名 URL 解析工具
    @staticmethod
    def object_name_from_presigned_url(file_url: str) -> str:
        """从预签名 URL 提取对象名。"""
        _, object_name = MinioService.bucket_and_object_from_presigned_url(file_url)
        return object_name

    @staticmethod
    def bucket_name_from_presigned_url(file_url: str) -> str:
        """从预签名 URL 提取桶名。"""
        bucket_name, _ = MinioService.bucket_and_object_from_presigned_url(file_url)
        return bucket_name

    @staticmethod
    def bucket_and_object_from_presigned_url(file_url: str) -> tuple[str, str]:
        """解析预签名 URL，返回 (桶名, 对象名)。"""
        parsed = urlparse(file_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Invalid MinIO presigned URL")

        path = parsed.path.lstrip("/")
        parts = path.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError("Invalid MinIO presigned URL: missing bucket/object")
        return parts[0], parts[1]

    # 内部 file_url 解析工具
    @staticmethod
    def object_name_from_file_url(file_url: str) -> str:
        """从内部 minio:// URL 提取对象名。"""
        _, object_name = MinioService.bucket_and_object_from_file_url(file_url)
        return object_name

    @staticmethod
    def bucket_name_from_file_url(file_url: str) -> str:
        """从内部 minio:// URL 提取桶名。"""
        bucket_name, _ = MinioService.bucket_and_object_from_file_url(file_url)
        return bucket_name

    @staticmethod
    def bucket_and_object_from_file_url(file_url: str) -> tuple[str, str]:
        """解析内部 minio:// URL，返回 (桶名, 对象名)。"""
        parsed = urlparse(file_url)
        if parsed.scheme != "minio":
            raise ValueError("Invalid MinIO storage URL")

        bucket_name = parsed.netloc.strip()
        if not bucket_name:
            raise ValueError("Invalid MinIO storage URL: missing bucket")

        object_name = parsed.path.lstrip("/")
        if not object_name:
            raise ValueError("Invalid MinIO storage URL: missing object name")
        return bucket_name, object_name
