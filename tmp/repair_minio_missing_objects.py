from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService


SOURCE_DIR = Path(r"D:\Desktop\测试文件\2-投标文件")
API_BASE = "http://127.0.0.1:8080"
RUN_TAG = datetime.now().strftime("%Y%m%d%H%M%S")


@dataclass(frozen=True)
class RestoreItem:
    identifier_id: str
    expected_file_name: str
    local_file_name: str
    role: str


RESTORE_ITEMS: tuple[RestoreItem, ...] = (
    RestoreItem(
        "03436cf0-46d3-4b42-848a-de9dda29f936",
        "（终稿）XTJS2021-952 上海浦江游览船体广告位.pdf",
        "（终稿）XTJS2021-952 上海浦江游览船体广告位.pdf",
        "tender",
    ),
    RestoreItem(
        "92013c65-0e69-40d0-80f8-656438d745d3",
        "翡翠公主号-投标文件（商务标）1108.pdf",
        "翡翠商务标.pdf",
        "business",
    ),
    RestoreItem(
        "d8a77f6a-55f5-4d33-97fb-e1258983bbcb",
        "亚元商务标.pdf",
        "亚元商务标.pdf",
        "business",
    ),
    RestoreItem(
        "41f33eb4-d950-4ec3-ad9d-157426c6ddc7",
        "上海浦江游览船体广告位采购项目-善元.pdf",
        "上海浦江游览船体广告位采购项目-善元.pdf",
        "business",
    ),
    RestoreItem(
        "3029746f-d926-4a8b-9a6a-586ff3396ae7",
        "翡翠公主号-投标文件（技术标）1108.pdf",
        "翡翠技术标.pdf",
        "technical",
    ),
    RestoreItem(
        "335bf943-c52c-463a-a28e-074bba92abb4",
        "亚元技术标.pdf",
        "亚元技术标.pdf",
        "technical",
    ),
    RestoreItem(
        "d2607012-d337-4871-8142-f23d6cf932b8",
        "阳生文化技术标.pdf",
        "阳生文化技术标.pdf",
        "technical",
    ),
    RestoreItem(
        "8470ae8c-3ede-4296-9475-2f1b25af09b5",
        "善元技术标.pdf",
        "善元技术标.pdf",
        "technical",
    ),
)


def unique_object_name(service: MinioService, role: str, identifier_id: str) -> str:
    prefix = f"restore_{role}_{identifier_id.split('-')[0]}_{RUN_TAG}"
    suffix = ".pdf"
    object_name = f"{prefix}{suffix}"
    counter = 1
    while service._object_exists(object_name):  # noqa: SLF001 - utility script
        object_name = f"{prefix}_{counter}{suffix}"
        counter += 1
    return object_name


def upload_and_update(
    pg_service: PostgreSQLService,
    minio_service: MinioService,
    item: RestoreItem,
) -> dict:
    document = pg_service.get_document_by_identifier(item.identifier_id)
    if not document:
        raise RuntimeError(f"数据库中未找到文档: {item.identifier_id}")

    local_path = SOURCE_DIR / item.local_file_name
    if not local_path.exists():
        raise FileNotFoundError(f"本地源文件不存在: {local_path}")

    object_name = unique_object_name(minio_service, item.role, item.identifier_id)
    content_type = minio_service.guess_content_type(local_path.name, "application/pdf")
    with local_path.open("rb") as handle:
        minio_service.ensure_bucket()
        minio_service.client.put_object(
            bucket_name=minio_service.bucket_name,
            object_name=object_name,
            data=handle,
            length=local_path.stat().st_size,
            content_type=content_type,
        )

    file_url = minio_service.build_file_url(object_name, minio_service.bucket_name)
    updated = pg_service.update_document(item.identifier_id, file_url=file_url)
    if not updated:
        raise RuntimeError(f"更新数据库失败: {item.identifier_id}")

    return {
        "identifier_id": item.identifier_id,
        "db_file_name": document["file_name"],
        "expected_file_name": item.expected_file_name,
        "local_file_name": item.local_file_name,
        "object_name": object_name,
        "file_url": file_url,
        "size": local_path.stat().st_size,
    }


def verify_preview(identifier_id: str) -> dict:
    url = f"{API_BASE}/api/postgresql/documents/{identifier_id}/preview/pages/1"
    try:
        with urlopen(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
            data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
            if not isinstance(data, dict):
                data = {}
            return {
                "status": response.status,
                "kind": data.get("source_kind"),
                "has_image": bool(data.get("image_data_url")),
            }
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"status": exc.code, "error": body}
    except URLError as exc:
        return {"status": "unreachable", "error": str(exc)}


def main(items: Iterable[RestoreItem]) -> int:
    minio_service = MinioService()
    pg_service = PostgreSQLService()

    restored: list[dict] = []
    for item in items:
        restored.append(upload_and_update(pg_service, minio_service, item))

    print("=== 已补回 MinIO 并更新数据库 ===")
    for row in restored:
        print(
            f"{row['identifier_id']} | {row['db_file_name']} | "
            f"{row['object_name']} | {row['size']}"
        )

    print("\n=== 预览接口验证 ===")
    success_count = 0
    for row in restored:
        result = verify_preview(row["identifier_id"])
        ok = result.get("status") == 200 and result.get("has_image")
        if ok:
            success_count += 1
        print(
            f"{row['identifier_id']} | status={result.get('status')} | "
            f"kind={result.get('kind')} | has_image={result.get('has_image')} | "
            f"error={result.get('error')}"
        )

    print(f"\n验证通过: {success_count}/{len(restored)}")
    return 0 if success_count == len(restored) else 1


if __name__ == "__main__":
    raise SystemExit(main(RESTORE_ITEMS))
