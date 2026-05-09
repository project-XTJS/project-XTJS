# unified/document_loader.py
"""
统一商务标审查 - 文档加载 Mixin

提供数据集扫描、文件加载、元数据构建以及项目标识解析等功能。
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import constants


class DocumentLoaderMixin:
    """
    文档加载相关的所有方法。

    依赖：
    - 实例属性：db_service, BUSINESS_FILE_RE, TECHNICAL_FILE_RE
    - 其他 Mixin：_page_count, _data_node, _utc_now_iso
    """

    # 声明实例属性类型提示（实际值由 __init__ 赋值）
    db_service: Any
    BUSINESS_FILE_RE: Any
    TECHNICAL_FILE_RE: Any

    # 文件发现与文档加载
    def _discover_dataset(self, dataset_dir: str | Path) -> dict[str, Any]:
        """扫描目录，自动匹配招标文件和投标人的商务/技术标文件对。"""
        base_dir = Path(dataset_dir).expanduser().resolve()
        if not base_dir.exists():
            raise FileNotFoundError(f"dataset_dir does not exist: {base_dir}")
        if not base_dir.is_dir():
            raise NotADirectoryError(f"dataset_dir is not a directory: {base_dir}")

        files = sorted(
            {
                path.resolve()
                for pattern in ("*.json", "*.JSON")
                for path in base_dir.glob(pattern)
                if path.is_file()
            },
            key=lambda item: item.name,
        )
        if not files:
            raise FileNotFoundError(f"no JSON files found under {base_dir}")

        tender_candidates: list[Path] = []
        bidder_docs: dict[str, dict[str, Any]] = {}

        for path in files:
            stem = path.stem.strip()
            if "招标" in stem:
                tender_candidates.append(path)
                continue

            role: str | None = None
            bidder_key = stem
            if "商务标" in stem:
                role = "business"
                bidder_key = self.BUSINESS_FILE_RE.sub("", stem).strip() or stem
            elif "技术标" in stem:
                role = "technical"
                bidder_key = self.TECHNICAL_FILE_RE.sub("", stem).strip() or stem

            if role is None:
                continue

            bidder_entry = bidder_docs.setdefault(
                bidder_key,
                {"bidder_key": bidder_key, "business_path": None, "technical_path": None},
            )
            bidder_entry[f"{role}_path"] = path

        if len(tender_candidates) != 1:
            raise ValueError(
                f"expected exactly one tender JSON file, found {len(tender_candidates)} under {base_dir}"
            )

        incomplete = [
            bidder_key
            for bidder_key, entry in bidder_docs.items()
            if not entry["business_path"] or not entry["technical_path"]
        ]
        if incomplete:
            raise ValueError(f"incomplete bidder document pairs: {', '.join(sorted(incomplete))}")

        tender_path = tender_candidates[0]
        dataset = {
            "base_dir": base_dir,
            "tender": self._load_document(tender_path, role="tender", bidder_key=None),
            "bidders": [],
        }

        for bidder_key in sorted(bidder_docs):
            entry = bidder_docs[bidder_key]
            dataset["bidders"].append(
                {
                    "bidder_key": bidder_key,
                    "business": self._load_document(entry["business_path"], role="business", bidder_key=bidder_key),
                    "technical": self._load_document(entry["technical_path"], role="technical", bidder_key=bidder_key),
                }
            )

        return dataset

    def _load_document(self, path: Path, *, role: str, bidder_key: str | None) -> dict[str, Any]:
        """从本地路径加载 JSON 文件，返回内容和元数据。"""
        content = json.loads(path.read_text(encoding="utf-8-sig"))
        return {
            "content": content,
            "meta": self._build_file_meta(path, role=role, bidder_key=bidder_key),
        }

    def _load_uploaded_document(
        self,
        *,
        file_name: str,
        raw_bytes: bytes,
        payload: dict[str, Any],
        role: str,
        bidder_key: str | None,
    ) -> dict[str, Any]:
        """封装上传文件的内存数据和元数据。"""
        return {
            "content": payload,
            "meta": self._build_uploaded_file_meta(
                file_name=file_name,
                raw_bytes=raw_bytes,
                payload=payload,
                role=role,
                bidder_key=bidder_key,
            ),
        }

    # 元数据构建
    def _build_file_meta(self, path: Path, *, role: str, bidder_key: str | None) -> dict[str, Any]:
        """构建本地文件的元数据，包含哈希、页码统计等。"""
        raw_bytes = path.read_bytes()
        stat = path.stat()
        data_node = self._data_node(json.loads(raw_bytes.decode("utf-8-sig")))
        return {
            "role": role,
            "bidder_key": bidder_key,
            "file_name": path.name,
            "file_path": str(path),
            "file_size": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "layout_section_count": len(data_node.get("layout_sections", []) or []),
            "logical_table_count": len(data_node.get("logical_tables", []) or []),
            "native_table_count": len(data_node.get("native_tables", []) or []),
            "page_count": self._page_count(data_node),
        }

    def _build_uploaded_file_meta(
        self,
        *,
        file_name: str,
        raw_bytes: bytes,
        payload: dict[str, Any],
        role: str,
        bidder_key: str | None,
    ) -> dict[str, Any]:
        """构建上传文件的元数据。"""
        data_node = self._data_node(payload)
        return {
            "role": role,
            "bidder_key": bidder_key,
            "file_name": file_name,
            "file_path": None,
            "file_size": len(raw_bytes),
            "modified_at": self._utc_now_iso(),
            "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "layout_section_count": len(data_node.get("layout_sections", []) or []),
            "logical_table_count": len(data_node.get("logical_tables", []) or []),
            "native_table_count": len(data_node.get("native_tables", []) or []),
            "page_count": self._page_count(data_node),
            "source_type": "upload",
        }

    def _build_project_record_meta(
        self,
        *,
        record: dict[str, Any],
        payload: dict[str, Any],
        role: str,
        bidder_key: str | None,
        file_name_key: str,
        file_url_key: str,
        identifier_key: str,
    ) -> dict[str, Any]:
        """根据数据库记录构建文档元数据，来源为已存储文档。"""
        data_node = self._data_node(payload)
        raw_bytes = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        return {
            "role": role,
            "bidder_key": bidder_key,
            "identifier_id": str(record.get(identifier_key) or "").strip() or None,
            "file_name": str(record.get(file_name_key) or "").strip() or None,
            "file_path": str(record.get(file_url_key) or "").strip() or None,
            "file_size": len(raw_bytes),
            "modified_at": self._utc_now_iso(),
            "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "layout_section_count": len(data_node.get("layout_sections", []) or []),
            "logical_table_count": len(data_node.get("logical_tables", []) or []),
            "native_table_count": len(data_node.get("native_tables", []) or []),
            "page_count": self._page_count(data_node),
            "source_type": "stored_document",
        }

    # 数据节点提取
    def _data_node(self, payload: dict[str, Any]) -> dict[str, Any]:
        """提取 payload 中的 'data' 节点，如果不存在则返回自身。"""
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    def _coerce_stored_payload(self, value: Any) -> dict[str, Any]:
        """将可能是 JSON 字符串的存储值安全转为字典。"""
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    return {}
                return parsed if isinstance(parsed, dict) else {}
        return {}

    # 项目标识相关辅助
    def _default_project_identifier(self, dataset_dir: Path) -> str:
        """基于目录路径生成默认的项目标识。"""
        digest = hashlib.sha1(str(dataset_dir).encode("utf-8")).hexdigest()[:10]
        return f"unified_business_review_{digest}"

    def _get_or_create_project(self, identifier_id: str) -> dict[str, Any]:
        """查找或创建项目。"""
        project = self.db_service.get_project_by_identifier(identifier_id)
        if project:
            return project
        return self.db_service.create_project(identifier_id)

    def _ensure_project(self, project_identifier: str | None) -> dict[str, Any]:
        """确保项目存在（若指定标识则查找/创建，否则自动创建）。"""
        normalized_identifier = (project_identifier or "").strip()
        if normalized_identifier:
            existing = self.db_service.get_project_by_identifier(normalized_identifier)
            if existing:
                return existing
            return self.db_service.create_project(normalized_identifier)
        return self.db_service.create_project()

    # 投标人标识处理
    def _derive_project_bidder_key(self, file_name: Any, fallback: str) -> str:
        """从文件名推导投标人标识。"""
        stem = Path(str(file_name or "").strip()).stem.strip()
        normalized = self.BUSINESS_FILE_RE.sub("", stem).strip()
        return normalized or stem or fallback

    def _ensure_project_bidder_key(self, candidate: str, used: set[str]) -> str:
        """确保投标人标识在批次内唯一，必要时加数字后缀。"""
        base = str(candidate or "").strip() or "unknown_bidder"
        unique = base
        suffix = 2
        while unique in used:
            unique = f"{base}_{suffix}"
            suffix += 1
        used.add(unique)
        return unique

    def _normalize_project_document_role(self, value: Any) -> str:
        """将文档角色归一化为内部常量。"""
        normalized = str(value or "").strip().lower()
        if normalized in {"business", "business_bid"}:
            from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID
            return DOCUMENT_TYPE_BUSINESS_BID
        return normalized

    # UTC 时间
    @staticmethod
    def _utc_now_iso() -> str:
        """返回当前 UTC 时间的 ISO 格式字符串。"""
        return datetime.now(timezone.utc).isoformat()