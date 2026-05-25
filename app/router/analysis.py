# -*- coding: utf-8 -*-
"""
OCR 抽取与规则分析路由。

提供文档解析（文本提取）、统一分析（文本/项目服务）等接口，
包含完整的文本清洗、结构化数据构建、项目级分析服务编排逻辑。
"""

import html
import json
import os
import re
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, UploadFile
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

from app.router.dependencies import (
    get_bid_document_review_service,
    get_db_service,
    get_duplicate_check_service,
    get_text_analysis_service,
)
from app.router.postgresql import (
    _ensure_project_ocr_idle,
    _ensure_project_analysis_status,
    _refresh_project_or_404,
    _resolve_project_typo_document_types,
    _run_project_duplicate_check,
    _run_project_personnel_reuse_check,
    _run_project_typo_check,
)
from app.schemas.analysis import TextAnalysisRequest
from app.schemas.recognition import build_analyze_file_metadata
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.postgresql_service import PostgreSQLService
from app.service.table_parser import build_logical_tables, build_table_structure
from app.utils.text_utils import cleanup_temp_file, preprocess_text, save_temp_file

router = APIRouter()

_RUN_ANALYSIS_OPENAPI_EXAMPLES = {
    "text_analysis_mode": {
        "summary": "文本分析模式",
        "description": "传入 task_type + text 执行单次文本分析，不需要 project_identifier 或 services。",
        "value": {
            "task_type": "integrity_check",
            "text": "请检查这份投标文件是否缺少必备附件。",
        },
    },
    "project_analysis_mode": {
        "summary": "项目分析模式",
        "description": "传入 project_identifier + services 执行项目级分析，不需要 task_type 或 text。",
        "value": {
            "project_identifier": "服务器设备",
            "services": [
                "business_bid_format_review",
                "business_bid_duplicate_check",
                "technical_bid_duplicate_check",
                "personnel_reuse_check",
                "typo_check",
            ],
            "max_evidence_sections": 5,
            "max_pairs_per_type": 0,
        },
    },
}

# 文本清洗辅助函数
def _clean_inline_text(value: Any) -> str:
    """
    行内文本精细化清洗：
    - 反转义 HTML 实体
    - 替换全角空格、不间断空格为普通空格
    - 压缩连续空白为单个空格
    - 移除中文之间的空格
    - 修正中英文标点前的多余空格
    """
    normalized = html.unescape(str(value or ""))
    normalized = normalized.replace("\u3000", " ")
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return ""
    # 去除中文之间的空格，避免分词打断
    normalized = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", normalized)
    # 去掉英文标点前的空格
    normalized = re.sub(r"\s+([,.;:!?%])", r"\1", normalized)
    # 去掉中文标点前的空格
    normalized = re.sub(r"\s+([，。；：！？、）】》])", r"\1", normalized)
    # 去掉中文左括号后的空格
    normalized = re.sub(r"([（【《])\s+", r"\1", normalized)
    return normalized.strip()


def _is_noise_line(line: str) -> bool:
    """
    判断一行是否为噪声行：
    - 空白行
    - 不包含任何数字、字母或中文字符（纯粹由标点符号等组成且长度 >= 2）
    """
    stripped = str(line or "").strip()
    if not stripped:
        return True
    if len(stripped) >= 2 and not re.search(r"[0-9A-Za-z\u4e00-\u9fff]", stripped):
        return True
    return False


def _build_clean_text(raw_value: Any) -> str:
    """
    从原始内容构建清洗后的完整文本：
    - 预处理换行符、全角空格等
    - 逐行清洗并过滤噪声行
    - 将所有有效行用空格连接为一段干净文本
    """
    normalized = html.unescape(str(raw_value or ""))
    normalized = normalized.replace("\u3000", " ")
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"\r\n?", "\n", normalized)
    normalized = normalized.replace("\t", " ")

    parts: list[str] = []
    for raw_line in normalized.splitlines():
        line = _clean_inline_text(raw_line)
        if not line or _is_noise_line(line):
            continue
        parts.append(line)

    return _clean_inline_text(" ".join(parts))


# 结构化数据构建（区段、表格）
def _build_public_sections(sections: list[dict] | None) -> list[dict]:
    """
    将 OCR 产出的区段列表转换为对外统一的格式：
    - 清洗文本
    - 标准化字段名（type, text, page, bbox, bbox_ocr）
    - 基于 (page, type, text, bbox) 去重
    """
    public_sections: list[dict] = []
    seen = set()

    for section in sections or []:
        if not isinstance(section, dict):
            continue
        text = _build_clean_text(section.get("raw_text") or section.get("text"))
        if not text:
            continue
        page = section.get("page")
        section_type = str(section.get("type") or "text").strip().lower() or "text"
        item = {"type": section_type, "text": text}
        if isinstance(page, int) and page > 0:
            item["page"] = page
        bbox = section.get("bbox") or section.get("box")
        if isinstance(bbox, (list, tuple)) and bbox:
            item["bbox"] = _build_public_native_table_value(list(bbox))
        bbox_ocr = section.get("bbox_ocr")
        if isinstance(bbox_ocr, (list, tuple)) and bbox_ocr:
            item["bbox_ocr"] = _build_public_native_table_value(list(bbox_ocr))
        signature = (item.get("page"), section_type, text, str(item.get("bbox")))
        if signature in seen:
            continue
        seen.add(signature)
        public_sections.append(item)

    return public_sections


def _build_public_native_table_value(value: Any) -> Any:
    """
    递归清洗一个值以用于对外暴露的 native_table / logical_table：
    - 基本类型直接返回
    - 字符串：HTML 反转义、全角空格处理、换行统一
    - 列表/字典：递归处理每个元素
    """
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        normalized = html.unescape(value)
        normalized = normalized.replace("\u3000", " ")
        normalized = normalized.replace("\xa0", " ")
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        return normalized.strip()
    if isinstance(value, list):
        return [_build_public_native_table_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _build_public_native_table_value(item)
            for key, item in value.items()
        }
    return html.unescape(str(value))


def _build_public_logical_tables(tables: list[dict] | None) -> list[dict]:
    """对外格式的表格列表清洗，直接应用 _build_public_native_table_value 到每个表格。"""
    public_tables: list[dict] = []
    for table in tables or []:
        if not isinstance(table, dict):
            continue
        public_tables.append(_build_public_native_table_value(table))
    return public_tables


def _rebuild_logical_tables_from_native(native_tables: list[dict] | None) -> list[dict]:
    """
    从 native_tables 中筛选出包含 HTML 表格的条目，重新解析出 logic_tables。
    用于当提取流水线未直接给出 logical_tables 时的兜底重建。
    """
    layout_sections: list[dict] = []
    for table in native_tables or []:
        if not isinstance(table, dict):
            continue
        block_content = str(table.get("block_content") or "").strip()
        if "<table" not in block_content.lower():
            continue
        table_structure = build_table_structure(html_parts=[block_content], raw_text="")
        if not isinstance(table_structure, dict):
            continue
        layout_sections.append(
            {
                "type": "table",
                "page": table.get("page"),
                "raw_text": block_content,
                "html": block_content,
                "table_structure": table_structure,
            }
        )
    if not layout_sections:
        return []
    return build_logical_tables(layout_sections)


# 单文件解析响应构建
def _build_analyze_file_response(
    upload: UploadFile,
    *,
    content: bytes,
    file_extension: str,
    extraction_result: dict,
) -> dict:
    """
    将 OCR 提取结果组装为统一的分析文件响应体，
    包含元数据、区段、逻辑表格、识别详情、印章/签名信息。
    """
    metadata = build_analyze_file_metadata(
        filename=upload.filename,
        file_type=file_extension,
        file_size=len(content),
        page_count=extraction_result["page_count"],
        mime_type=upload.content_type or "",
        text_length=extraction_result["text_length"],
        parser_engine=extraction_result["parser_engine"],
        source_mode=extraction_result["source_mode"],
        ocr_engine=extraction_result["ocr_engine"],
        ocr_used=extraction_result["ocr_used"],
        layout_used=extraction_result["layout_used"],
        layout_section_count=extraction_result["layout_section_count"],
        recognition_route=extraction_result["recognition_route"],
        recognition_reason=extraction_result["recognition_reason"],
        pdf_mode=extraction_result["pdf_mode"],
        active_device=extraction_result["active_device"],
        seal_detected=extraction_result["seal_detected"],
        seal_count=extraction_result["seal_count"],
        ppstructure_v3_requested=extraction_result["ppstructure_v3_requested"],
        ppstructure_v3_enabled=extraction_result["ppstructure_v3_enabled"],
        seal_recognition_enabled=extraction_result["seal_recognition_enabled"],
    )
    public_layout_sections = _build_public_sections(extraction_result["layout_sections"])
    rebuilt_logical_tables = _rebuild_logical_tables_from_native(extraction_result.get("native_tables"))
    public_native_tables = _build_public_logical_tables(extraction_result.get("native_tables"))
    public_logical_tables = _build_public_logical_tables(
        rebuilt_logical_tables or extraction_result.get("logical_tables") or extraction_result.get("native_tables")
    )

    return {
        "filename": upload.filename,
        "file_type": file_extension,
        "file_size": len(content),
        "text_length": extraction_result["text_length"],
        "page_count": extraction_result["page_count"],
        "layout_sections": public_layout_sections,
        "logical_tables": public_logical_tables,
        "native_tables": public_native_tables,
        "recognition": {
            "route": extraction_result["recognition_route"],
            "parser_engine": extraction_result["parser_engine"],
            "ocr_engine": extraction_result["ocr_engine"],
            "ocr_used": extraction_result["ocr_used"],
            "layout_used": extraction_result["layout_used"],
            "bbox_coordinate_space": extraction_result.get("bbox_coordinate_space", "ocr_image"),
            "bbox_source_coordinate_space": extraction_result.get("bbox_source_coordinate_space", "ocr_image"),
        },
        "seal": {
            "detected": extraction_result["seal_detected"],
            "count": extraction_result["seal_count"],
            "texts": extraction_result["seal_texts"],
            "locations": _build_public_native_table_value(extraction_result.get("seal_locations") or []),
        },
        "signature": {
            "detected": extraction_result["signature_detected"],
            "count": extraction_result["signature_count"],
            "texts": extraction_result["signature_texts"],
            "locations": _build_public_native_table_value(extraction_result.get("signature_locations") or []),
        },
        "metadata": metadata,
    }


# 源文件路径解析与校验
def _coerce_source_path(raw_value: Any) -> Path | None:
    """将输入值转为绝对路径，若为空则返回 None；若非绝对路径则抛出 400。"""
    if raw_value is None:
        return None

    path_text = str(raw_value).strip().strip('"').strip("'")
    if not path_text:
        return None
    if not os.path.isabs(path_text):
        raise HTTPException(
            status_code=400,
            detail="source_paths_json 必须填写绝对路径。",
        )
    return Path(path_text)


def _parse_source_paths_json(raw_value: str | None, expected_count: int) -> list[Path | None]:
    """
    解析 source_paths_json 表单值：
    - 空值或未提供时返回与文件数量一致的 None 列表
    - 单文件时可接收纯字符串路径
    - 多文件时必须是 JSON 数组且长度匹配
    """
    if expected_count <= 0:
        return []

    if raw_value is None or not str(raw_value).strip():
        return [None] * expected_count

    raw_text = str(raw_value).strip()
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        if expected_count == 1:
            return [_coerce_source_path(raw_text)]
        raise HTTPException(
            status_code=400,
            detail="source_paths_json 必须是与上传文件一一对应的 JSON 数组。",
        ) from None

    if isinstance(parsed, list):
        if len(parsed) != expected_count:
            raise HTTPException(
                status_code=400,
                detail="source_paths_json 的长度必须与上传文件数量一致。",
            )
        return [_coerce_source_path(item) for item in parsed]

    if expected_count == 1:
        return [_coerce_source_path(parsed)]

    raise HTTPException(
        status_code=400,
        detail="上传多个文件时，source_paths_json 必须是 JSON 数组。",
    )


def _resolve_source_path(upload: UploadFile, explicit_source_path: Path | None) -> Path | None:
    """
    确定最终用于保存 JSON 的源文件路径：
    优先使用显式指定的 source_path，否则尝试从文件名推导绝对路径。
    """
    if explicit_source_path is not None:
        return explicit_source_path

    filename = str(upload.filename or "").strip()
    if not filename or "fakepath" in filename.lower():
        return None
    if not os.path.isabs(filename):
        return None
    return Path(filename)


# JSON 保存结果构建
def _build_save_result(
    status: str,
    *,
    json_path: Path | None = None,
    message: str | None = None,
) -> dict:
    """构建保存操作的状态响应。"""
    result = {
        "status": status,
        "json_path": str(json_path) if json_path is not None else None,
    }
    if message:
        result["message"] = message
    return result


# 项目级分析服务编排
_PROJECT_SERVICE_RESULT_KEYS = {
    "business_bid_format_review": UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
    "business_bid_duplicate_check": "business_bid_duplicate_check",
    "technical_bid_duplicate_check": "technical_bid_duplicate_check",
    "personnel_reuse_check": "personnel_reuse_check",
    "typo_check": "typo_check",
}


def _normalize_selected_services(services: list[str] | None) -> list[str]:
    """去重并过滤空字符串，返回规范化的服务名列表。"""
    normalized: list[str] = []
    seen: set[str] = set()
    for service_name in services or []:
        token = str(service_name or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _http_exception_detail_to_text(detail: Any) -> str:
    """HTTPException 的 detail 转换为纯文本（用于统一错误记录）。"""
    if isinstance(detail, str):
        return detail
    return json.dumps(detail, ensure_ascii=False)


def _parse_task_json_object(
    raw_text: str,
    *,
    task_type: str,
) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{task_type} 要求 text 为 JSON 对象字符串。",
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail=f"{task_type} 要求 text 解析后为 JSON 对象。",
        )
    return parsed


def _coerce_task_document(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip().startswith("{"):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _resolve_consistency_documents(task_payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate_pairs = (
        ("model_json", "test_json"),
        ("model", "test"),
        ("tender_document", "bid_document"),
        ("tender_document", "business_bid_document"),
        ("tender", "bid"),
        ("招标文件", "投标文件"),
        ("招标", "投标"),
    )
    for model_key, test_key in candidate_pairs:
        model_json = _coerce_task_document(task_payload.get(model_key))
        test_json = _coerce_task_document(task_payload.get(test_key))
        if model_json is not None and test_json is not None:
            return model_json, test_json

    raise HTTPException(
        status_code=400,
        detail=(
            "consistency_check 要求 text 中提供成对文档 JSON，"
            "例如 model_json/test_json 或 tender_document/bid_document。"
        ),
    )


def _build_verification_payload(raw_text: str) -> dict[str, Any]:
    stripped = str(raw_text or "").strip()
    if not stripped:
        raise HTTPException(status_code=400, detail="verification_check 的 text 不能为空。")
    if stripped.startswith("{"):
        return _parse_task_json_object(stripped, task_type="verification_check")
    return {"content": raw_text}


async def _run_selected_project_services(
    *,
    identifier_id: str,
    selected_services: list[str],
    max_evidence_sections: int,
    max_pairs_per_type: int,
    db_service: PostgreSQLService,
    duplicate_check_service: Any,
    bid_document_review_service: Any,
) -> dict:
    """
    按请求的服务列表依次执行项目级分析任务：
    - 商务标格式审查
    - 商务标/技术标雷同检查
    - 人员复用检查
    - 错别字检查
    返回包含各服务执行状态和结果的聚合响应。
    """
    review_service = UnifiedBusinessReviewService(db_service=db_service)
    items: list[dict] = []
    results: dict[str, Any] = {}
    business_review_response: dict[str, Any] | None = None
    # 整个项目分析批次只取一次最新项目状态，后续各服务共用这份门槛判断。
    project = await run_in_threadpool(_refresh_project_or_404, db_service, identifier_id)
    _ensure_project_ocr_idle(project, analysis_name="项目业务分析")
    typo_document_types: list[str] | None = None

    async def _ensure_business_review_response() -> dict[str, Any]:
        nonlocal business_review_response
        if business_review_response is None:
            # 商务标形式审查只允许在商务标 OCR 完成后触发。
            _ensure_project_analysis_status(
                project,
                required_status=PostgreSQLService.PARSING_STATUS_BUSINESS_OCR_COMPLETED,
                analysis_name="商务标形式审查",
            )
            business_review_response = await run_in_threadpool(
                review_service.persist_project_business_review,
                project_identifier=identifier_id,
                result_key=UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
            )
        return business_review_response

    for service_name in selected_services:
        result_key = _PROJECT_SERVICE_RESULT_KEYS.get(service_name, service_name)
        try:
            if service_name == "business_bid_format_review":
                result = await _ensure_business_review_response()
            elif service_name == "business_bid_duplicate_check":
                _ensure_project_analysis_status(
                    project,
                    required_status=PostgreSQLService.PARSING_STATUS_BUSINESS_OCR_COMPLETED,
                    analysis_name="商务标查重",
                )
                result = await run_in_threadpool(
                    _run_project_duplicate_check,
                    identifier_id=identifier_id,
                    document_types=["business_bid"],
                    max_evidence_sections=max_evidence_sections,
                    max_pairs_per_type=max_pairs_per_type,
                    result_key="business_bid_duplicate_check",
                    db_service=db_service,
                    duplicate_check_service=duplicate_check_service,
                )
            elif service_name == "technical_bid_duplicate_check":
                _ensure_project_analysis_status(
                    project,
                    required_status=PostgreSQLService.PARSING_STATUS_TECHNICAL_OCR_COMPLETED,
                    analysis_name="技术标查重",
                )
                result = await run_in_threadpool(
                    _run_project_duplicate_check,
                    identifier_id=identifier_id,
                    document_types=["technical_bid"],
                    max_evidence_sections=max_evidence_sections,
                    max_pairs_per_type=max_pairs_per_type,
                    result_key="technical_bid_duplicate_check",
                    db_service=db_service,
                    duplicate_check_service=duplicate_check_service,
                )
            elif service_name == "personnel_reuse_check":
                _ensure_project_analysis_status(
                    project,
                    required_status=PostgreSQLService.PARSING_STATUS_BUSINESS_OCR_COMPLETED,
                    analysis_name="一人多用检查",
                )
                result = await run_in_threadpool(
                    _run_project_personnel_reuse_check,
                    identifier_id=identifier_id,
                    db_service=db_service,
                    bid_document_review_service=bid_document_review_service,
                )
            elif service_name == "typo_check":
                # 错别字范围依赖当前 OCR 阶段，按需延迟计算即可。
                typo_document_types = _resolve_project_typo_document_types(project)
                result = await run_in_threadpool(
                    _run_project_typo_check,
                    identifier_id=identifier_id,
                    db_service=db_service,
                    bid_document_review_service=bid_document_review_service,
                    document_types=typo_document_types,
                )
            else:
                raise HTTPException(status_code=400, detail=f"不支持的分析服务：{service_name}")
        except HTTPException as exc:
            items.append(
                {
                    "service": service_name,
                    "result_key": result_key,
                    "status": "failed",
                    "status_code": exc.status_code,
                    "error": _http_exception_detail_to_text(exc.detail),
                }
            )
            continue
        except ValueError as exc:
            items.append(
                {
                    "service": service_name,
                    "result_key": result_key,
                    "status": "failed",
                    "status_code": 400,
                    "error": str(exc),
                }
            )
            continue
        except PsycopgError as exc:
            items.append(
                {
                    "service": service_name,
                    "result_key": result_key,
                    "status": "failed",
                    "status_code": 500,
                    "error": f"数据库错误：{exc}",
                }
            )
            continue

        items.append(
            {
                "service": service_name,
                "result_key": result_key,
                "status": "success",
                "result": result,
            }
        )
        results[result_key] = result

    success_count = sum(1 for item in items if item.get("status") == "success")
    failed_count = len(items) - success_count
    if failed_count == 0:
        status = "success"
    elif success_count == 0:
        status = "failed"
    else:
        status = "partial_success"

    return {
        "mode": "project_analysis",
        "project_identifier": identifier_id,
        "requested_services": selected_services,
        "status": status,
        "summary": {
            "total": len(selected_services),
            "success": success_count,
            "failed": failed_count,
        },
        "results": results,
        "items": items,
    }


# 单文件解析及 JSON 保存
def _save_analyze_file_json(
    payload: dict,
    *,
    source_path: Path | None,
    enabled: bool,
) -> dict:
    """
    当 enabled 且提供了有效的源文件路径时，
    将解析结果 JSON 写入源文件同目录下（.json 后缀）。
    """
    if not enabled:
        return _build_save_result(
            "disabled",
            message="当前请求未启用 JSON 保存。",
        )

    if source_path is None:
        return _build_save_result(
            "skipped",
            message="未提供可用的源文件路径，因此未保存解析后的 JSON。",
        )

    target_path = source_path.with_suffix(".json")
    try:
        target_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        return _build_save_result(
            "failed",
            json_path=target_path,
            message=str(exc),
        )

    return _build_save_result(
        "saved",
        json_path=target_path,
        message="解析后的 JSON 已保存到源文件同目录。",
    )


async def _analyze_single_upload(
    upload: UploadFile,
    *,
    analysis_service: Any,
    explicit_source_path: Path | None,
    save_json_to_source: bool,
) -> dict:
    """
    处理单个文件上传的完整流程：
    校验扩展名 -> 保存临时文件 -> 调用 OCR 提取 -> 构建响应体 -> 可选保存 JSON。
    """
    allowed_extensions = set(analysis_service.get_supported_extensions())
    filename = str(upload.filename or "")
    file_extension = os.path.splitext(filename)[1].lower().lstrip(".")

    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=(
                f"不支持的文件类型：{file_extension}。"
                f"支持的类型：{', '.join(sorted(allowed_extensions))}。"
            ),
        )

    content = await upload.read()
    temp_file_path = save_temp_file(content, f".{file_extension}")

    try:
        extraction_result = await run_in_threadpool(
            analysis_service.extract_text_result,
            temp_file_path,
            file_extension,
        )
        payload = _build_analyze_file_response(
            upload,
            content=content,
            file_extension=file_extension,
            extraction_result=extraction_result,
        )
        _save_analyze_file_json(
            payload,
            source_path=_resolve_source_path(upload, explicit_source_path),
            enabled=save_json_to_source,
        )
        return payload
    finally:
        cleanup_temp_file(temp_file_path)


# 接口：文档解析（抽取文本）
@router.post("/analyze-file", summary="文档解析（抽取文本）")
async def analyze_file(
    file: list[UploadFile] = File(...),
    source_paths_json: str | None = Form(
        default=None,
        description="可选的 JSON 字符串或 JSON 数组，需与上传文件一一对应，路径将用于保存解析 JSON。",
    ),
    save_json_to_source: bool = Form(
        default=True,
        description="当存在可用源文件路径时，是否将每个解析结果 JSON 保存到源文件同目录。",
    ),
    analysis_service=Depends(get_text_analysis_service),
):
    """解析一个或多个上传文件，并可选择将 JSON 保存到源文件同目录。"""
    uploads = [upload for upload in file if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="未上传任何文件。")

    source_paths = _parse_source_paths_json(source_paths_json, len(uploads))

    # 单文件直接返回细节
    if len(uploads) == 1:
        try:
            return await _analyze_single_upload(
                uploads[0],
                analysis_service=analysis_service,
                explicit_source_path=source_paths[0],
                save_json_to_source=save_json_to_source,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    # 多文件时汇总每个文件的状态
    items: list[dict] = []
    success_count = 0

    for upload, source_path in zip(uploads, source_paths):
        try:
            result = await _analyze_single_upload(
                upload,
                analysis_service=analysis_service,
                explicit_source_path=source_path,
                save_json_to_source=save_json_to_source,
            )
        except (ValueError, RuntimeError) as exc:
            items.append(
                {
                    "filename": str(upload.filename or ""),
                    "status": "failed",
                    "error": str(exc),
                }
            )
            continue
        except HTTPException as exc:
            detail = exc.detail
            if not isinstance(detail, str):
                detail = json.dumps(detail, ensure_ascii=False)
            items.append(
                {
                    "filename": str(upload.filename or ""),
                    "status": "failed",
                    "error": detail,
                }
            )
            continue

        items.append(
            {
                "filename": str(upload.filename or ""),
                "status": "success",
                "result": result,
            }
        )
        success_count += 1

    failed_count = len(items) - success_count
    if failed_count == 0:
        overall_status = "success"
    elif success_count == 0:
        overall_status = "failed"
    else:
        overall_status = "partial_success"

    return {
        "status": overall_status,
        "total": len(uploads),
        "success": success_count,
        "failed": failed_count,
        "items": items,
    }


# 接口：统一分析（文本/项目服务）
@router.post("/run", summary="统一分析接口（文本/项目服务）")
async def run_text_analysis(
    payload: TextAnalysisRequest = Body(openapi_examples=_RUN_ANALYSIS_OPENAPI_EXAMPLES),
    analysis_service=Depends(get_text_analysis_service),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service=Depends(get_duplicate_check_service),
    bid_document_review_service=Depends(get_bid_document_review_service),
):
    """统一执行文本分析或项目级业务分析，根据传入参数自动路由。"""
    selected_services = _normalize_selected_services(payload.services)
    if selected_services:
        # 项目级分析模式下必须提供 project_identifier
        identifier_id = str(payload.project_identifier or "").strip()
        if not identifier_id:
            raise HTTPException(status_code=400, detail="project_identifier 不能为空。")
        return await _run_selected_project_services(
            identifier_id=identifier_id,
            selected_services=selected_services,
            max_evidence_sections=payload.max_evidence_sections,
            max_pairs_per_type=payload.max_pairs_per_type,
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
            bid_document_review_service=bid_document_review_service,
        )

    # 纯文本分析模式
    if payload.task_type is None:
        raise HTTPException(
            status_code=400,
            detail="请传入 task_type 执行文本分析，或传入 project_identifier + services 执行项目分析。",
        )

    raw_text = payload.text or ""
    if not raw_text.strip():
        raise HTTPException(status_code=400, detail="text 不能为空。")
    text = preprocess_text(raw_text)

    # 按任务类型分发
    if payload.task_type == "integrity_check":
        return analysis_service.integrity.check_integrity(text)
    if payload.task_type == "consistency_check":
        task_payload = _parse_task_json_object(raw_text, task_type="consistency_check")
        model_json, test_json = _resolve_consistency_documents(task_payload)
        return analysis_service.consistency.compare_raw_data(model_json, test_json)
    if payload.task_type == "verification_check":
        verification_payload = _build_verification_payload(raw_text)
        return analysis_service.verification.check_seal_and_date(verification_payload)
    if payload.task_type == "pricing_reason":
        return analysis_service.reasonableness.check_price_reasonableness(text)
    if payload.task_type == "itemized_pricing":
        return analysis_service.itemized.check_itemized_logic(raw_text)
    if payload.task_type == "deviation_check":
        return analysis_service.deviation.check_technical_deviation(text)
    if payload.task_type == "full_analysis":
        return analysis_service.run_full_analysis(text, extraction_meta={})

    raise HTTPException(status_code=400, detail=f"不支持的任务类型：{payload.task_type}")
