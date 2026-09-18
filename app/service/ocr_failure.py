"""Public OCR error descriptions. Never expose storage URLs, credentials or traces."""
from datetime import datetime, timezone


def describe_ocr_failure(error: object) -> dict:
    message = str(error).lower()
    if "pdfium" in message and ("data format error" in message or "failed to load document" in message):
        code, detail = "invalid_pdf", "PDF 无法解析，可能损坏或不完整。请检查原件能否正常打开，并使用“替换文件”上传有效 PDF。"
    elif any(term in message for term in ("password required", "password protected", "password error", "encrypted")):
        code, detail = "encrypted_pdf", "文件可能有密码保护，请确认后替换为可正常读取的 PDF。"
    elif "nosuchkey" in message or "nosuchbucket" in message:
        code, detail = "source_missing", "无法找到存储中的原文件，请联系管理员核对，或替换文件。"
    elif "accessdenied" in message or "nopermission" in message:
        code, detail = "storage_permission", "文件存储访问被拒绝，请联系管理员检查存储权限后重试。"
    elif "timeout" in message or "timed out" in message:
        code, detail = "timeout", "识别或文件读取超时，本轮未完成。可点击继续 OCR 重试；反复失败请联系管理员查看日志。"
    elif "out of memory" in message:
        code, detail = "resource_exhausted", "识别服务内存或显存不足，请联系管理员处理后重试。"
    else:
        code, detail = "recognition_failed", "识别未完成，详细原因已记录在后台日志。可重试；反复失败请联系管理员按文件名排查。"
    return {"code": code, "message": detail, "failed_at": datetime.now(timezone.utc).isoformat()}
