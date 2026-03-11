import os
import re
import shutil
import subprocess
import tempfile
from typing import List

from docx import Document
from pdfplumber import open as open_pdf


def preprocess_text(text: str) -> str:
    """文本预处理：去首尾空白并压缩连续空白字符。"""
    return re.sub(r"\s+", " ", text.strip())


def extract_text_from_pdf(file_path: str) -> str:
    """从 PDF 中抽取可直接识别的文本层内容。"""
    text = ""
    with open_pdf(file_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text


def extract_text_from_docx(file_path: str) -> str:
    """从 docx 文档中按段落抽取文本。"""
    doc = Document(file_path)
    return "\n".join(paragraph.text for paragraph in doc.paragraphs)


def _decode_command_output(raw: bytes) -> str:
    """解码命令行输出文本，兼容常见中文编码。"""
    for encoding in ("utf-8", "gb18030", "gbk"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _extract_text_from_doc_by_command(file_path: str, command: List[str]) -> str:
    """通过外部命令抽取 .doc 文本。"""
    result = subprocess.run(command, capture_output=True, check=False)
    if result.returncode != 0:
        stderr = _decode_command_output(result.stderr).strip()
        raise RuntimeError(stderr or "命令执行失败。")

    text = _decode_command_output(result.stdout).strip()
    if not text:
        raise RuntimeError("命令执行成功但未抽取到文本内容。")
    return text


def _extract_text_from_doc_via_libreoffice(file_path: str) -> str:
    """通过 LibreOffice 将 .doc 转换为 .docx 后再解析。"""
    soffice = shutil.which("soffice")
    if not soffice:
        raise RuntimeError("未安装 LibreOffice（soffice）。")

    with tempfile.TemporaryDirectory() as temp_dir:
        result = subprocess.run(
            [
                soffice,
                "--headless",
                "--convert-to",
                "docx",
                "--outdir",
                temp_dir,
                file_path,
            ],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = _decode_command_output(result.stderr).strip()
            raise RuntimeError(stderr or "LibreOffice 转换失败。")

        converted_name = f"{os.path.splitext(os.path.basename(file_path))[0]}.docx"
        converted_path = os.path.join(temp_dir, converted_name)
        if not os.path.exists(converted_path):
            raise RuntimeError("LibreOffice 转换成功但未找到输出文件。")
        return extract_text_from_docx(converted_path)


def _extract_text_from_doc_via_word(file_path: str) -> str:
    """Windows 环境下通过 Word COM 将 .doc 转换为 .docx 后再解析。"""
    if os.name != "nt":
        raise RuntimeError("当前系统不支持 Word COM 转换。")

    try:
        import win32com.client  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime optional dependency
        raise RuntimeError("未安装 pywin32，无法使用 Word COM 转换。") from exc

    with tempfile.TemporaryDirectory() as temp_dir:
        converted_path = os.path.join(
            temp_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}.docx"
        )
        word = None
        doc = None
        try:
            word = win32com.client.Dispatch("Word.Application")
            word.Visible = False
            doc = word.Documents.Open(file_path)
            # 16 = wdFormatDocumentDefault (.docx)
            doc.SaveAs(converted_path, FileFormat=16)
        finally:
            if doc is not None:
                doc.Close(False)
            if word is not None:
                word.Quit()

        if not os.path.exists(converted_path):
            raise RuntimeError("Word 转换成功但未找到输出文件。")
        return extract_text_from_docx(converted_path)


def extract_text_from_doc(file_path: str) -> str:
    """从 .doc 文档中抽取文本（多后端回退）。"""
    errors: List[str] = []

    for tool_name in ("antiword", "catdoc"):
        tool_path = shutil.which(tool_name)
        if not tool_path:
            errors.append(f"{tool_name} 不可用")
            continue
        try:
            return _extract_text_from_doc_by_command(file_path, [tool_path, file_path])
        except RuntimeError as exc:
            errors.append(f"{tool_name} 失败: {exc}")

    try:
        return _extract_text_from_doc_via_libreoffice(file_path)
    except RuntimeError as exc:
        errors.append(f"LibreOffice 失败: {exc}")

    try:
        return _extract_text_from_doc_via_word(file_path)
    except RuntimeError as exc:
        errors.append(f"Word COM 失败: {exc}")

    joined_errors = " | ".join(errors)
    raise ValueError(
        "当前环境无法解析 .doc 文件。请安装 antiword/catdoc，或安装 LibreOffice，"
        f"Windows 下可安装 pywin32 + Microsoft Word。详情: {joined_errors}"
    )


def extract_text(file_path: str, file_type: str) -> str:
    """根据文件类型分发到对应解析器。"""
    if file_type == "pdf":
        return extract_text_from_pdf(file_path)
    if file_type == "docx":
        return extract_text_from_docx(file_path)
    if file_type == "doc":
        return extract_text_from_doc(file_path)
    raise ValueError(f"Unsupported file type: {file_type}")


def split_text(text: str, chunk_size: int = 1000, overlap: int = 150) -> List[str]:
    """
    文本切片：按固定窗口分段并保留重叠区，便于后续相似度/向量检索。
    """
    cleaned = preprocess_text(text)
    if not cleaned:
        return []

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")

    chunks: List[str] = []
    start = 0
    while start < len(cleaned):
        # 逐段推进，并保留 overlap 长度上下文。
        end = min(start + chunk_size, len(cleaned))
        chunks.append(cleaned[start:end])
        if end == len(cleaned):
            break
        start = end - overlap
    return chunks


def save_temp_file(content: bytes, suffix: str) -> str:
    """将上传字节保存为临时文件并返回路径。"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(content)
        return temp_file.name


def cleanup_temp_file(file_path: str) -> None:
    """删除临时文件（存在时）。"""
    if os.path.exists(file_path):
        os.unlink(file_path)
