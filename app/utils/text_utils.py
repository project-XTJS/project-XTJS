import os
import re
import shutil
import subprocess
import tempfile
from typing import List

from docx import Document
from pdfplumber import open as open_pdf

UNSUPPORTED_WORD_EXTENSIONS = {"doc", "docx"}

# 文本处理与切片
def preprocess_text(text: str) -> str:
    """文本预处理：去首尾空白并压缩连续空白字符。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())

def split_text(text: str, chunk_size: int = 1000, overlap: int = 150) -> List[str]:
    """
    文本切片：按固定窗口分段并保留重叠区。
    优化：对于超长文本，内部逻辑更精简，避免重复的字符串拷贝计算。
    """
    cleaned = preprocess_text(text)
    if not cleaned:
        return []

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")

    chunks: List[str] = []
    text_length = len(cleaned)
    start = 0
    
    while start < text_length:
        end = min(start + chunk_size, text_length)
        chunks.append(cleaned[start:end])
        if end == text_length:
            break
        # 前进 chunk_size 的长度，但回退 overlap 的长度
        start = end - overlap
        
    return chunks

# 文件文本抽取
def extract_text_from_pdf(file_path: str) -> str:
    """从 PDF 中抽取可直接识别的文本层内容。"""
    return "\n".join(extract_text_pages_from_pdf(file_path)).strip()

def extract_text_pages_from_pdf(file_path: str) -> List[str]:
    """从 PDF 中按页抽取可直接识别的文本层内容。"""
    page_texts: List[str] = []
    with open_pdf(file_path) as pdf:
        for page in pdf.pages:
            # 兼容某些全是图片的 PDF，extract_text() 可能返回 None
            extracted = page.extract_text()
            page_texts.append(extracted if extracted else "")
    return page_texts

def extract_text_from_docx(file_path: str) -> str:
    """从 docx 文档中按段落抽取文本。"""
    doc = Document(file_path)
    return "\n".join(paragraph.text for paragraph in doc.paragraphs)

def _decode_command_output(raw: bytes) -> str:
    """解码命令行输出文本，兼容常见中文编码。"""
    if not raw:
        return ""
    for encoding in ("utf-8", "gb18030", "gbk"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")

def _extract_text_from_doc_by_command(file_path: str, command: List[str]) -> str:
    """通过外部命令抽取 .doc 文本（增加超时控制防挂起）。"""
    try:
        # 增加 timeout=30，防止进程死锁
        result = subprocess.run(command, capture_output=True, check=False, timeout=30)
        if result.returncode != 0:
            stderr = _decode_command_output(result.stderr).strip()
            raise RuntimeError(stderr or f"命令执行失败，状态码: {result.returncode}")

        text = _decode_command_output(result.stdout).strip()
        if not text:
            raise RuntimeError("命令执行成功但未抽取到文本内容。")
        return text
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"命令执行超时 ({command[0]})")

def _extract_text_from_doc_via_libreoffice(file_path: str) -> str:
    """通过 LibreOffice 将 .doc 转换为 .docx 后再解析。"""
    soffice = shutil.which("soffice")
    if not soffice:
        raise RuntimeError("未安装 LibreOffice（soffice）。")

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # 增加 timeout=60，LibreOffice 极易因为字体或格式损坏而永远挂起
            result = subprocess.run(
                [
                    soffice,
                    "--headless",
                    "--invisible",
                    "--nocrashreport",   # 生产环境必备参数：禁止弹出崩溃报告框
                    "--nodefault",
                    "--nofirststartwizard",
                    "--nologo",
                    "--norestore",
                    "--convert-to",
                    "docx",
                    "--outdir",
                    temp_dir,
                    file_path,
                ],
                capture_output=True,
                check=False,
                timeout=60
            )
            if result.returncode != 0:
                stderr = _decode_command_output(result.stderr).strip()
                raise RuntimeError(stderr or "LibreOffice 转换失败。")

            converted_name = f"{os.path.splitext(os.path.basename(file_path))[0]}.docx"
            converted_path = os.path.join(temp_dir, converted_name)
            if not os.path.exists(converted_path):
                raise RuntimeError("LibreOffice 转换成功但未找到输出文件。")
            return extract_text_from_docx(converted_path)
            
        except subprocess.TimeoutExpired:
            raise RuntimeError("LibreOffice 转换超时。")

def _extract_text_from_doc_via_word(file_path: str) -> str:
    """Windows 环境下通过 Word COM 将 .doc 转换为 .docx 后再解析。"""
    if os.name != "nt":
        raise RuntimeError("当前系统不支持 Word COM 转换。")

    try:
        import win32com.client  # type: ignore
        import pythoncom        # type: ignore
    except ImportError as exc:
        raise RuntimeError("未安装 pywin32，无法使用 Word COM 转换。") from exc

    # 在多线程（如 FastAPI/Celery）中调用 COM 必须先初始化套间
    pythoncom.CoInitialize()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        converted_path = os.path.join(
            temp_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}.docx"
        )
        word = None
        doc = None
        try:
            word = win32com.client.Dispatch("Word.Application")
            word.Visible = False
            word.DisplayAlerts = 0  # 生产必备：禁止弹出任何警告框阻止代码执行
            
            doc = word.Documents.Open(file_path, ConfirmConversions=False, ReadOnly=True)
            # 16 = wdFormatDocumentDefault (.docx)
            doc.SaveAs(converted_path, FileFormat=16)
        except Exception as e:
            raise RuntimeError(f"Word COM 执行异常: {e}")
        finally:
            if doc is not None:
                try:
                    doc.Close(False)
                except Exception:
                    pass
            if word is not None:
                try:
                    word.Quit()
                except Exception:
                    pass
            # 释放 COM 资源
            pythoncom.CoUninitialize()

        if not os.path.exists(converted_path):
            raise RuntimeError("Word 转换成功但未找到输出文件。")
        return extract_text_from_docx(converted_path)

def extract_text_from_doc(file_path: str) -> str:
    """从 .doc 文档中抽取文本（多后端回退）。"""
    errors: List[str] = []

    # 1. 尝试轻量级命令行工具
    for tool_name in ("antiword", "catdoc"):
        tool_path = shutil.which(tool_name)
        if not tool_path:
            errors.append(f"{tool_name} 不可用")
            continue
        try:
            return _extract_text_from_doc_by_command(file_path, [tool_path, file_path])
        except RuntimeError as exc:
            errors.append(f"{tool_name} 失败: {exc}")

    # 2. 尝试 LibreOffice
    try:
        return _extract_text_from_doc_via_libreoffice(file_path)
    except RuntimeError as exc:
        errors.append(f"LibreOffice 失败: {exc}")

    # 3. 尝试 Word COM (仅限 Windows)
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
    normalized_type = file_type.lower().lstrip(".")
    if normalized_type in UNSUPPORTED_WORD_EXTENSIONS:
        raise ValueError("Word files are not supported")
    if normalized_type == "pdf":
        return extract_text_from_pdf(file_path)
    raise ValueError(f"Unsupported file type: {file_type}")

# 临时文件管理
def save_temp_file(content: bytes, suffix: str) -> str:
    """将上传字节保存为临时文件并返回路径。"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(content)
        return temp_file.name

def cleanup_temp_file(file_path: str) -> None:
    """删除临时文件（存在时）。"""
    if not file_path:
        return
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except Exception as e:
        # 日志记录清理失败，但不抛出异常中断主流程
        print(f"Warning: Failed to cleanup temp file {file_path}: {e}")

def extract_file_data(file_path: str, file_type: str) -> dict:
    """
    结构化抽取文件数据，返回包含分页信息、页数和解析引擎的字典。
    复用项目中已有的高质量抽取逻辑 (pdfplumber, image)。
    """
    normalized_type = file_type.lower().lstrip(".")
    if normalized_type in UNSUPPORTED_WORD_EXTENSIONS:
        raise ValueError("Word files are not supported")

    result = {
        "content": "",
        "pages": [],
        "page_count": 0,
        "parser_engine": "unknown"
    }
    
    try:
        if normalized_type == "pdf":
            # 复用 pdfplumber 分页抽取逻辑
            page_texts = extract_text_pages_from_pdf(file_path)
            result["page_count"] = len(page_texts)
            result["parser_engine"] = "pdfplumber"
            result["content"] = "\n".join(page_texts).strip()
            
            # 组装结构化分页数据
            for i, text in enumerate(page_texts):
                result["pages"].append({"page": i + 1, "text": text})
                
        elif normalized_type in ["jpg", "jpeg", "png"]:
            result["parser_engine"] = "image_input"
            result["page_count"] = 1
            result["pages"] = [{"page": 1, "text": ""}] # 留空，等待 analysis_service 里的 OCR 填充
            
        else:
            raise ValueError(f"Unsupported file type for structural extraction: {file_type}")
            
    except Exception as e:
        print(f"提取结构化文件数据异常 ({file_path}): {e}")
        # 异常时不抛出，返回空结构，防止整个流程阻断
        
    return result