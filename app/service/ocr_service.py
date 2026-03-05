import os
from typing import List, Dict, Any
from pydantic import BaseModel


class OCRService:
    def __init__(self):
        # 初始化OCR服务
        self.available = False
        self.ocr = None
        try:
            # 尝试导入PaddleOCR
            from paddleocr import PaddleOCR
            # 初始化PaddleOCR
            self.ocr = PaddleOCR(use_angle_cls=False, lang='ch', use_gpu=False, show_log=False)
            self.available = True
            print("PaddleOCR initialized successfully")
        except ImportError as e:
            print(f"ImportError: {e}")
        except Exception as e:
            print(f"Error initializing PaddleOCR: {e}")
            import traceback
            traceback.print_exc()

    def recognize_text(self, image_path: str) -> str:
        """识别图片中的文本"""
        if not self.available:
            return "PaddleOCR is not available. Please check if PaddleOCR is installed correctly."
        
        try:
            result = self.ocr.ocr(image_path, cls=True)
            text = ""
            for line in result:
                for word in line:
                    text += word[1][0] + " "
            return text.strip()
        except Exception as e:
            return f"Error during text recognition: {e}"

    def recognize_pdf(self, pdf_path: str) -> str:
        """识别PDF文件中的文本"""
        if not self.available:
            return "PaddleOCR is not available. Please check if PaddleOCR is installed correctly."
        
        try:
            # 使用pymupdf将PDF转换为图片
            import fitz
            doc = fitz.open(pdf_path)
            text = ""
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                pix = page.get_pixmap()
                # 保存为临时图片
                temp_image_path = f"temp_page_{page_num}.png"
                pix.save(temp_image_path)
                # 识别图片中的文本
                page_text = self.recognize_text(temp_image_path)
                text += page_text + "\n"
                # 清理临时图片
                if os.path.exists(temp_image_path):
                    os.unlink(temp_image_path)
            return text.strip()
        except Exception as e:
            return f"Error during PDF recognition: {e}"

    def recognize_bytes(self, image_bytes: bytes) -> str:
        """识别字节流中的文本"""
        if not self.available:
            return "PaddleOCR is not available. Please check if PaddleOCR is installed correctly."
        
        try:
            # 将字节流转换为图片
            import cv2
            import numpy as np
            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            # 保存为临时图片
            temp_image_path = "temp_image.png"
            cv2.imwrite(temp_image_path, img)
            # 识别文本
            text = self.recognize_text(temp_image_path)
            # 清理临时图片
            if os.path.exists(temp_image_path):
                os.unlink(temp_image_path)
            return text
        except Exception as e:
            return f"Error during byte stream recognition: {e}"

    def is_available(self) -> bool:
        """检查PaddleOCR是否可用"""
        return self.available
