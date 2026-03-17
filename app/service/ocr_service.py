import os
import cv2
import fitz  # PyMuPDF
import numpy as np
from tqdm import tqdm
import re

class OCRService:
    def __init__(self):
        self.available = False
        self.ocr = None
        # 定义印章保存路径
        self.seal_dir = "output_seals"
        if not os.path.exists(self.seal_dir):
            os.makedirs(self.seal_dir)
        
        try:
            from paddleocr import PaddleOCR
            # 初始化最新版 PaddleX 架构引擎
            self.ocr = PaddleOCR(lang='ch') 
            self.available = True
            print(f"OCRService: PaddleOCR 加载成功")
            print(f"印章截图将保存至: {self.seal_dir}")
        except Exception as exc:
            print(f"OCRService 加载失败: {exc}")

    def extract_all(self, file_path: str, file_type: str = "pdf") -> dict:
        if not self.available:
            return {"text": "", "pages": [], "seals": {"count": 0, "texts": [], "locations": []}}

        ext = file_type.lower().lstrip('.')
        if ext == "pdf":
            return self._recognize_pdf(file_path)
        else:
            return self._recognize_image(file_path)

    def _extract_text_from_result(self, ocr_res, join_char="\n"):
        """
        数据解析器
        """
        if not ocr_res:
            return ""
        
        texts = []
        try:
            # 遍历返回的结果（通常是一个列表，里面包含字典）
            for item in ocr_res:
                # 1. 适配最新版 PaddleX (字典格式)
                if isinstance(item, dict) and 'rec_texts' in item:
                    texts.extend(item['rec_texts'])
                # 适配如果它返回的是对象
                elif hasattr(item, 'rec_texts'):
                    texts.extend(getattr(item, 'rec_texts'))
                # 2. 适配老版本嵌套列表格式 (兜底逻辑)
                elif isinstance(item, list):
                    for line in item:
                        if isinstance(line, list) and len(line) == 2 and isinstance(line[1], tuple):
                            texts.append(str(line[1][0]))
        except Exception as e:
            print(f"警告: {e}")

        # 过滤掉空的字符串并拼接
        return join_char.join([str(t) for t in texts if t])

    def _detect_seals(self, img_bgr, page_no: int = 1) -> dict:
        seal_info = {"count": 0, "texts": [], "locations": []}
        if img_bgr is None:
            return seal_info

        hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
        lower_red1, upper_red1 = np.array([0, 43, 46]), np.array([10, 255, 255])
        lower_red2, upper_red2 = np.array([156, 43, 46]), np.array([180, 255, 255])
        mask = cv2.add(cv2.inRange(hsv, lower_red1, upper_red1), 
                        cv2.inRange(hsv, lower_red2, upper_red2))

        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        mask = cv2.dilate(mask, kernel, iterations=2)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        seal_idx = 0
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area > 1200:
                x, y, w, h = cv2.boundingRect(cnt)
                aspect_ratio = float(w) / h
                if 0.5 < aspect_ratio < 2.0:
                    seal_idx += 1
                    box = [int(x), int(y), int(w), int(h)]
                    
                    seal_crop = img_bgr[y:y+h, x:x+w]
                    save_path = os.path.join(self.seal_dir, f"seal_P{page_no}_{seal_idx}.png")
                    cv2.imwrite(save_path, seal_crop)
                    
                    resized_crop = cv2.resize(seal_crop, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
                    
                    crop_res = list(self.ocr.predict(resized_crop))
                    
                    # 提取印章文字
                    txt = self._extract_text_from_result(crop_res, join_char="")
                    clean_txt = re.sub(r'[〇一二三四五六七八九十月年\d\-\.：:（）\(\)]', '', txt)
                    if len(clean_txt) > 2: 
                        seal_info["texts"].append(clean_txt)
                    
                    seal_info["count"] += 1
                    seal_info["locations"].append(box)

        return seal_info

    def _recognize_pdf(self, pdf_path: str) -> dict:
        pages_data, full_text = [], []
        all_seals = {"count": 0, "texts": [], "locations": []}
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        pbar = tqdm(range(total_pages), desc="解析中", unit="页")
        
        for i in pbar:
            page_no = i + 1
            page = doc[i]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
            
            if pix.n == 4: img = cv2.cvtColor(img, cv2.COLOR_RGBA2BGR)
            elif pix.n == 3: img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
            else: img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
            
            ocr_res = list(self.ocr.predict(img))
            
            # 提取整页正文
            p_txt = self._extract_text_from_result(ocr_res, join_char="\n")
            
            s_res = self._detect_seals(img, page_no=page_no)
            
            if s_res["count"] > 0:
                all_seals["count"] += s_res["count"]
                all_seals["texts"].extend(s_res["texts"])
                for box in s_res["locations"]:
                    all_seals["locations"].append({"page": page_no, "box": box})
            
            pages_data.append({"page": page_no, "text": p_txt})
            full_text.append(p_txt)
            pbar.set_postfix({"印章数": all_seals["count"]})

        all_seals["texts"] = list(set(all_seals["texts"]))
        return {"text": "\n".join(full_text), "pages": pages_data, "seals": all_seals}

    def _recognize_image(self, img_path: str) -> dict:
        img = cv2.imread(img_path)
        if img is None:
            return {"text": "", "pages": [], "seals": {"count": 0, "texts": [], "locations": []}}
            
        ocr_res = list(self.ocr.predict(img))
        
        # 提取图片正文
        text = self._extract_text_from_result(ocr_res, join_char="\n")
        
        seal_res = self._detect_seals(img, page_no=1)
        formatted_locations = [{"page": 1, "box": box} for box in seal_res["locations"]]
        
        return {
            "text": text,
            "pages": [{"page": 1, "text": text}],
            "seals": {
                "count": seal_res["count"],
                "texts": seal_res["texts"],
                "locations": formatted_locations
            }
        }