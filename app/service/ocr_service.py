import os
import cv2
import fitz  # PyMuPDF
import numpy as np
from tqdm import tqdm
import time

class OCRService:
    def __init__(self):
        self.available = False
        self.ocr = None
        # 定义印章保存路径
        self.seal_dir = "output_seals"
        if not os.path.exists(self.seal_dir):
            os.makedirs(self.seal_dir)
        
        try:
            from rapidocr_onnxruntime import RapidOCR
            self.ocr = RapidOCR(text_score=0.5, print_verbose=False) 
            self.available = True
            print(f"OCRService: RapidOCR 加载成功，印章保存至: {self.seal_dir}")
        except ImportError as exc:
            print(f"OCRService: 缺少依赖: {exc}")

    def extract_all(self, file_path: str, file_type: str = "pdf") -> dict:
        if not self.available:
            return {"text": "", "pages": [], "seals": {"count": 0, "texts": [], "locations": []}}

        ext = file_type.lower().lstrip('.')
        if ext == "pdf":
            return self._recognize_pdf(file_path)
        else:
            return self._recognize_image(file_path)

    def _detect_seals(self, img_bgr, page_no: int = 1) -> dict:
        """
        核心逻辑：检测印章、记录位置并保存截图
        """
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
            if area > 1000:
                x, y, w, h = cv2.boundingRect(cnt)
                aspect_ratio = float(w) / h
                if 0.5 < aspect_ratio < 2.0:
                    seal_idx += 1
                    box = [int(x), int(y), int(w), int(h)]
                    
                    # 裁剪印章区域
                    seal_crop = img_bgr[y:y+h, x:x+w]
                    
                    # 保存截图：文件名格式为 seal_P页码_N序号.png
                    save_path = os.path.join(self.seal_dir, f"seal_P{page_no}_{seal_idx}.png")
                    cv2.imwrite(save_path, seal_crop)
                    
                    # 更新信息
                    seal_info["count"] += 1
                    seal_info["locations"].append(box)
                    
                    # 局部 OCR
                    resized_crop = cv2.resize(seal_crop, None, fx=1.5, fy=1.5)
                    crop_res, _ = self.ocr(resized_crop)
                    if crop_res:
                        txt = "".join([item[1] for item in crop_res])
                        if len(txt) > 2: 
                            seal_info["texts"].append(txt)

        return seal_info

    def _recognize_pdf(self, pdf_path: str) -> dict:
        pages_data, full_text = [], []
        all_seals = {"count": 0, "texts": [], "locations": []}
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        pbar = tqdm(range(total_pages), desc="OCR解析中", unit="页")
        
        for i in pbar:
            page_no = i + 1
            page = doc[i]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
            
            if pix.n == 4: img = cv2.cvtColor(img, cv2.COLOR_RGBA2BGR)
            elif pix.n == 3: img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
            else: img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
            
            ocr_res, _ = self.ocr(img)
            p_txt = "\n".join([item[1] for item in ocr_res]) if ocr_res else ""
            
            # 传入页码以便保存文件名区分
            s_res = self._detect_seals(img, page_no=page_no)
            
            if s_res["count"] > 0:
                all_seals["count"] += s_res["count"]
                all_seals["texts"].extend(s_res["texts"])
                for box in s_res["locations"]:
                    all_seals["locations"].append({"page": page_no, "box": box})
            
            pages_data.append({"page": page_no, "text": p_txt})
            full_text.append(p_txt)

        all_seals["texts"] = list(set(all_seals["texts"]))
        return {"text": "\n".join(full_text), "pages": pages_data, "seals": all_seals}

    def _recognize_image(self, img_path: str) -> dict:
        img = cv2.imread(img_path)
        if img is None:
            return {"text": "", "pages": [], "seals": {"count": 0, "texts": [], "locations": []}}
            
        ocr_res, _ = self.ocr(img)
        text = "\n".join([item[1] for item in ocr_res]) if ocr_res else ""
        
        # 单张图片默认页码为 1
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