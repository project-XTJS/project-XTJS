"""MacBERT Chinese spelling correction worker used by the idle manager."""

import argparse
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BertForMaskedLM, BertForTokenClassification, BertTokenizerFast


CHINESE_CHARACTER = re.compile(r"[\u4e00-\u9fff]")
CEC3_PROMPT = (
    "你是中文错别字纠正专家。只纠正输入文本中的错别字，保持字符数量、"
    "语法、标点、数字和专有名称不变；没有明确错别字时原样输出。"
    "只输出纠正后的文本，不添加解释。输入文本："
)


class MacBertWorker:
    def __init__(self, model_path: str, cec3_model_path: str | None = None, device: str = "auto", detector_path: str | None = None):
        if device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = torch.device(device)
        self.tokenizer = BertTokenizerFast.from_pretrained(
            model_path, local_files_only=True
        )
        self.model = BertForMaskedLM.from_pretrained(
            model_path, local_files_only=True
        )
        self.model.eval().to(self.device)
        self.cec3_model_path = cec3_model_path
        self.cec3_tokenizer = None
        self.cec3_model = None
        self.detector_path = detector_path
        self.detector = None
        self.detector_tokenizer = None

    def detect(self, text: str) -> list[float | None]:
        """Score original characters only; no proposed correction enters this model."""
        if not self.detector_path:
            raise RuntimeError("detector_not_configured")
        if self.detector is None:
            torch.set_num_threads(min(4, os.cpu_count() or 1))
            self.detector_tokenizer = BertTokenizerFast.from_pretrained(self.detector_path, local_files_only=True)
            self.detector = BertForTokenClassification.from_pretrained(
                self.detector_path, local_files_only=True
            ).eval().to("cpu")
        encoded = self.detector_tokenizer(
            text, return_offsets_mapping=True, return_tensors="pt", truncation=False,
        )
        offsets = encoded.pop("offset_mapping")[0].tolist()
        if encoded["input_ids"].shape[1] > self.detector.config.max_position_embeddings:
            raise ValueError("text_too_long")
        with torch.inference_mode():
            probabilities = self.detector(**encoded).logits.softmax(dim=-1)[0, :, 1].tolist()
        scores: list[float | None] = [None] * len(text)
        for (start, end), score in zip(offsets, probabilities):
            if end == start + 1 and 0 <= start < len(text) and CHINESE_CHARACTER.fullmatch(text[start]):
                scores[start] = float(score)
        return scores

    def cec3_correct(self, text: str) -> str:
        if not self.cec3_model_path:
            raise RuntimeError("cec3_model_not_configured")
        if self.cec3_model is None:
            self.cec3_tokenizer = AutoTokenizer.from_pretrained(
                self.cec3_model_path, local_files_only=True
            )
            dtype = torch.bfloat16 if self.device.type == "cuda" else torch.float32
            self.cec3_model = AutoModelForCausalLM.from_pretrained(
                self.cec3_model_path,
                dtype=dtype,
                local_files_only=True,
            ).eval().to(self.device)
        prompt = self.cec3_tokenizer.apply_chat_template(
            [{"role": "user", "content": CEC3_PROMPT + text}],
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False,
        )
        inputs = self.cec3_tokenizer([prompt], return_tensors="pt").to(self.device)
        max_new_tokens = min(384, int(inputs["input_ids"].shape[1]) + 32)
        with torch.inference_mode():
            output = self.cec3_model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=False,
                pad_token_id=self.cec3_tokenizer.eos_token_id,
            )
        generated = output[0, inputs["input_ids"].shape[1]:]
        return self.cec3_tokenizer.decode(generated, skip_special_tokens=True).strip()

    def analyze(
        self,
        text: str,
        min_probability: float,
        min_probability_ratio: float,
        top_k: int = 1,
    ) -> dict:
        encoded = self.tokenizer(
            text,
            return_offsets_mapping=True,
            return_tensors="pt",
            truncation=False,
        )
        offsets = encoded.pop("offset_mapping")[0].tolist()
        inputs = {key: value.to(self.device) for key, value in encoded.items()}
        if inputs["input_ids"].shape[1] > self.model.config.max_position_embeddings:
            raise ValueError("text_too_long")
        with torch.inference_mode():
            logits = self.model(**inputs).logits[0]
            probabilities = torch.softmax(logits.float(), dim=-1)
            confidence, token_ids = probabilities.topk(16 if top_k > 1 else 1, dim=-1)
            positions = torch.arange(inputs["input_ids"].shape[1], device=self.device)
            source_probability = probabilities[
                positions, inputs["input_ids"][0]
            ]
        output = list(text)
        candidates = []
        for index, (start, end) in enumerate(offsets):
            if end - start != 1 or not CHINESE_CHARACTER.fullmatch(text[start:end]):
                continue
            original_probability = float(source_probability[index])
            accepted = 0
            for predicted_id, score in zip(token_ids[index], confidence[index]):
                predicted = self.tokenizer.convert_ids_to_tokens(int(predicted_id))
                candidate_probability = float(score)
                probability_ratio = candidate_probability / max(original_probability, 1e-12)
                if (
                    len(predicted) == 1
                    and CHINESE_CHARACTER.fullmatch(predicted)
                    and predicted != text[start:end]
                    and candidate_probability >= min_probability
                    and probability_ratio >= min_probability_ratio
                ):
                    if not accepted:
                        output[start] = predicted
                    candidates.append({
                        "start": start, "end": end, "original": text[start:end],
                        "replacement": predicted,
                        "candidate_probability": candidate_probability,
                        "source_probability": original_probability,
                        "probability_ratio": probability_ratio,
                    })
                    accepted += 1
                    if accepted >= top_k:
                        break
        return {"corrected_text": "".join(output), "candidates": candidates}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--cec3-model", required=True)
    parser.add_argument("--detector-model", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--device", default="auto", choices=("auto", "cpu", "cuda"))
    args = parser.parse_args()
    worker = MacBertWorker(args.model, args.cec3_model, args.device, args.detector_model)

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, data):
            body = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                self.reply(200, {"status": "ready", "device": str(worker.device)})
            else:
                self.reply(404, {"error": "not_found"})

        def do_POST(self):
            if self.path not in {"/correct", "/cec3", "/detect"}:
                return self.reply(404, {"error": "not_found"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 20000:
                    raise ValueError("invalid_request_size")
                body = json.loads(self.rfile.read(size))
                text = body["text"]
                if not isinstance(text, str) or not 0 < len(text) <= 300:
                    raise ValueError("invalid_request")
                if self.path == "/cec3":
                    self.reply(200, {"corrected_text": worker.cec3_correct(text)})
                elif self.path == "/detect":
                    self.reply(200, {"scores": worker.detect(text)})
                else:
                    min_probability = float(body["min_probability"])
                    min_probability_ratio = float(body["min_probability_ratio"])
                    top_k = int(body["top_k"])
                    if not 0.0 <= min_probability <= 1.0 or min_probability_ratio < 0.0 or top_k != 2:
                        raise ValueError("invalid_request")
                    result = worker.analyze(text, min_probability, min_probability_ratio, top_k)
                    self.reply(200, {
                        **result,
                        "min_probability": min_probability,
                        "min_probability_ratio": min_probability_ratio,
                    })
            except (ValueError, KeyError, TypeError) as exc:
                self.reply(400, {"error": str(exc)})
            except Exception as exc:
                self.reply(503, {"error": "cec3_inference_failed", "detail": type(exc).__name__})

        def log_message(self, format, *args):
            pass

    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
