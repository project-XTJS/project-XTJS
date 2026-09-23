"""MacBERT Chinese spelling correction worker used by the idle manager."""

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import torch
from transformers import BertForMaskedLM, BertTokenizerFast


CHINESE_CHARACTER = re.compile(r"[\u4e00-\u9fff]")


class MacBertWorker:
    def __init__(self, model_path: str, device: str = "auto"):
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

    def analyze(
        self,
        text: str,
        min_probability: float,
        min_probability_ratio: float,
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
            confidence, token_ids = probabilities.max(dim=-1)
            positions = torch.arange(inputs["input_ids"].shape[1], device=self.device)
            source_probability = probabilities[
                positions, inputs["input_ids"][0]
            ]
        output = list(text)
        candidates = []
        for index, (start, end) in enumerate(offsets):
            if end - start != 1 or not CHINESE_CHARACTER.fullmatch(text[start:end]):
                continue
            predicted = self.tokenizer.convert_ids_to_tokens(int(token_ids[index]))
            candidate_probability = float(confidence[index])
            original_probability = float(source_probability[index])
            probability_ratio = candidate_probability / max(original_probability, 1e-12)
            if (
                len(predicted) == 1
                and CHINESE_CHARACTER.fullmatch(predicted)
                and predicted != text[start:end]
                and candidate_probability >= min_probability
                and probability_ratio >= min_probability_ratio
            ):
                output[start] = predicted
                candidates.append(
                    {
                        "start": start,
                        "end": end,
                        "original": text[start:end],
                        "replacement": predicted,
                        "candidate_probability": candidate_probability,
                        "source_probability": original_probability,
                        "probability_ratio": probability_ratio,
                    }
                )
        return {"corrected_text": "".join(output), "candidates": candidates}

    def correct(
        self,
        text: str,
        min_probability: float,
        min_probability_ratio: float,
    ) -> str:
        """Compatibility wrapper for callers that only need corrected text."""
        return self.analyze(text, min_probability, min_probability_ratio)["corrected_text"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--device", default="auto", choices=("auto", "cpu", "cuda"))
    args = parser.parse_args()
    worker = MacBertWorker(args.model, args.device)

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
            if self.path != "/correct":
                return self.reply(404, {"error": "not_found"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 20000:
                    raise ValueError("invalid_request_size")
                body = json.loads(self.rfile.read(size))
                text = body["text"]
                min_probability = float(body["min_probability"])
                min_probability_ratio = float(body["min_probability_ratio"])
                if (
                    not isinstance(text, str)
                    or not 0 < len(text) <= 300
                    or not 0.0 <= min_probability <= 1.0
                    or min_probability_ratio < 1.0
                ):
                    raise ValueError("invalid_request")
                result = worker.analyze(text, min_probability, min_probability_ratio)
                self.reply(200, {
                    **result,
                    "min_probability": min_probability,
                    "min_probability_ratio": min_probability_ratio,
                })
            except (ValueError, KeyError, TypeError) as exc:
                self.reply(400, {"error": str(exc)})

        def log_message(self, format, *args):
            pass

    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
