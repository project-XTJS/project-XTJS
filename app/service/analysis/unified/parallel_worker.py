# -*- coding: utf-8 -*-
"""商务标格式审查的多进程并行 worker。

每个投标方的审查（完整性/模板一致性/报价/分项报价/签字盖章）相互独立，
通过 ProcessPoolExecutor(spawn) 在多个 CPU 进程并行执行，充分利用多核。
worker 内新建独立的 UnifiedBusinessReviewService 实例，不共享父进程状态。
"""

from __future__ import annotations

from typing import Any


def run_business_bidder_review(task: dict[str, Any]) -> dict[str, Any]:
    """在子进程内对单个投标方执行完整商务标审查，返回审查结果字典。"""
    from app.service.analysis.unified import UnifiedBusinessReviewService

    service = UnifiedBusinessReviewService(db_service=None)
    review = service._review_business_bidder(
        tender_payload=task["tender_payload"],
        tender_meta=task["tender_meta"],
        bidder_key=task["bidder_key"],
        business_payload=task["business_payload"],
        business_meta=task["business_meta"],
    )
    return review
