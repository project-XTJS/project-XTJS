"""
报价合理性检查模块
负责人：曾俊、滑鹏鹏
"""

class ReasonablenessChecker:
    def check_price_reasonableness(self, text: str) -> dict:
        """
        TODO: 曾俊、滑鹏鹏 - 提取总报价，校验大写与小写是否匹配，判断报价是否偏离预算范围
        """
        # 初始逻辑占位
        has_price = "报价" in text or "金额" in text
        return {
            "has_price_info": has_price,
            "capital_matches_small": "pending",
            "price_score": 0
        }