# -*- coding: utf-8 -*-
"""偏离条款合规性检查器（最终组合类）"""
from .constants import DeviationConstants
from .text_utils import TextUtilsMixin
from .mixins.parse import ParseMixin
from .mixins.star_extract import StarExtractMixin
from .mixins.deviation_table import DeviationTableMixin
from .mixins.match import MatchMixin
from .mixins.results import ResultsMixin


class DeviationChecker(
    DeviationConstants,
    TextUtilsMixin,
    ParseMixin,
    StarExtractMixin,
    DeviationTableMixin,
    MatchMixin,
    ResultsMixin,
):
    """检查星号条款响应，并识别偏离表中投标人主动声明的偏离。"""

    pass
