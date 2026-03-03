from app.tasks import celery_app
from app.service.business_format.service import BusinessFormatService
from app.service.business_duplication.service import BusinessDuplicationService
from app.service.technical_duplication.service import TechnicalDuplicationService

@celery_app.task
def business_format_check(text: str):
    """商务标格式审查任务"""
    return BusinessFormatService.check_format(text)

@celery_app.task
def business_duplication_check(text: str, historical_texts=None):
    """商务标查重任务"""
    if historical_texts is None:
        historical_texts = []
    return BusinessDuplicationService.check_duplication(text, historical_texts)

@celery_app.task
def business_quote_check(text: str, historical_quotes=None):
    """商务标报价查重任务"""
    if historical_quotes is None:
        historical_quotes = []
    return BusinessDuplicationService.check_quote_duplication(text, historical_quotes)

@celery_app.task
def technical_duplication_check(text: str, historical_texts=None):
    """技术标查重任务"""
    if historical_texts is None:
        historical_texts = []
    return TechnicalDuplicationService.check_duplication(text, historical_texts)

@celery_app.task
def technical_content_check(text: str):
    """技术标内容检查任务"""
    return TechnicalDuplicationService.check_technical_content(text)