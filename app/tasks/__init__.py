from celery import Celery
from app.config.settings import settings

# 创建 Celery 实例
celery_app = Celery(
    'xtjs_tasks',  # 建议起一个带项目前缀的名字，避免在共用 Redis 时和其他项目冲突
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

# 配置 Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=True,
    task_acks_late=True,           # 任务执行完再确认，防止 worker 崩溃导致任务丢失（非常适合 OCR）
    worker_prefetch_multiplier=1,  # 每次只拿一个任务，防止长任务霸占队列
    broker_connection_retry_on_startup=True # 消除未来 Celery 版本的警告
)

# 自动发现任务：它会自动去寻找 app/tasks 目录下所有文件里带有 @celery_app.task 的函数
celery_app.autodiscover_tasks(['app'])