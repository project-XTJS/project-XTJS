from celery import Celery
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 创建 Celery 实例
celery_app = Celery(
    'tasks',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
)

# 配置 Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1
)

# 自动发现任务
celery_app.autodiscover_tasks(['app.tasks'])