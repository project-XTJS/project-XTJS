import os


class PostgresConfig:
    """PostgreSQL 连接配置。"""

    # 兼容本地开发和容器部署，通过环境变量覆盖。
    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://admin:password@localhost:5432/xtjs_db"
    )
