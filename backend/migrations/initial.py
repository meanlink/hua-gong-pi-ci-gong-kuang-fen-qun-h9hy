from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models  # noqa: F401  注册全部模型，create_all 才能建表

# 既有库就地补齐的列；create_all 只建缺失表，不会为老表加列。
ADDED_COLUMNS = {
    'jobs': [
        ('lease_epoch', 'INTEGER NOT NULL DEFAULT 0'),
    ],
    'reports': [
        ('created_by', "VARCHAR(150) NOT NULL DEFAULT ''"),
        ('restore_paused', 'BOOLEAN NOT NULL DEFAULT 0'),
    ],
}
MYSQL_TYPES = {
    'BOOLEAN NOT NULL DEFAULT 0': 'TINYINT(1) NOT NULL DEFAULT 0',
}


def migrate():
    Base.metadata.create_all(engine)
    dialect = engine.dialect.name
    if dialect not in ('mysql', 'sqlite'):
        return
    insp = inspect(engine)
    with engine.begin() as conn:
        for table, columns in ADDED_COLUMNS.items():
            existing = {c['name'] for c in insp.get_columns(table)}
            for name, ddl in columns:
                if name in existing:
                    continue
                if dialect == 'mysql':
                    ddl = MYSQL_TYPES.get(ddl, ddl)
                conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl}'))


if __name__ == '__main__':
    migrate()
