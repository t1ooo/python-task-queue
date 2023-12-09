import peewee as pw


def peewee_sqlite_database(path: str) -> pw.SqliteDatabase:
    return pw.SqliteDatabase(
        path,
        pragmas={
            "journal_mode": "wal",
            "cache_size": -1 * 64000,  # 64MB
            "busy_timeout": 5 * 1000,  # 5 seconds
            "foreign_keys": 1,
            "ignore_check_constraints": 0,
            "synchronous": 0,
        },
    )