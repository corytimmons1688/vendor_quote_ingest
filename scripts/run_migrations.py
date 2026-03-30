#!/usr/bin/env python3
"""
Runs pending SQL migrations against the Neon database.
Tracks applied migrations in a schema_migrations table.
"""

import os
import sys
from pathlib import Path

import psycopg2


MIGRATIONS_DIR = Path(__file__).parent.parent / "db" / "migrations"


def get_connection():
    return psycopg2.connect(os.environ["NEON_DATABASE_URL"], sslmode="require")


def ensure_tracking_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    conn.commit()


def get_applied(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM schema_migrations ORDER BY version")
        return {row[0] for row in cur.fetchall()}


def run_migrations(conn):
    ensure_tracking_table(conn)
    applied = get_applied(conn)

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("No migration files found")
        return 0

    count = 0
    for mf in migration_files:
        version = mf.name
        if version in applied:
            print(f"  Skip (already applied): {version}")
            continue

        print(f"  Applying: {version}")
        sql = mf.read_text()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s)",
                    (version,),
                )
            conn.commit()
            count += 1
        except Exception as e:
            conn.rollback()
            print(f"  ERROR applying {version}: {e}", file=sys.stderr)
            raise

    return count


def main():
    conn = get_connection()
    try:
        applied = run_migrations(conn)
        print(f"Migrations complete: {applied} applied")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
