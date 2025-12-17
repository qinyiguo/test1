 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/warehouse_schema.py b/warehouse_schema.py
new file mode 100644
index 0000000000000000000000000000000000000000..546219ffac404e794ea7705941fd94ab2afa7296
--- /dev/null
+++ b/warehouse_schema.py
@@ -0,0 +1,356 @@
+"""Data warehouse schema and ETL helpers for factory and KPI datasets.
+
+This module sets up dimension and fact tables and provides normalization
+helpers for handling alias values (e.g., factory code casing, employee IDs
+with leading zeros).
+"""
+
+from __future__ import annotations
+
+import sqlite3
+from typing import Mapping, Optional, Sequence
+
+# -------------------------- DDL DEFINITIONS --------------------------
+
+WAREHOUSE_DDL = """
+CREATE TABLE IF NOT EXISTS dim_factory (
+    factory_key INTEGER PRIMARY KEY AUTOINCREMENT,
+    factory_code TEXT UNIQUE NOT NULL,
+    region TEXT,
+    line_of_business TEXT
+);
+
+CREATE TABLE IF NOT EXISTS dim_employee (
+    employee_key INTEGER PRIMARY KEY AUTOINCREMENT,
+    employee_id TEXT UNIQUE NOT NULL,
+    factory_key INTEGER,
+    dept TEXT,
+    title TEXT,
+    manager_id TEXT,
+    FOREIGN KEY (factory_key) REFERENCES dim_factory(factory_key)
+);
+
+CREATE TABLE IF NOT EXISTS dim_period (
+    period_key INTEGER PRIMARY KEY AUTOINCREMENT,
+    month INTEGER NOT NULL,
+    quarter INTEGER NOT NULL,
+    year INTEGER NOT NULL,
+    UNIQUE (year, month)
+);
+
+CREATE TABLE IF NOT EXISTS fact_operations (
+    id INTEGER PRIMARY KEY AUTOINCREMENT,
+    factory_key INTEGER NOT NULL,
+    period_key INTEGER NOT NULL,
+    revenue REAL,
+    cost REAL,
+    output_qty REAL,
+    downtime_hours REAL,
+    FOREIGN KEY (factory_key) REFERENCES dim_factory(factory_key),
+    FOREIGN KEY (period_key) REFERENCES dim_period(period_key)
+);
+
+CREATE TABLE IF NOT EXISTS fact_kpi (
+    id INTEGER PRIMARY KEY AUTOINCREMENT,
+    employee_key INTEGER NOT NULL,
+    period_key INTEGER NOT NULL,
+    metric_code TEXT NOT NULL,
+    value REAL,
+    target REAL,
+    FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
+    FOREIGN KEY (period_key) REFERENCES dim_period(period_key)
+);
+
+-- Mapping tables for cleansing/alias handling
+CREATE TABLE IF NOT EXISTS factory_code_alias (
+    alias TEXT PRIMARY KEY,
+    factory_code TEXT NOT NULL
+);
+
+CREATE TABLE IF NOT EXISTS employee_id_alias (
+    alias TEXT PRIMARY KEY,
+    employee_id TEXT NOT NULL
+);
+"""
+
+
+# -------------------------- SETUP HELPERS --------------------------
+
+def initialize_warehouse(connection: sqlite3.Connection) -> None:
+    """Create all dimension/fact tables and alias mapping helpers."""
+
+    cursor = connection.cursor()
+    cursor.executescript(WAREHOUSE_DDL)
+    connection.commit()
+
+
+# ------------------------- NORMALIZATION ----------------------------
+
+def normalize_factory_code(
+    raw_code: Optional[str],
+    *,
+    alias_lookup: Optional[Mapping[str, str]] = None,
+) -> Optional[str]:
+    """
+    Normalize factory codes to uppercase and resolve known aliases.
+
+    Whitespace is trimmed; alias mappings override casing differences.
+    """
+
+    if raw_code is None:
+        return None
+
+    canonical = str(raw_code).strip().upper()
+    if canonical == "":
+        return None
+
+    if alias_lookup:
+        return alias_lookup.get(canonical, canonical)
+    return canonical
+
+
+def normalize_employee_id(
+    raw_id: Optional[str],
+    *,
+    alias_lookup: Optional[Mapping[str, str]] = None,
+) -> Optional[str]:
+    """
+    Normalize employee identifiers by trimming whitespace, removing leading
+    zeros, and resolving aliases.
+    """
+
+    if raw_id is None:
+        return None
+
+    normalized = str(raw_id).strip()
+    if normalized == "":
+        return None
+
+    normalized = normalized.lstrip("0") or "0"
+
+    if alias_lookup:
+        return alias_lookup.get(normalized, normalized)
+    return normalized
+
+
+# ---------------------------- ETL HELPERS ---------------------------
+
+def _load_alias_map(cursor: sqlite3.Cursor, table: str) -> Mapping[str, str]:
+    """Load alias mapping table into an in-memory dict."""
+
+    if "factory" in table:
+        column = "factory_code"
+    else:
+        column = "employee_id"
+    rows = cursor.execute(f"SELECT alias, {column} FROM {table}").fetchall()
+    return {row[0]: row[1] for row in rows}
+
+
+def get_or_create_factory(
+    connection: sqlite3.Connection,
+    *,
+    factory_code: str,
+    region: Optional[str] = None,
+    line_of_business: Optional[str] = None,
+) -> int:
+    """Ensure the factory dimension row exists and return its surrogate key."""
+
+    cursor = connection.cursor()
+    alias_map = _load_alias_map(cursor, "factory_code_alias")
+    normalized_code = normalize_factory_code(factory_code, alias_lookup=alias_map)
+    if normalized_code is None:
+        raise ValueError("factory_code is required")
+
+    row = cursor.execute(
+        "SELECT factory_key FROM dim_factory WHERE factory_code = ?",
+        (normalized_code,),
+    ).fetchone()
+    if row:
+        return row[0]
+
+    cursor.execute(
+        """
+        INSERT INTO dim_factory (factory_code, region, line_of_business)
+        VALUES (?, ?, ?)
+        """,
+        (normalized_code, region, line_of_business),
+    )
+    connection.commit()
+    return cursor.lastrowid
+
+
+def get_or_create_employee(
+    connection: sqlite3.Connection,
+    *,
+    employee_id: str,
+    factory_key: Optional[int] = None,
+    dept: Optional[str] = None,
+    title: Optional[str] = None,
+    manager_id: Optional[str] = None,
+) -> int:
+    """Ensure the employee dimension row exists and return its surrogate key."""
+
+    cursor = connection.cursor()
+    alias_map = _load_alias_map(cursor, "employee_id_alias")
+    normalized_id = normalize_employee_id(employee_id, alias_lookup=alias_map)
+    if normalized_id is None:
+        raise ValueError("employee_id is required")
+
+    row = cursor.execute(
+        "SELECT employee_key FROM dim_employee WHERE employee_id = ?",
+        (normalized_id,),
+    ).fetchone()
+    if row:
+        return row[0]
+
+    cursor.execute(
+        """
+        INSERT INTO dim_employee (employee_id, factory_key, dept, title, manager_id)
+        VALUES (?, ?, ?, ?, ?)
+        """,
+        (normalized_id, factory_key, dept, title, manager_id),
+    )
+    connection.commit()
+    return cursor.lastrowid
+
+
+def get_or_create_period(
+    connection: sqlite3.Connection,
+    *,
+    month: int,
+    year: int,
+) -> int:
+    """Create or retrieve the period dimension using month/year (quarter derived)."""
+
+    if month < 1 or month > 12:
+        raise ValueError("month must be between 1 and 12")
+
+    quarter = (month - 1) // 3 + 1
+    cursor = connection.cursor()
+    row = cursor.execute(
+        "SELECT period_key FROM dim_period WHERE year = ? AND month = ?",
+        (year, month),
+    ).fetchone()
+    if row:
+        return row[0]
+
+    cursor.execute(
+        """
+        INSERT INTO dim_period (month, quarter, year)
+        VALUES (?, ?, ?)
+        """,
+        (month, quarter, year),
+    )
+    connection.commit()
+    return cursor.lastrowid
+
+
+def load_fact_operations(
+    connection: sqlite3.Connection,
+    records: Sequence[Mapping[str, object]],
+) -> None:
+    """Load operational facts, normalizing dimension references in the process."""
+
+    cursor = connection.cursor()
+    for record in records:
+        factory_key = get_or_create_factory(
+            connection,
+            factory_code=str(record.get("factory_code", "")),
+            region=record.get("region"),
+            line_of_business=record.get("line_of_business"),
+        )
+        period_key = get_or_create_period(
+            connection,
+            month=int(record["month"]),
+            year=int(record["year"]),
+        )
+
+        cursor.execute(
+            """
+            INSERT INTO fact_operations (
+                factory_key, period_key, revenue, cost, output_qty, downtime_hours
+            ) VALUES (?, ?, ?, ?, ?, ?)
+            """,
+            (
+                factory_key,
+                period_key,
+                record.get("revenue"),
+                record.get("cost"),
+                record.get("output_qty"),
+                record.get("downtime_hours"),
+            ),
+        )
+    connection.commit()
+
+
+def load_fact_kpi(
+    connection: sqlite3.Connection,
+    records: Sequence[Mapping[str, object]],
+) -> None:
+    """Load KPI facts, cleaning employee IDs and time periods."""
+
+    cursor = connection.cursor()
+    for record in records:
+        factory_key = None
+        if "factory_code" in record:
+            factory_key = get_or_create_factory(
+                connection,
+                factory_code=str(record.get("factory_code", "")),
+                region=record.get("region"),
+                line_of_business=record.get("line_of_business"),
+            )
+
+        employee_key = get_or_create_employee(
+            connection,
+            employee_id=str(record.get("employee_id", "")),
+            factory_key=factory_key,
+            dept=record.get("dept"),
+            title=record.get("title"),
+            manager_id=record.get("manager_id"),
+        )
+
+        period_key = get_or_create_period(
+            connection,
+            month=int(record["month"]),
+            year=int(record["year"]),
+        )
+
+        cursor.execute(
+            """
+            INSERT INTO fact_kpi (employee_key, period_key, metric_code, value, target)
+            VALUES (?, ?, ?, ?, ?)
+            """,
+            (
+                employee_key,
+                period_key,
+                str(record.get("metric_code")),
+                record.get("value"),
+                record.get("target"),
+            ),
+        )
+    connection.commit()
+
+
+def seed_alias_mappings(
+    connection: sqlite3.Connection,
+    *,
+    factory_aliases: Optional[Mapping[str, str]] = None,
+    employee_aliases: Optional[Mapping[str, str]] = None,
+) -> None:
+    """Populate alias mapping tables for consistent normalization rules."""
+
+    cursor = connection.cursor()
+    if factory_aliases:
+        cursor.executemany(
+            "INSERT OR REPLACE INTO factory_code_alias (alias, factory_code) VALUES (?, ?)",
+            [
+                (alias.strip().upper(), code.strip().upper())
+                for alias, code in factory_aliases.items()
+            ],
+        )
+    if employee_aliases:
+        cursor.executemany(
+            "INSERT OR REPLACE INTO employee_id_alias (alias, employee_id) VALUES (?, ?)",
+            [(alias.strip(), emp_id.strip()) for alias, emp_id in employee_aliases.items()],
+        )
+    connection.commit()
 
EOF
)
