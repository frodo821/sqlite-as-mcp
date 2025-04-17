import sqlite3
from typing import Any

from pydantic import BaseModel, Field

from sqlite_as_mcp.database.ddl import DDL


class SQLiteForeignKey(BaseModel):

  table: str = Field(
    ...,
    title='table name',
    description=(
      'The name of the table this constraint belongs to. '
      'For example, the name of the table like "users".'
    ),
  )
  referenced_table: str = Field(
    ...,
    title='referenced table name',
    description=(
      'The name of the table that this foreign key references. '
      'For example, the name of the table like "products".'
    ),
  )
  columns: list[tuple[
    str,
    str,
  ]] = Field(
    ...,
    title='columns',
    description=(
      'The pairs of columns that this foreign key references. '
      'For example, constraint defined by `FOREIGN KEY (customer_id, product_id) REFERENCES customers(id, product_id)`,'
      ' this will be [("customer_id", "id"), ("product_id", "product_id")].'
    ),
  )
  on_update: str = Field(
    'NO ACTION',
    title='on update action',
    description=(
      'The action to take when the referenced row is updated. '
      'For example, "CASCADE", "SET NULL", "SET DEFAULT", or "NO ACTION".'
    ),
  )
  on_delete: str = Field(
    'NO ACTION',
    title='on delete action',
    description=(
      'The action to take when the referenced row is deleted. '
      'For example, "CASCADE", "SET NULL", "SET DEFAULT", or "NO ACTION".'
    ),
  )
  match: str = Field(
    'NONE',
    title='match type',
    description=(
      'The match type for the foreign key constraint. '
      'For example, "NONE", "FULL", "PARTIAL".'
    ),
  )

  @classmethod
  def describe(cls, cur: sqlite3.Cursor, table: str):
    try:
      cur.execute(f'PRAGMA foreign_key_list({table})')
    except sqlite3.OperationalError:
      raise ValueError(f"Table {table} does not exist or is not a valid SQLite table name.")

    # id  seq  table       from          to            on_update  on_delete  match
    rows: list[tuple[int, int, str, str, str, str, str, str]] = cur.fetchall()

    tables: set[str] = set()
    reference_rows: dict[str, list[tuple[str, str]]] = {}
    actions: dict[str, tuple[str, str, str]] = {}

    for row in rows:
      id, seq, table, from_col, to_col, on_update, on_delete, match = row

      tables.add(table)
      reference_rows.setdefault(table, []).append((from_col, to_col))
      actions.setdefault(table, (on_update, on_delete, match))

    foreign_keys: list[SQLiteForeignKey] = []

    for table in tables:
      on_update, on_delete, match = actions[table]
      columns = reference_rows[table]

      foreign_keys.append(
        cls(
          table=table,
          referenced_table=table,
          columns=columns,
          on_update=on_update,
          on_delete=on_delete,
          match=match,
        ),
      )

    return foreign_keys


class SQLiteObject(BaseModel):
  type: str = Field(
    'table',
    title='object type',
    description='The type of the object. For example, table, index, view or trigger.',
    examples=[
      "table",
      "index",
      "trigger",
      "view",
    ],
  )
  name: str = Field(
    ...,
    title='object name',
    description='The name of the object. For example, the name of the table like "users".',
  )
  tbl_name: str = Field(
    ...,
    title='table name',
    description=
    'The name of the table that the object belongs to. For example, the name of the table like "users".',
  )
  root_page: int | None = Field(
    ...,
    title='root page number',
    description=
    'The page number of the object in the root b-tree index. null if the object is not a table or index, i.e. a view, virtual table or trigger.',
  )
  sql: str | None = Field(
    ...,
    title='SQL statement',
    description=
    'The SQL statement that created the object. If not created via a DDL statement, this will be null.',
  )
  foreign_keys: list[SQLiteForeignKey] = Field(
    default_factory=list,
    title='foreign keys',
    description='The foreign keys that this object has.',
  )

  @classmethod
  def from_row(cls, cur: sqlite3.Cursor, row: tuple):
    return cls(
      type=row[0],
      name=row[1],
      tbl_name=row[2],
      root_page=row[3],
      sql=row[4],
      foreign_keys=SQLiteForeignKey.describe(
        cur,
        row[1],
      ),
    )


def dml_authorizer(action_code, table_or_trigger, column, database, trigger_or_view, sql=None):
  ddl_action_codes = [
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_CREATE_TEMP_INDEX,
    sqlite3.SQLITE_CREATE_TEMP_TABLE,
    sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
    sqlite3.SQLITE_CREATE_TEMP_VIEW,
    sqlite3.SQLITE_CREATE_TRIGGER,
    sqlite3.SQLITE_CREATE_VIEW,
    sqlite3.SQLITE_DROP_INDEX,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_DROP_TEMP_INDEX,
    sqlite3.SQLITE_DROP_TEMP_TABLE,
    sqlite3.SQLITE_DROP_TEMP_TRIGGER,
    sqlite3.SQLITE_DROP_TEMP_VIEW,
    sqlite3.SQLITE_DROP_TRIGGER,
    sqlite3.SQLITE_DROP_VIEW,
    sqlite3.SQLITE_ALTER_TABLE,
  ]

  if action_code in ddl_action_codes:
    return sqlite3.SQLITE_DENY

  return sqlite3.SQLITE_OK


class SQLiteDatabase:

  @staticmethod
  def enable_config(conn: sqlite3.Connection, read_only: bool = False):
    conn.executescript('PRAGMA foreign_keys=ON')

    if not read_only:
      conn.execute("PRAGMA journal_mode=WAL")

  @staticmethod
  def to_dict(cur: sqlite3.Cursor) -> list[dict[str, Any]]:
    if not cur.description:
      return []

    rows: list[str] = [n[0] for n in cur.description]
    result: list[dict[str, Any]] = []

    for row in cur.fetchall():
      result.append(
        dict(zip(
          rows,
          row,
        )),
      )

    return result

  def __init__(self, file: str = ':memory:'):
    self.rw_conn = sqlite3.connect(file)
    self.ro_conn = sqlite3.connect(f"file:{file}?mode=ro", uri=True)

    self.enable_config(self.ro_conn, read_only=True)
    self.enable_config(self.rw_conn)

  def list_tables(self) -> list[str]:
    cur = self.ro_conn.execute(
      "SELECT name FROM sqlite_master WHERE type='table'",
    )
    return [table[0] for table in cur]

  def describe_table(self, table: str):
    cur = self.ro_conn.execute(
      "SELECT * FROM sqlite_master WHERE type='table' AND name=?",
      [table],
    )
    return [SQLiteObject.from_row(cur, row) for row in [*cur.fetchall()]]

  def describe_all(self):
    tables = self.list_tables()
    result: list[SQLiteObject] = []
    for table in tables:
      result.extend(self.describe_table(table))
    return result

  def run_ddl(self, ddl: list[DDL]):
    cursor = self.rw_conn.cursor()
    cursor.execute('PRAGMA foreign_keys=OFF')
    cursor.execute('BEGIN TRANSACTION')

    try:
      for statement in ddl:
        cursor.executescript(statement.to_sql())
    except sqlite3.OperationalError as e:
      self.rw_conn.rollback()
      cursor.close()
      raise

    self.rw_conn.commit()
    self.rw_conn.execute('PRAGMA foreign_keys=ON')
    cursor.close()

  def run_modification(self, sql: str):
    """
    Run a modification SQL statement.
    """
    try:
      self.rw_conn.set_authorizer(dml_authorizer)

      cursor = self.rw_conn.cursor()
      cursor.execute('BEGIN TRANSACTION')

      try:
        cursor.executescript(sql)
      except sqlite3.OperationalError as e:
        self.rw_conn.rollback()
        raise

      self.rw_conn.commit()

      try:
        return self.to_dict(cursor)
      finally:
        cursor.close()
    finally:
      self.rw_conn.set_authorizer(None)

  def select(self, sql: str):
    cur = self.ro_conn.execute(sql)
    return self.to_dict(cur)

  def close(self):
    self.ro_conn.close()
    self.rw_conn.close()
