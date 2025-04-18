from typing import Literal
from pydantic import BaseModel, Field, RootModel


def indent(s: str, level: int) -> str:
  """
  Indent the string by n spaces.
  """
  return '\n'.join([f"{'\t' * level}{line}" for line in s.split('\n')])


class Column(BaseModel):
  name: str = Field(
    ...,
    title='column name',
    description=('The name of the column. '
                 'For example, the name of the column like "id".'),
  )
  data_type: str = Field(
    ...,
    title='data type',
    description=('The data type of the column. '
                 'For example, the data type like "INTEGER".'),
  )
  primary_key: bool = Field(
    False,
    title='is primary key',
    description=(
      'Whether the column is a primary key. '
      'For example, if the column is a primary key, this will be True.'
    ),
  )
  unique: bool = Field(
    False,
    title='is unique',
    description=(
      'Whether the column is unique. '
      'For example, if the column is unique, this will be True.'
    ),
  )
  not_null: bool = Field(
    False,
    title='is not null',
    description=(
      'Whether the column is not null. '
      'For example, if the column is not null, this will be True.'
    ),
  )
  default: str | None = Field(
    None,
    title='default value',
    description=(
      'The default value of the column. '
      'For example, the default value like "0".'
    ),
  )
  comment: str | None = Field(
    None,
    title='comment',
    description=(
      'The comment of the column. '
      'For example, the comment like "This is the id column".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the column to SQL statement.
    """
    sql = f"{self.name} {self.data_type}"
    if self.unique:
      sql += " UNIQUE"
    if self.not_null:
      sql += " NOT NULL"
    if self.default is not None:
      sql += f" DEFAULT {self.default}"
    if self.primary_key:
      sql += " PRIMARY KEY"
    if self.comment is not None:
      sql = f"-- {self.name}: {self.comment}\n{sql}"
    return indent(sql, 1)


class ForeignKey(BaseModel):
  reference_to: str = Field(
    ...,
    title='reference to',
    description=(
      'The table that the foreign key references. '
      'For example, the name of the table like "users".'
    ),
  )
  column_pairs: list[tuple[
    str,
    str,
  ]] = Field(
    ...,
    title='column pairs',
    description=(
      'The column pairs that the foreign key references. '
      'For example, the column pairs like [("id", "user_id")].'
    ),
  )
  on_delete: str | None = Field(
    None,
    title='on delete',
    description=(
      'The action to take when the referenced row is deleted. '
      'For example, the action like "CASCADE".'
    ),
  )
  on_update: str | None = Field(
    None,
    title='on update',
    description=(
      'The action to take when the referenced row is updated. '
      'For example, the action like "CASCADE".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the foreign key to SQL statement.
    """
    sql = f"FOREIGN KEY ({', '.join([col[0] for col in self.column_pairs])})"
    sql += f" REFERENCES {self.reference_to}({', '.join([col[1] for col in self.column_pairs])})"

    if self.on_delete is not None:
      sql += f" ON DELETE {self.on_delete}"

    if self.on_update is not None:
      sql += f" ON UPDATE {self.on_update}"

    return indent(sql, 1)


class Index(BaseModel):

  name: str = Field(
    ...,
    title='index name',
    description=(
      'The name of the index. '
      'For example, the name of the index like "idx_users_id".'
    ),
  )
  table: str = Field(
    ...,
    title='table name',
    description=(
      'The name of the table the index belongs to. '
      'For example, the name of the table like "users".'
    ),
  )
  columns: list[str] = Field(
    ...,
    title='columns',
    description=(
      'The columns that the index is created on. '
      'For example, the columns like ["id", "name"].'
    ),
  )
  unique: bool = Field(
    False,
    title='is unique',
    description=(
      'Whether the index is unique. '
      'For example, if the index is unique, this will be True.'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the index to SQL statement.
    """
    # CREATE INDEX idx_books_category ON books(category_id)
    sql = "CREATE"

    if self.unique:
      sql += " UNIQUE"

    return f"{sql} INDEX {self.name} ON {self.table} ({', '.join(self.columns)});"


class CreateTable(BaseModel):
  action: Literal['create_table'] = Field(
    'create_table',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "create_table".'
    ),
  )

  table_name: str = Field(
    ...,
    title='table name',
    description=(
      'The name of the table to create. '
      'For example, the name of the table like "users".'
    ),
  )

  comment: str | None = Field(
    None,
    title='comment',
    description=(
      'The comment of the table. '
      'For example, the comment like "This is the users table".'
    ),
  )

  columns: list[Column] = Field(
    ...,
    title='columns',
    description=(
      'The columns of the table. '
      'For example, the columns like [Column(name="id", data_type="INTEGER")]'
    ),
  )

  foreign_keys: list[ForeignKey] = Field(
    [],
    title='foreign keys',
    description=(
      'The foreign keys of the table. '
      'For example, the foreign keys like [ForeignKey(reference_to="users", column_pairs=[("user_id", "id")])]'
    ),
  )

  indexes: list[Index] = Field(
    [],
    title='indexes',
    description=(
      'The indexes of the table. '
      'For example, the indexes like [Index(name="idx_users_id", table="users", columns=["id"])]'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the create table statement to SQL statement.
    """
    sql = f"CREATE TABLE {self.table_name} ("

    if self.comment is not None:
      sql = f"-- {self.table_name}: {self.comment}\n{sql}"

    sql += ",\n".join([col.to_sql() for col in self.columns])

    if self.foreign_keys:
      sql += ',\n' + ",\n".join([fk.to_sql() for fk in self.foreign_keys])

    sql += "\n);"

    if self.indexes:
      sql += "\n\n" + "\n".join([index.to_sql() for index in self.indexes])

    return sql


class DropTable(BaseModel):
  """
  Drop table statement.
  """

  action: Literal['drop_table'] = Field(
    'drop_table',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "drop_table".'
    ),
  )

  table_name: str = Field(
    ...,
    title='table name',
    description=(
      'The name of the table to drop. '
      'For example, the name of the table like "users".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the drop table statement to SQL statement.
    """
    return f"DROP TABLE {self.table_name};"


class RenameTable(BaseModel):
  """
  Rename table statement.
  """

  action: Literal['rename_table'] = Field(
    'rename_table',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "rename_table".'
    ),
  )

  old_name: str = Field(
    ...,
    title='old name',
    description=(
      'The old name of the table. '
      'For example, the old name of the table like "users".'
    ),
  )

  new_name: str = Field(
    ...,
    title='new name',
    description=(
      'The new name of the table. '
      'For example, the new name of the table like "customers".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the rename table statement to SQL statement.
    """
    return f"ALTER TABLE {self.old_name} RENAME TO {self.new_name};"


class CreateIndex(BaseModel):
  """
  Create index statement.
  """

  action: Literal['create_index'] = Field(
    'create_index',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "create_index".'
    ),
  )

  index: Index = Field(
    ...,
    title='index',
    description=(
      'The index to create. '
      'For example, the index like Index(name="idx_users_id", table="users", columns=["id"]).'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the create index statement to SQL statement.
    """
    return self.index.to_sql()


class DropIndex(BaseModel):
  """
  Drop index statement.
  """

  action: Literal['drop_index'] = Field(
    'drop_index',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "drop_index".'
    ),
  )

  index_name: str = Field(
    ...,
    title='index name',
    description=(
      'The name of the index to drop. '
      'For example, the name of the index like "idx_users_id".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the drop index statement to SQL statement.
    """
    return f"DROP INDEX {self.index_name};"


class CreateView(BaseModel):
  """
  Create view statement.
  """

  action: Literal['create_view'] = Field(
    'create_view',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "create_view".'
    ),
  )

  view_name: str = Field(
    ...,
    title='view name',
    description=(
      'The name of the view to create. '
      'For example, the name of the view like "users_view".'
    ),
  )

  sql: str = Field(
    ...,
    title='SQL statement',
    description=(
      'The SQL statement to create the view. '
      'For example, the SQL statement like "SELECT * FROM users".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the create view statement to SQL statement.
    """
    sql = self.sql.strip()

    if sql.endswith(';'):
      sql = sql[:-1].strip()

    return f"CREATE VIEW {self.view_name} AS {sql};"


class DropView(BaseModel):
  """
  Drop view statement.
  """

  action: Literal['drop_view'] = Field(
    'drop_view',
    title='action',
    description=(
      'The action to perform. '
      'For example, the action like "drop_view".'
    ),
  )

  view_name: str = Field(
    ...,
    title='view name',
    description=(
      'The name of the view to drop. '
      'For example, the name of the view like "users_view".'
    ),
  )

  def to_sql(self) -> str:
    """
    Convert the drop view statement to SQL statement.
    """
    return f"DROP VIEW {self.view_name};"


type DDL = CreateTable | DropTable | RenameTable | CreateIndex | DropIndex | CreateView | DropView

class SQLiteDDL(RootModel[list[DDL]]):
  pass
