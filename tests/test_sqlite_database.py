import os
import sqlite3
import pytest
import tempfile
import shutil

from sqlite_as_mcp.database.sqlite import SQLiteDatabase
from sqlite_as_mcp.database.ddl import CreateTable, DropTable, RenameTable, Column, ForeignKey, Index


@pytest.fixture(scope="session")
def temp_dir():
  """テスト用の一時ディレクトリを提供"""
  dir_path = tempfile.mkdtemp()
  yield dir_path
  shutil.rmtree(dir_path)


@pytest.fixture
def db_path(temp_dir):
  """テスト用のデータベースファイルパスを提供"""
  return os.path.join(temp_dir, "test_db.sqlite")


@pytest.fixture
def db(db_path):
  """フィクスチャ: データベースのインスタンスを提供"""
  # ファイルが存在する場合は削除して新規作成
  if os.path.exists(db_path):
    os.remove(db_path)

  db = SQLiteDatabase(db_path)
  yield db
  db.close()


@pytest.fixture
def db_with_table(db_path):
  """フィクスチャ: 基本テーブルを持つデータベースを提供"""
  # ファイルが存在する場合は削除して新規作成
  if os.path.exists(db_path):
    os.remove(db_path)

  db = SQLiteDatabase(db_path)
  test_table = CreateTable(
    action="create_table",
    table_name="test_table",
    comment=None,
    columns=[
      Column(
        name="id",
        data_type="INTEGER",
        not_null=True,
        unique=False,
        default=None,
        comment=None
      ),
      Column(
        name="name",
        data_type="TEXT",
        not_null=True,
        unique=False,
        default=None,
        comment=None
      ),
      Column(
        name="value",
        data_type="REAL",
        not_null=False,
        unique=False,
        default=None,
        comment=None
      )
    ],
    foreign_keys=[],
    indexes=[]
  )
  db.run_ddl([test_table])
  yield db
  db.close()


@pytest.fixture
def db_with_data(db_path):
  """フィクスチャ: データが入ったテーブルを持つデータベースを提供"""
  # ファイルが存在する場合は削除して新規作成
  if os.path.exists(db_path):
    os.remove(db_path)

  db = SQLiteDatabase(db_path)

  # テーブル作成
  test_table = CreateTable(
    action="create_table",
    table_name="test_table",
    comment=None,
    columns=[
      Column(
        name="id",
        data_type="INTEGER",
        not_null=True,
        unique=False,
        default=None,
        comment=None
      ),
      Column(
        name="name",
        data_type="TEXT",
        not_null=True,
        unique=False,
        default=None,
        comment=None
      ),
      Column(
        name="value",
        data_type="REAL",
        not_null=False,
        unique=False,
        default=None,
        comment=None
      )
    ],
    foreign_keys=[],
    indexes=[]
  )
  db.run_ddl([test_table])

  # データ挿入
  db.run_modification("INSERT INTO test_table (id, name, value) VALUES (1, 'item1', 10.5)")
  db.run_modification("INSERT INTO test_table (id, name, value) VALUES (2, 'item2', 20.5)")
  db.run_modification("INSERT INTO test_table (id, name, value) VALUES (3, 'item3', 30.5)")

  yield db
  db.close()


class TestListTables:

  def test_empty_database(self, db):
    """空のデータベースでlist_tablesが空のリストを返すことを確認"""
    assert db.list_tables() == []

  def test_with_one_table(self, db_with_table):
    """テーブルが1つある場合、そのテーブルがリストに含まれることを確認"""
    tables = db_with_table.list_tables()
    assert "test_table" in tables
    assert len(tables) == 1

  def test_with_multiple_tables(self, db_with_table):
    """複数のテーブルがある場合、すべてのテーブルがリストに含まれることを確認"""
    # 2つ目のテーブルを作成
    second_table = CreateTable(
      action="create_table",
      table_name="second_table",
      comment=None,
      columns=[
        Column(
          name="id",
          data_type="INTEGER",
          not_null=False,
          unique=False,
          default=None,
          comment=None
        ),
        Column(
          name="description",
          data_type="TEXT",
          not_null=False,
          unique=False,
          default=None,
          comment=None
        )
      ],
      foreign_keys=[],
      indexes=[]
    )
    db_with_table.run_ddl([second_table])

    tables = db_with_table.list_tables()
    assert "test_table" in tables
    assert "second_table" in tables
    assert len(tables) == 2


class TestDescribeTable:

  def test_describe_existing_table(self, db_with_table):
    """存在するテーブルの情報が正確に取得できることを確認"""
    result = db_with_table.describe_table("test_table")

    assert len(result) == 1
    table_info = result[0]

    assert table_info.type == "table"
    assert table_info.name == "test_table"
    assert table_info.tbl_name == "test_table"
    assert table_info.root_page is not None
    assert table_info.sql is not None
    assert "CREATE TABLE test_table" in table_info.sql

  def test_describe_nonexistent_table(self, db):
    """存在しないテーブルにアクセスした場合、空のリストが返ることを確認"""
    result = db.describe_table("nonexistent_table")
    assert result == []


class TestRunDDL:

  def test_create_table(self, db):
    """CreateTableを使用してテーブルを正常に作成できることを確認"""
    test_table = CreateTable(
      action="create_table",
      table_name="new_table",
      comment=None,
      columns=[
        Column(
          name="id",
          data_type="INTEGER",
          not_null=True,
          unique=False,
          default=None,
          comment=None
        ),
        Column(
          name="name",
          data_type="TEXT",
          not_null=False,
          unique=False,
          default=None,
          comment=None
        )
      ],
      foreign_keys=[],
      indexes=[]
    )

    db.run_ddl([test_table])
    tables = db.list_tables()

    assert "new_table" in tables

  def test_create_table_with_foreign_key(self, db_with_table):
    """外部キーを含むテーブルを正常に作成できることを確認"""
    related_table = CreateTable(
      action="create_table",
      table_name="related_table",
      comment=None,
      columns=[
        Column(
          name="id",
          data_type="INTEGER",
          not_null=True,
          unique=False,
          default=None,
          comment=None
        ),
        Column(
          name="test_id",
          data_type="INTEGER",
          not_null=True,
          unique=False,
          default=None,
          comment=None
        ),
        Column(
          name="data",
          data_type="TEXT",
          not_null=False,
          unique=False,
          default=None,
          comment=None
        )
      ],
      foreign_keys=[
        ForeignKey(
          reference_to="test_table",
          column_pairs=[(
            "test_id",
            "id",
          )],
          on_delete="CASCADE",
          on_update=None
        )
      ],
      indexes=[]
    )

    db_with_table.run_ddl([related_table])
    tables = db_with_table.list_tables()

    assert "related_table" in tables

    # 外部キー制約が正しく作成されたか確認
    result = db_with_table.describe_table("related_table")[0]
    assert "FOREIGN KEY" in result.sql
    assert "REFERENCES test_table" in result.sql

  def test_drop_table(self, db_with_table):
    """DropTableを使用してテーブルを正常に削除できることを確認"""
    drop_table = DropTable(
      action="drop_table",
      table_name="test_table",
    )

    db_with_table.run_ddl([drop_table])
    tables = db_with_table.list_tables()

    assert "test_table" not in tables
    assert len(tables) == 0

  def test_rename_table(self, db_with_table):
    """RenameTableを使用してテーブル名を正常に変更できることを確認"""
    rename_table = RenameTable(
      action="rename_table",
      old_name="test_table",
      new_name="renamed_table",
    )

    db_with_table.run_ddl([rename_table])
    tables = db_with_table.list_tables()

    assert "test_table" not in tables
    assert "renamed_table" in tables
    assert len(tables) == 1

  def test_invalid_ddl(self, db):
    """無効なDDL実行時にエラーが発生することを確認"""
    # 既に存在するテーブルと同じ名前で作成を試みる
    test_table = CreateTable(
      action="create_table",
      table_name="duplicate_table",
      comment=None,
      columns=[
        Column(
          name="id",
          data_type="INTEGER",
          not_null=False,
          unique=False,
          default=None,
          comment=None
        )
      ],
      foreign_keys=[],
      indexes=[]
    )

    db.run_ddl([test_table])

    # 同じ名前のテーブルをもう一度作成しようとするとエラーになるはず
    with pytest.raises(sqlite3.OperationalError):
      db.run_ddl([test_table])


class TestRunModification:

  def test_insert_data(self, db_with_table):
    """INSERT文でデータを正常に挿入できることを確認"""
    db_with_table.run_modification(
      "INSERT INTO test_table (id, name, value) VALUES (1, 'test_item', 42.5)"
    )

    result = db_with_table.select("SELECT * FROM test_table WHERE id = 1")

    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["name"] == "test_item"
    assert result[0]["value"] == 42.5

  def test_update_data(self, db_with_data):
    """UPDATE文でデータを正常に更新できることを確認"""
    db_with_data.run_modification(
      "UPDATE test_table SET name = 'updated_name', value = 99.9 WHERE id = 2"
    )

    result = db_with_data.select("SELECT * FROM test_table WHERE id = 2")

    assert len(result) == 1
    assert result[0]["name"] == "updated_name"
    assert result[0]["value"] == 99.9

  def test_delete_data(self, db_with_data):
    """DELETE文でデータを正常に削除できることを確認"""
    db_with_data.run_modification("DELETE FROM test_table WHERE id = 3")

    result = db_with_data.select("SELECT * FROM test_table WHERE id = 3")
    assert len(result) == 0

    # 他のデータは残っていることを確認
    all_data = db_with_data.select("SELECT * FROM test_table")
    assert len(all_data) == 2

  def test_transaction_rollback(self, db_with_data):
    """エラー時にトランザクションがロールバックされることを確認"""
    # エラーを起こすSQL (存在しないカラムへの挿入)
    with pytest.raises(sqlite3.OperationalError):
      db_with_data.run_modification(
        "INSERT INTO test_table (id, nonexistent_column) VALUES (4, 'value')"
      )

    # トランザクションがロールバックされているので、id=4のデータは挿入されていないはず
    result = db_with_data.select("SELECT * FROM test_table WHERE id = 4")
    assert len(result) == 0

  def test_blocks_ddl_create_table(self, db):
    """run_modificationでCREATE TABLEが実行できないことを確認"""
    with pytest.raises(sqlite3.DatabaseError):
      db.run_modification("CREATE TABLE blocked_table (id INTEGER);")

  def test_blocks_ddl_drop_table(self, db_with_table):
    """run_modificationでDROP TABLEが実行できないことを確認"""
    with pytest.raises(sqlite3.DatabaseError):
      db_with_table.run_modification("DROP TABLE test_table;")

  def test_blocks_ddl_alter_table(self, db_with_table):
    """run_modificationでALTER TABLEが実行できないことを確認"""
    with pytest.raises(sqlite3.DatabaseError):
      db_with_table.run_modification("ALTER TABLE test_table ADD COLUMN new_column TEXT;")


class TestSelect:

  def test_simple_select(self, db_with_data):
    """単純なSELECT文が正常に実行され、結果が期待通りのフォーマットで返ってくることを確認"""
    result = db_with_data.select("SELECT * FROM test_table")

    assert len(result) == 3
    assert isinstance(result, list)
    assert all(isinstance(item, dict) for item in result)

    # 期待される列が存在することを確認
    assert all("id" in item for item in result)
    assert all("name" in item for item in result)
    assert all("value" in item for item in result)

  def test_conditional_select(self, db_with_data):
    """条件付きSELECT文が正しく動作することを確認"""
    result = db_with_data.select("SELECT * FROM test_table WHERE id > 1")

    assert len(result) == 2
    assert all(item["id"] > 1 for item in result)

  def test_empty_result(self, db_with_data):
    """結果がない場合に空のリストが返ることを確認"""
    result = db_with_data.select("SELECT * FROM test_table WHERE id > 100")
    assert result == []

  def test_select_specific_columns(self, db_with_data):
    """特定のカラムのみ選択した場合、正しく結果が返ることを確認"""
    result = db_with_data.select("SELECT id, name FROM test_table")

    assert len(result) == 3
    assert all("id" in item for item in result)
    assert all("name" in item for item in result)
    assert all("value" not in item for item in result)

  def test_aggregation_function(self, db_with_data):
    """集計関数を使用したSELECT文が正しく動作することを確認"""
    result = db_with_data.select(
      "SELECT COUNT(*) as count, AVG(value) as avg_value FROM test_table"
    )

    assert len(result) == 1
    assert result[0]["count"] == 3
    assert result[0]["avg_value"] == 20.5  # (10.5 + 20.5 + 30.5) / 3 = 20.5
