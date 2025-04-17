from contextlib import asynccontextmanager
from dataclasses import dataclass
from json import dumps
from typing import Any, AsyncGenerator, Literal
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.utilities.types import Image
from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel

from sqlite_as_mcp.database.ddl import SQLiteDDL
from sqlite_as_mcp.database.sqlite import SQLiteDatabase, SQLiteObject


@dataclass
class AppContext:
  db: SQLiteDatabase


class SQLiteMCPServerArgs(BaseModel):
  database_path: str = "sqlite.db"
  server_name: str = "SQLite MCP Server"
  server_port: int = 8000
  server_host: str = "localhost"
  mode: Literal['sse', 'stdio'] = 'sse'


def create_server(args: SQLiteMCPServerArgs) -> FastMCP:

  @asynccontextmanager
  async def lifespan(server: FastMCP) -> 'AsyncGenerator[AppContext, Any]':
    """Get the application context."""
    db = SQLiteDatabase(args.database_path)
    try:
      yield AppContext(db)
    finally:
      db.close()

  mcp = FastMCP(
    name=args.server_name,
    lifespan=lifespan,
    host=args.server_host,
    port=args.server_port,
    instructions="""<sqlite_tool_info>
# Database Tools Manual

This document explains how to use the available database tools. The current system supports SQLite databases.

## Table of Contents

1. [Retrieving Database Information](#retrieving-database-information)
2. [Executing DDL Statements](#executing-ddl-statements)
3. [Executing DML Statements](#executing-dml-statements)
4. [Executing SELECT Statements](#executing-select-statements)
5. [Usage Examples](#usage-examples)

## Retrieving Database Information

### describe_database

Retrieves the structure of the current database (tables, columns, constraints, etc.).

**Usage:**
```
describe_database
```

**Return Value:**
- Information about each table in the database (table name, column names, data types, constraints, etc.)

## Executing DDL Statements

### run_ddl

Executes Data Definition Language (DDL) statements. This tool allows operations such as creating, modifying, and deleting tables. It uses structured JSON objects and can execute multiple DDL statements at once.

**Usage:**
```
run_ddl
  Parameter: ddl (JSON object array in SQLiteDDL format)
```

**Main DDL Operations:**
- create_table: Creating a table
- drop_table: Deleting a table
- rename_table: Renaming a table

**Examples:**

1. Creating a table:
```json
{
  "ddl": [
    {
      "action": "create_table",
      "table_name": "users",
      "columns": [
        {
          "name": "user_id",
          "data_type": "INTEGER",
          "not_null": true,
          "unique": true
        },
        {
          "name": "username",
          "data_type": "TEXT",
          "not_null": true
        },
        {
          "name": "email",
          "data_type": "TEXT",
          "unique": true
        }
      ]
    }
  ]
}
```

2. Deleting a table:
```json
{
  "ddl": [
    {
      "action": "drop_table",
      "table_name": "users"
    }
  ]
}
```

3. Renaming a table:
```json
{
  "ddl": [
    {
      "action": "rename_table",
      "old_name": "users",
      "new_name": "customers"
    }
  ]
}
```

4. Executing multiple DDL statements at once:
```json
{
  "ddl": [
    {
      "action": "create_table",
      "table_name": "categories",
      "columns": [
        {
          "name": "category_id",
          "data_type": "INTEGER",
          "not_null": true,
          "unique": true
        },
        {
          "name": "category_name",
          "data_type": "TEXT",
          "not_null": true
        }
      ]
    },
    {
      "action": "create_table",
      "table_name": "product_categories",
      "columns": [
        {
          "name": "product_id",
          "data_type": "INTEGER",
          "not_null": true
        },
        {
          "name": "category_id",
          "data_type": "INTEGER",
          "not_null": true
        }
      ],
      "foreign_keys": [
        {
          "reference_to": "categories",
          "column_pairs": [
            ["category_id", "category_id"]
          ]
        }
      ]
    }
  ]
}
```

## Executing DML Statements

### write_database

Executes Data Manipulation Language (DML) statements. This allows operations such as inserting, updating, and deleting data.

**Usage:**
```
write_database
  Parameter: sql (string)
```

**Main DML Operations:**
- INSERT: Inserting data
- UPDATE: Updating data
- DELETE: Deleting data

**Examples:**
```sql
-- Example of data insertion
INSERT INTO users (username, email) VALUES ('tanaka', 'tanaka@example.com');

-- Example of data update
UPDATE users SET email = 'new_email@example.com' WHERE username = 'tanaka';

-- Example of data deletion
DELETE FROM users WHERE username = 'tanaka';
```

## Executing SELECT Statements

### select

Executes SELECT statements to retrieve data.

**Usage:**
```
select
  Parameter: sql (string)
```

**Examples:**
```sql
-- Basic SELECT statement
SELECT * FROM users;

-- Filtering with WHERE clause
SELECT username, email FROM users WHERE user_id > 10;

-- Joining multiple tables
SELECT u.username, p.post_title 
FROM users u
JOIN posts p ON u.user_id = p.user_id;

-- Aggregation with GROUP BY clause
SELECT department, COUNT(*) as employee_count
FROM employees
GROUP BY department;
```

## Usage Examples

### Creating and Working with a New Table

1. Creating a table
```sql
CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  stock INTEGER DEFAULT 0
);
```

2. Inserting data
```sql
INSERT INTO products (name, price, stock) VALUES 
('Laptop', 89800, 10),
('Smartphone', 79800, 20),
('Tablet', 49800, 15);
```

3. Searching data
```sql
SELECT * FROM products WHERE price < 80000;
```

4. Updating data
```sql
UPDATE products SET stock = stock - 1 WHERE name = 'Laptop';
```

5. Establishing relationships between tables
```sql
CREATE TABLE orders (
  order_id INTEGER PRIMARY KEY,
  product_id INTEGER,
  quantity INTEGER NOT NULL,
  order_date TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

INSERT INTO orders (product_id, quantity) VALUES (1, 2);

SELECT o.order_id, p.name, o.quantity, o.order_date
FROM orders o
JOIN products p ON o.product_id = p.product_id;
```

### Error Handling

If an error occurs during database operations, an error message will be returned. Common errors include:

- Syntax errors: Incorrect SQL statement syntax
- Table does not exist
- Column does not exist
- Unique constraint violation
- Foreign key constraint violation

If these errors occur, check the error message and handle accordingly.
</sqlite_tool_info>"""
  )

  @mcp.tool()
  def describe_database(ctx: Context) -> list[SQLiteObject]:
    """Describe the current database."""
    db: SQLiteDatabase = ctx.request_context.lifespan_context.db
    data = db.describe_all()
    return dumps([d.model_dump() for d in data], ensure_ascii=False)  # type: ignore

  @mcp.tool()
  def run_ddl(ctx: Context, ddl: SQLiteDDL):
    """Run DDL statement on the current database."""
    db: SQLiteDatabase = ctx.request_context.lifespan_context.db
    try:
      return db.run_ddl(ddl.root)
    except Exception as e:
      raise ToolError(f"Failed to run DDL statement: {e}") from e

  @mcp.tool()
  def write_database(ctx: Context, sql: str) -> list[dict[str, Any]] | None:
    """Run a modification (i.e. create, update, delete) SQL statement.
    This tool can run a sql script that contains multiple statements.

    Please note that this tool cannot run DDL statements, only can run DML statements.

    Also, this tool returns only the last statement result.
    For example, if you run a sql script that contains multiple statements with multiple results, but only the last statement result will be returned.
    """
    db: SQLiteDatabase = ctx.request_context.lifespan_context.db

    try:
      return dumps(db.run_modification(sql), ensure_ascii=False)  # type: ignore
    except Exception as e:
      raise ToolError(f"Failed to run SQL statement: {e}") from e

  @mcp.tool()
  def select(ctx: Context, sql: str) -> list[dict[str, Any]]:
    """Run a select SQL statement."""
    db: SQLiteDatabase = ctx.request_context.lifespan_context.db
    try:
      return dumps(db.select(sql), ensure_ascii=False)  # type: ignore
    except Exception as e:
      raise ToolError(f"Failed to run SQL statement: {e}") from e

  return mcp


def main():
  import argparse
  parser = argparse.ArgumentParser(description="SQLite MCP Server")
  parser.add_argument(
    "--database-path",
    type=str,
    default="sqlite.db",
    help="Path to the SQLite database file.",
  )
  parser.add_argument(
    "--server-name",
    type=str,
    default="SQLite MCP Server",
    help="Name of the server.",
  )
  parser.add_argument(
    "--server-port",
    type=int,
    default=8000,
    help="Port of the server.",
  )
  parser.add_argument(
    "--server-host",
    type=str,
    default="localhost",
    help="Host of the server.",
  )
  parser.add_argument(
    "--mode",
    type=str,
    choices=[
      'sse',
      'stdio',
    ],
    default='sse',
    help="Mode of the server.",
  )
  args = SQLiteMCPServerArgs(**vars(parser.parse_args()))
  mcp = create_server(args)
  mcp.run(transport=args.mode)
