from contextlib import asynccontextmanager
from dataclasses import dataclass
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
Claude has access to database tools that can interact with a SQLite database. The database state persists throughout the conversation, allowing you to build and query data incrementally.

# What are the database tools?
There are four main functions available:
1. describe_database: View the current database schema
2. run_ddl: Execute Data Definition Language (DDL) statements to create/modify database structure
3. write_database: Execute Data Manipulation Language (DML) statements to insert/update/delete data
4. select: Query data with SELECT statements

# When to use the database tools
Use these tools when you need to:
* Create a structured data environment to store and analyze information
* Demonstrate database concepts, SQL queries, or data modeling principles
* Build functioning examples of application data layers
* Analyze data provided by the user in a relational format
* Create sample datasets for testing or educational purposes

# Guidelines for effective database use

## Database Schema Design
* Start by planning your table structure with proper relationships
* Use appropriate data types (INTEGER, TEXT, REAL, BLOB, etc.)
* Implement constraints (NOT NULL, UNIQUE, PRIMARY KEY, FOREIGN KEY)
* Consider normalization principles to reduce redundancy
* Use meaningful names for tables and columns that reflect their purpose

## Creating Tables
When creating tables with run_ddl, follow these practices:
* Always include a primary key (usually an INTEGER with AUTOINCREMENT)
* Define foreign keys to establish relationships between tables
* Add appropriate constraints to maintain data integrity
* Include helpful comments to document table/column purposes
* Consider adding indexes for frequently queried columns

## Managing Data
When manipulating data with write_database:
* Use parameterized values in INSERT statements to prevent SQL injection
* Group related INSERT statements together for better readability
* For bulk operations, consider using transactions for better performance
* Validate data before insertion when possible
* Use meaningful sample data that demonstrates real-world scenarios

## Querying Data
When querying with select:
* Start with simple queries before building more complex ones
* Use proper JOIN syntax to combine related tables
* Apply appropriate WHERE conditions to filter results
* Utilize GROUP BY, HAVING, ORDER BY for organizing and analyzing results
* Consider query performance for larger datasets

# Example workflow
A typical workflow might look like:
1. Use describe_database to check the current state
2. Use run_ddl to create the necessary table structure
3. Use write_database to populate the tables with data
4. Use select to query and analyze the data
5. Iterate by modifying schema or adding more data as needed

# Common pitfalls to avoid
* Creating tables without primary keys
* Omitting foreign key constraints in related tables
* Using inconsistent naming conventions
* Writing queries without proper JOIN conditions
* Neglecting to include sample data that tests all constraints

# Advanced SQLite features
SQLite supports various advanced features that can be utilized:
* Virtual tables and FTS (Full-Text Search)
* Window functions for analytical queries
* Common Table Expressions (CTEs) for recursive queries
* JSON functions for working with JSON data
* Various date and time functions
* Aggregate functions with GROUP BY clauses

Remember that SQLite has some limitations compared to other database systems:
* Limited ALTER TABLE functionality
* No stored procedures or triggers (though triggers are partially supported)
* Simplified data types compared to other RDBMS systems
* Some constraints are not enforced by default (foreign keys must be enabled)

When demonstrating database concepts, always prioritize clarity and educational value over optimization.
</sqlite_tool_info>"""
  )

  @mcp.tool()
  def describe_database(ctx: Context) -> list[SQLiteObject]:
    """Describe the current database."""
    db: SQLiteDatabase = ctx.request_context.lifespan_context.db
    return db.describe_all()

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
      return db.run_modification(sql)
    except Exception as e:
      raise ToolError(f"Failed to run SQL statement: {e}") from e

  @mcp.tool()
  def select(ctx: Context, sql: str) -> list[dict[str, Any]]:
    """Run a select SQL statement."""
    db: SQLiteDatabase = ctx.request_context.lifespan_context.db
    try:
      return db.select(sql)
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
