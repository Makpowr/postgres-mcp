# PostgreSQL MCP Server

MCP (Model Context Protocol) server that provides **read-only** tools for querying, inspecting, and monitoring PostgreSQL databases.

Built with [FastMCP](https://github.com/jlowin/fastmcp) and [asyncpg](https://github.com/MagicStack/asyncpg).

## Features

- **Read-only by design** — all queries are validated and executed within read-only transactions
- **Schema exploration** — list schemas, tables, describe table structure with columns, indexes, and constraints
- **Custom queries** — execute arbitrary SELECT/WITH/EXPLAIN queries
- **Performance monitoring** — top slow queries (via `pg_stat_statements`), table stats, index usage
- **Diagnostics** — active connections, locks, bloat/dead tuples, unused indexes
- **Server info** — connection test, server status, database sizes
- **Multi-host failover** — configure multiple hosts via `PG_HOSTS` for automatic failover

## Available Tools

| Tool | Description |
|------|-------------|
| `pg_test_connection` | Test connection and show server info |
| `pg_server_status` | Server health metrics (connections, size, config) |
| `pg_list_schemas` | List all schemas |
| `pg_list_tables` | List tables with size and row estimates |
| `pg_describe_table` | Describe columns, indexes, and constraints |
| `pg_query` | Execute read-only SELECT queries |
| `pg_active_connections` | Show active connections and running queries |
| `pg_top_queries` | Top slow queries (requires pg_stat_statements) |
| `pg_table_stats` | Table access statistics (seq scans, index scans) |
| `pg_bloat_check` | Identify tables needing VACUUM |
| `pg_index_usage` | Index usage statistics |
| `pg_unused_indexes` | Find unused indexes (candidates for removal) |
| `pg_database_sizes` | List all databases with sizes |
| `pg_locks` | Show active locks |

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/YOUR_USER/postgres-mcp.git
cd postgres-mcp
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_HOSTS` | (uses PG_HOST) | Comma-separated list of hosts for failover |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_USER` | `postgres` | Database user |
| `PG_PASSWORD` | *(empty)* | Database password |
| `PG_DATABASE` | `postgres` | Default database |

### 3. Configure in Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": ["path/to/postgres_mcp.py"],
      "env": {
        "PG_HOST": "your-host",
        "PG_PORT": "5432",
        "PG_USER": "your-user",
        "PG_PASSWORD": "your-password",
        "PG_DATABASE": "your-database"
      }
    }
  }
}
```

## Security

- All queries run within **read-only transactions** (`SET default_transaction_read_only = on`)
- DML/DDL keywords (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, etc.) are blocked at validation level
- Only `SELECT`, `WITH`, `SHOW`, and `EXPLAIN` statements are allowed
- Use a database user with minimal read-only privileges for best security

## License

MIT
