#!/usr/bin/env python3
"""
MCP Server for PostgreSQL.

Provides read-only tools for querying databases, inspecting schema,
monitoring performance, and diagnosing common issues on PostgreSQL instances.

Transport: stdio (for Claude Desktop / Claude Cowork)
Auth: PostgreSQL user + password via environment variables
Driver: asyncpg (async, no ODBC required)
"""

import json
import os
import sys
import logging
from typing import Optional
from enum import Enum
from datetime import datetime, date
from decimal import Decimal

import asyncpg
from pydantic import BaseModel, Field, field_validator, ConfigDict
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_HOSTS    = [h.strip() for h in os.getenv("PG_HOSTS", PG_HOST).split(",") if h.strip()]
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("postgres_mcp")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def _get_connection(database: str = None) -> asyncpg.Connection:
    last_error = None
    for host in PG_HOSTS:
        try:
            return await asyncpg.connect(
                host=host,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                database=database or PG_DATABASE,
                timeout=10,
                command_timeout=30,
            )
        except Exception as e:
            last_error = e
            logger.warning("Falló conexión a %s:%s -> %s", host, PG_PORT, e)
    if last_error:
        raise last_error
    raise RuntimeError("No hay hosts PostgreSQL configurados en PG_HOSTS")


async def _execute_readonly(query: str, database: str = None,
                             params: tuple = None, max_rows: int = 500) -> list:
    conn = await _get_connection(database)
    try:
        await conn.execute("SET default_transaction_read_only = on;")
        if params:
            rows = await conn.fetch(query, *params)
        else:
            rows = await conn.fetch(query)
        result = []
        for row in rows[:max_rows]:
            clean = {}
            for k, v in dict(row).items():
                if isinstance(v, (datetime, date)):
                    clean[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    clean[k] = float(v)
                elif isinstance(v, bytes):
                    clean[k] = v.hex()
                else:
                    clean[k] = v
            result.append(clean)
        return result
    finally:
        await conn.close()


def _handle_db_error(e: Exception) -> str:
    name = type(e).__name__
    if "Connection refused" in str(e) or "could not connect" in str(e).lower():
        return f"Error de conexion: No se pudo conectar a {PG_HOST}:{PG_PORT}. Verifica host, puerto y que PostgreSQL este corriendo."
    if isinstance(e, asyncpg.InvalidPasswordError):
        return f"Error de autenticacion: usuario o password incorrectos."
    if isinstance(e, asyncpg.UndefinedTableError):
        return f"Error: tabla no encontrada. {e}"
    if isinstance(e, asyncpg.PostgresSyntaxError):
        return f"Error de sintaxis SQL: {e}"
    if "read-only" in str(e).lower() or "read only" in str(e).lower():
        return "Operacion bloqueada: este servidor es de solo lectura."
    return f"Error inesperado ({name}): {e}"


_BLOCKED_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
    "GRANT", "REVOKE", "COPY", "VACUUM", "REINDEX", "CLUSTER",
}


def _validate_readonly_query(sql: str) -> Optional[str]:
    upper = sql.upper().strip()
    for kw in _BLOCKED_KEYWORDS:
        if kw in upper.split():
            return f"Operacion bloqueada: '{kw}' no esta permitida. Este servidor es de solo lectura."
    if not (upper.startswith("SELECT") or upper.startswith("WITH")
            or upper.startswith("SHOW") or upper.startswith("EXPLAIN")):
        return "Solo se permiten consultas SELECT, WITH (CTEs), SHOW o EXPLAIN."
    return None


# ---------------------------------------------------------------------------
# Enums & Pydantic models
# ---------------------------------------------------------------------------

class ResponseFormat(str, Enum):
    MARKDOWN = "markdown"
    JSON = "json"


class QueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    sql: str = Field(..., description="Consulta SELECT a ejecutar (solo lectura)", min_length=5, max_length=4000)
    database: Optional[str] = Field(default=None, description="Base de datos destino")
    max_rows: Optional[int] = Field(default=100, ge=1, le=1000)
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


class ListTablesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    schema_name: Optional[str] = Field(default=None, description="Filtrar por schema (ej: 'public')")
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


class DescribeTableInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    table_name: str = Field(..., description="Nombre de la tabla (ej: 'public.ventas')", min_length=1, max_length=256)
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


class DatabaseInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


class TopQueriesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    top_n: Optional[int] = Field(default=10, ge=1, le=50)
    order_by: Optional[str] = Field(default="total_exec_time", description="total_exec_time | calls | mean_exec_time | rows")
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)

    @field_validator("order_by")
    @classmethod
    def validate_order_by(cls, v):
        allowed = {"total_exec_time", "calls", "mean_exec_time", "rows"}
        if v not in allowed:
            raise ValueError(f"order_by debe ser uno de: {', '.join(allowed)}")
        return v


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _rows_to_markdown(rows: list, title: str = "") -> str:
    if not rows:
        return f"## {title}\n\nSin resultados." if title else "Sin resultados."
    headers = list(rows[0].keys())
    lines = []
    if title:
        lines.append(f"## {title}\n")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        vals = []
        for h in headers:
            v = row.get(h, "")
            v_str = str(v) if v is not None else ""
            if len(v_str) > 120:
                v_str = v_str[:117] + "..."
            vals.append(v_str.replace("|", "\\|").replace("\n", " "))
        lines.append("| " + " | ".join(vals) + " |")
    lines.append(f"\n*{len(rows)} fila(s) retornada(s)*")
    return "\n".join(lines)


def _format_response(rows: list, fmt: ResponseFormat, title: str = "") -> str:
    if fmt == ResponseFormat.MARKDOWN:
        return _rows_to_markdown(rows, title)
    return json.dumps(rows, indent=2, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP("postgres_mcp")


# ---- Connection & Server Info ----

@mcp.tool(
    name="pg_test_connection",
    annotations={"title": "Test PostgreSQL Connection", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_test_connection() -> str:
    """Prueba la conexion a PostgreSQL y retorna informacion basica del servidor.

    Returns:
        str: Version, base de datos actual y usuario conectado.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                version()                     AS pg_version,
                current_database()            AS current_db,
                current_user                  AS connected_user,
                pg_postmaster_start_time()    AS server_start_time,
                inet_server_addr()            AS server_ip,
                inet_server_port()            AS server_port
        """)
        if rows:
            r = rows[0]
            return (
                f"# Conexion exitosa a PostgreSQL\n\n"
                f"- **Version**: {r['pg_version']}\n"
                f"- **Base de datos**: {r['current_db']}\n"
                f"- **Usuario**: {r['connected_user']}\n"
                f"- **Servidor**: {r['server_ip']}:{r['server_port']}\n"
                f"- **Inicio del servicio**: {r['server_start_time']}\n"
            )
        return "Conexion exitosa pero no se pudo obtener informacion del servidor."
    except Exception as e:
        return _handle_db_error(e)


@mcp.tool(
    name="pg_server_status",
    annotations={"title": "PostgreSQL Server Status", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_server_status() -> str:
    """Retorna el estado general del servidor: conexiones, tamano, actividad.

    Returns:
        str: Metricas de salud del servidor PostgreSQL.
    """
    try:
        stats = await _execute_readonly("""
            SELECT
                (SELECT count(*) FROM pg_stat_activity WHERE state = 'active')  AS active_connections,
                (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle')    AS idle_connections,
                (SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL) AS waiting_connections,
                (SELECT count(*) FROM pg_database WHERE datallowconn)           AS total_databases,
                pg_size_pretty(pg_database_size(current_database()))            AS current_db_size,
                (SELECT setting FROM pg_settings WHERE name = 'max_connections') AS max_connections,
                (SELECT setting FROM pg_settings WHERE name = 'shared_buffers') AS shared_buffers
        """)
        if not stats:
            return "No se pudo obtener el estado del servidor."
        r = stats[0]
        return (
            f"# Estado del Servidor PostgreSQL\n\n"
            f"- **Conexiones activas**: {r['active_connections']}\n"
            f"- **Conexiones idle**: {r['idle_connections']}\n"
            f"- **Conexiones en espera**: {r['waiting_connections']}\n"
            f"- **Max conexiones**: {r['max_connections']}\n"
            f"- **Bases de datos**: {r['total_databases']}\n"
            f"- **Tamano BD actual**: {r['current_db_size']}\n"
            f"- **Shared buffers**: {r['shared_buffers']}\n"
        )
    except Exception as e:
        return _handle_db_error(e)


# ---- Schema & Data Exploration ----

@mcp.tool(
    name="pg_list_schemas",
    annotations={"title": "List PostgreSQL Schemas", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_list_schemas(params: DatabaseInput) -> str:
    """Lista todos los schemas de la base de datos actual.

    Returns:
        str: Schemas con propietario y descripcion.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                n.nspname                          AS schema_name,
                pg_catalog.pg_get_userbyid(n.nspowner) AS owner,
                pg_catalog.obj_description(n.oid, 'pg_namespace') AS description
            FROM pg_catalog.pg_namespace n
            WHERE n.nspname NOT LIKE 'pg_%'
              AND n.nspname != 'information_schema'
            ORDER BY n.nspname
        """)
        return _format_response(rows, params.response_format, "Schemas")
    except Exception as e:
        return _handle_db_error(e)


@mcp.tool(
    name="pg_list_tables",
    annotations={"title": "List Tables", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_list_tables(params: ListTablesInput) -> str:
    """Lista todas las tablas con schema, filas estimadas y tamano.

    Args:
        params (ListTablesInput): Schema opcional y formato.
    Returns:
        str: Lista de tablas con metadatos.
    """
    try:
        schema_filter = f"AND n.nspname = '{params.schema_name}'" if params.schema_name else \
                        "AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
        rows = await _execute_readonly(f"""
            SELECT
                n.nspname                               AS schema_name,
                c.relname                               AS table_name,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                pg_size_pretty(pg_relation_size(c.oid))       AS table_size,
                c.reltuples::bigint                     AS estimated_rows,
                pg_stat_get_last_analyze_time(c.oid)   AS last_analyze,
                pg_stat_get_last_vacuum_time(c.oid)    AS last_vacuum
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              {schema_filter}
            ORDER BY n.nspname, c.relname
        """)
        return _format_response(rows, params.response_format, "Tablas")
    except Exception as e:
        return _handle_db_error(e)


@mcp.tool(
    name="pg_describe_table",
    annotations={"title": "Describe Table Structure", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_describe_table(params: DescribeTableInput) -> str:
    """Describe la estructura de una tabla: columnas, tipos, PKs, indices y FKs.

    Args:
        params (DescribeTableInput): Nombre de tabla (schema.tabla o solo tabla) y formato.
    Returns:
        str: Detalle de columnas, indices y restricciones.
    """
    try:
        parts = params.table_name.split(".")
        schema = parts[0] if len(parts) == 2 else "public"
        table  = parts[1] if len(parts) == 2 else parts[0]

        cols = await _execute_readonly("""
            SELECT
                a.attnum                                        AS ordinal,
                a.attname                                       AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END AS nullable,
                pg_get_expr(d.adbin, d.adrelid)                AS default_value,
                col_description(a.attrelid, a.attnum)          AS description
            FROM pg_attribute a
            JOIN pg_class c     ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE c.relname = $1 AND n.nspname = $2
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """, params=(table, schema))

        indexes = await _execute_readonly("""
            SELECT
                i.relname        AS index_name,
                ix.indisprimary  AS is_primary,
                ix.indisunique   AS is_unique,
                pg_get_indexdef(ix.indexrelid) AS definition
            FROM pg_index ix
            JOIN pg_class c  ON c.oid = ix.indrelid
            JOIN pg_class i  ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1 AND n.nspname = $2
            ORDER BY ix.indisprimary DESC, i.relname
        """, params=(table, schema))

        out = [f"## Tabla: {schema}.{table}\n"]
        out.append(_rows_to_markdown(cols,     "Columnas"))
        out.append("\n")
        out.append(_rows_to_markdown(indexes,  "Indices"))
        return "\n".join(out)
    except Exception as e:
        return _handle_db_error(e)


@mcp.tool(
    name="pg_query",
    annotations={"title": "Execute Read-Only Query", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_query(params: QueryInput) -> str:
    """Ejecuta una consulta SELECT arbitraria (solo lectura).

    Args:
        params (QueryInput): SQL, base de datos opcional, max_rows y formato.
    Returns:
        str: Resultados en markdown o JSON.
    """
    err = _validate_readonly_query(params.sql)
    if err:
        return err
    try:
        rows = await _execute_readonly(
            params.sql,
            database=params.database,
            max_rows=params.max_rows or 100,
        )
        return _format_response(rows, params.response_format, "Resultado")
    except Exception as e:
        return _handle_db_error(e)


# ---- Active Connections ----

@mcp.tool(
    name="pg_active_connections",
    annotations={"title": "Active Connections", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_active_connections(params: DatabaseInput) -> str:
    """Muestra todas las conexiones activas con su estado y query en ejecucion.

    Returns:
        str: Conexiones con pid, usuario, BD, estado y query.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                pid,
                usename       AS username,
                datname       AS database,
                application_name,
                client_addr,
                state,
                wait_event_type,
                wait_event,
                EXTRACT(EPOCH FROM (now() - query_start))::int AS query_seconds,
                LEFT(query, 120) AS current_query
            FROM pg_stat_activity
            WHERE state IS NOT NULL
            ORDER BY state, query_start
        """)
        return _format_response(rows, params.response_format, "Conexiones Activas")
    except Exception as e:
        return _handle_db_error(e)


# ---- Top Queries (requires pg_stat_statements) ----

@mcp.tool(
    name="pg_top_queries",
    annotations={"title": "Top Slow Queries", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_top_queries(params: TopQueriesInput) -> str:
    """Muestra las consultas mas costosas usando pg_stat_statements.

    Requiere que la extension pg_stat_statements este habilitada.

    Args:
        params (TopQueriesInput): Top N, orden y formato.
    Returns:
        str: Queries con tiempo total, llamadas y tiempo medio.
    """
    try:
        rows = await _execute_readonly(f"""
            SELECT
                LEFT(query, 150)                AS query_preview,
                calls,
                ROUND(total_exec_time::numeric, 2) AS total_exec_time_ms,
                ROUND(mean_exec_time::numeric,  2) AS mean_exec_time_ms,
                rows,
                shared_blks_hit,
                shared_blks_read
            FROM pg_stat_statements
            ORDER BY {params.order_by} DESC
            LIMIT {params.top_n}
        """)
        return _format_response(rows, params.response_format,
                                 f"Top {params.top_n} Queries (orden: {params.order_by})")
    except asyncpg.UndefinedTableError:
        return "La extension pg_stat_statements no esta habilitada en este servidor."
    except Exception as e:
        return _handle_db_error(e)


# ---- Table Stats ----

@mcp.tool(
    name="pg_table_stats",
    annotations={"title": "Table Statistics", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_table_stats(params: ListTablesInput) -> str:
    """Muestra estadisticas de acceso por tabla: seq scans, index scans, tuplas.

    Args:
        params (ListTablesInput): Schema opcional y formato.
    Returns:
        str: Estadisticas de uso por tabla.
    """
    try:
        schema_filter = f"AND schemaname = '{params.schema_name}'" if params.schema_name else \
                        "AND schemaname NOT IN ('pg_catalog','information_schema','pg_toast')"
        rows = await _execute_readonly(f"""
            SELECT
                schemaname                              AS schema_name,
                relname                                 AS table_name,
                seq_scan,
                seq_tup_read,
                idx_scan,
                idx_tup_fetch,
                n_tup_ins   AS inserts,
                n_tup_upd   AS updates,
                n_tup_del   AS deletes,
                n_live_tup  AS live_rows,
                n_dead_tup  AS dead_rows,
                last_analyze,
                last_vacuum
            FROM pg_stat_user_tables
            WHERE 1=1 {schema_filter}
            ORDER BY seq_scan DESC
        """)
        return _format_response(rows, params.response_format, "Estadisticas de Tablas")
    except Exception as e:
        return _handle_db_error(e)


# ---- Bloat & Dead Tuples ----

@mcp.tool(
    name="pg_bloat_check",
    annotations={"title": "Table Bloat Check", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_bloat_check(params: DatabaseInput) -> str:
    """Identifica tablas con alta cantidad de dead tuples que necesitan VACUUM.

    Returns:
        str: Tablas con dead_rows y recomendacion de vacuum.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                schemaname                              AS schema_name,
                relname                                 AS table_name,
                n_live_tup                              AS live_rows,
                n_dead_tup                              AS dead_rows,
                CASE WHEN n_live_tup > 0
                     THEN ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                     ELSE 0 END                         AS dead_pct,
                pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
                last_vacuum,
                last_autovacuum
            FROM pg_stat_user_tables
            WHERE n_dead_tup > 100
            ORDER BY dead_rows DESC
            LIMIT 30
        """)
        return _format_response(rows, params.response_format, "Tablas con Bloat (Dead Tuples)")
    except Exception as e:
        return _handle_db_error(e)


# ---- Index Usage ----

@mcp.tool(
    name="pg_index_usage",
    annotations={"title": "Index Usage Statistics", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_index_usage(params: ListTablesInput) -> str:
    """Muestra estadisticas de uso de indices: scans, tuplas leidas y fetched.

    Args:
        params (ListTablesInput): Schema opcional y formato.
    Returns:
        str: Indices con su nivel de uso.
    """
    try:
        schema_filter = f"AND schemaname = '{params.schema_name}'" if params.schema_name else \
                        "AND schemaname NOT IN ('pg_catalog','information_schema')"
        rows = await _execute_readonly(f"""
            SELECT
                schemaname  AS schema_name,
                relname     AS table_name,
                indexrelname AS index_name,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch,
                pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
            FROM pg_stat_user_indexes
            WHERE 1=1 {schema_filter}
            ORDER BY idx_scan DESC
        """)
        return _format_response(rows, params.response_format, "Uso de Indices")
    except Exception as e:
        return _handle_db_error(e)


# ---- Unused Indexes ----

@mcp.tool(
    name="pg_unused_indexes",
    annotations={"title": "Unused Indexes", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_unused_indexes(params: DatabaseInput) -> str:
    """Identifica indices que no han sido usados (candidatos para eliminar).

    Returns:
        str: Indices con 0 scans y su tamano.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                schemaname                                      AS schema_name,
                relname                                         AS table_name,
                indexrelname                                    AS index_name,
                idx_scan                                        AS times_used,
                pg_size_pretty(pg_relation_size(indexrelid))   AS index_size
            FROM pg_stat_user_indexes
            WHERE idx_scan = 0
              AND schemaname NOT IN ('pg_catalog','information_schema')
            ORDER BY pg_relation_size(indexrelid) DESC
        """)
        return _format_response(rows, params.response_format, "Indices Sin Uso")
    except Exception as e:
        return _handle_db_error(e)


# ---- Database Sizes ----

@mcp.tool(
    name="pg_database_sizes",
    annotations={"title": "Database Sizes", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_database_sizes(params: DatabaseInput) -> str:
    """Lista todas las bases de datos del cluster con su tamano y encoding.

    Returns:
        str: Bases de datos con tamano, encoding y collation.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                datname                                     AS database_name,
                pg_size_pretty(pg_database_size(datname))  AS size,
                pg_encoding_to_char(encoding)               AS encoding,
                datcollate                                  AS collation,
                datallowconn                                AS allow_connections,
                datconnlimit                                AS connection_limit
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY pg_database_size(datname) DESC
        """)
        return _format_response(rows, params.response_format, "Bases de Datos")
    except Exception as e:
        return _handle_db_error(e)


# ---- Locks ----

@mcp.tool(
    name="pg_locks",
    annotations={"title": "Current Locks", "readOnlyHint": True,
                  "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def pg_locks(params: DatabaseInput) -> str:
    """Muestra locks activos que podrian estar causando bloqueos.

    Returns:
        str: Locks con pid, modo, relacion y estado.
    """
    try:
        rows = await _execute_readonly("""
            SELECT
                l.pid,
                l.locktype,
                l.mode,
                l.granted,
                a.usename       AS username,
                a.datname       AS database,
                a.state,
                EXTRACT(EPOCH FROM (now() - a.query_start))::int AS seconds,
                LEFT(a.query, 100) AS query
            FROM pg_locks l
            JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE l.pid != pg_backend_pid()
            ORDER BY l.granted, seconds DESC
        """)
        return _format_response(rows, params.response_format, "Locks Activos")
    except Exception as e:
        return _handle_db_error(e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
