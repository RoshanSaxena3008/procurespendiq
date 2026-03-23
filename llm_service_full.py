"""
LLM service for ProcureSpendIQ Analytics.

Enhancements:
  - Cache-first strategy: session cache is checked before every AI call (req 8).
  - Data Vault YAML enrichment: AI generates Hub/Satellite/Link/Hierarchy
    metadata whenever a new table is detected (req 4, 7).
  - No emojis in any output or log messages (req 1).
  - AI model and temperature come from app_settings.yaml (req 13, 15).
"""

from __future__ import annotations

import json
import logging
import re
import textwrap
from typing import Dict, List, Optional

import pandas as pd
import yaml
from openai import AzureOpenAI

from config import Config
from db_service import (
    cache_get,
    cache_set,
    get_table_columns,
    get_primary_keys,
    list_tables_in_schema,
    run_df,
    sql_escape,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Azure OpenAI client
# ---------------------------------------------------------------------------

_client = AzureOpenAI(
    azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
    api_key=Config.AZURE_OPENAI_API_KEY,
    api_version=Config.AZURE_OPENAI_API_VERSION,
)


# ---------------------------------------------------------------------------
# Schema prompt builder
# ---------------------------------------------------------------------------

def load_schema_from_yaml(yaml_path: str | None = None) -> str:
    """
    Build the SQL-generation system prompt from schema_metadata.yaml.
    Falls back to a minimal prompt if the file is missing.
    """
    path = yaml_path or Config.SCHEMA_METADATA_FILE
    try:
        with open(path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh)
    except FileNotFoundError:
        logger.warning("Schema metadata file not found: %s. Using minimal prompt.", path)
        return (
            f"You are a SQL expert for Microsoft Fabric SQL (T-SQL).\n"
            f"DATABASE: {Config.FABRIC_DATABASE}\nSCHEMA: {Config.SCHEMA}\n"
            "Generate T-SQL SELECT queries only. Use DATETRUNC, GETDATE, TOP N syntax."
        )

    prompt = textwrap.dedent(f"""
        You are a SQL expert for Microsoft Fabric SQL (T-SQL).

        DATABASE: {Config.FABRIC_DATABASE}
        SCHEMA: {Config.SCHEMA}

    """)

    if "custom_instructions" in schema_data:
        prompt += schema_data["custom_instructions"] + "\n\n"

    prompt += "AVAILABLE TABLES:\n\n"

    for table in schema_data.get("tables", []):
        base = table.get("base_table", {})
        actual_schema = base.get("schema", Config.SCHEMA)
        actual_table  = base.get("table", "")

        prompt += f"Table: {actual_schema}.{actual_table}\n"
        prompt += f"Friendly name: {table.get('name', '')} (DO NOT use in SQL)\n"
        prompt += f"Description: {table.get('description', '')}\n"
        prompt += "Columns:\n"

        for dim in table.get("dimensions", []):
            col  = dim.get("expr", dim.get("name", ""))
            dtype = dim.get("data_type", "varchar")
            desc = dim.get("description", "")
            syns = dim.get("synonyms", [])
            prompt += f"  - {col} ({dtype}): {desc}\n"
            if syns:
                prompt += f"    Synonyms: {', '.join(syns)}\n"

        for measure in table.get("measures", []):
            col  = measure.get("expr", measure.get("name", ""))
            dtype = measure.get("data_type", "number")
            desc = measure.get("description", "")
            agg  = measure.get("default_aggregation", "sum").upper()
            prompt += f"  - {col} ({dtype}): {desc}  [Default agg: {agg}]\n"

        prompt += "\n"

    if "verified_queries" in schema_data:
        prompt += "VERIFIED QUERIES - USE THESE EXACT QUERIES WHEN THE QUESTION MATCHES:\n\n"
        for vq in schema_data.get("verified_queries", []):
            q   = vq.get("question", "")
            sql = vq.get("sql", "").strip()
            if q and sql:
                prompt += f"Question: {q}\nSQL:\n{sql}\n\n"

    prompt += textwrap.dedent("""
        CRITICAL SQL GENERATION RULES:
        1. Always use fully qualified table names: schema.table_name
        2. Never use friendly names in SQL - use actual table names
        3. Table names are case-sensitive
        4. Only generate SELECT queries
        5. Use T-SQL syntax: TOP N instead of LIMIT

        DATE FUNCTIONS (T-SQL):
          Month grouping : DATETRUNC(month, column)
          Current date   : GETDATE()
          6 months ago   : DATEADD(month, -6, GETDATE())
          Never use      : DATE_TRUNC, CURRENT_DATE()

        WINDOW FUNCTIONS for top-N per group:
          Use ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
          Never use bare TOP N for per-group rankings.

        OUTPUT FORMAT (ABSOLUTE RULE):
          - Return ONLY the raw SQL query.
          - No markdown, no code fences, no explanations, no bullet points.
          - Do NOT write "Descriptive:", "Prescriptive:", or any prose.
          - If the question is vague or conversational, return exactly:
            SELECT 'Please ask a specific question about invoices, vendors, spend, or payments.' AS MESSAGE
          - The very first character of your response must be S (for SELECT) or W (for WITH).
    """)

    return prompt.replace("{schema}", Config.SCHEMA)


# Preload system prompt at module import time
SYSTEM_PROMPT = load_schema_from_yaml()


# ---------------------------------------------------------------------------
# SQL generation (cache-first, req 8)
# ---------------------------------------------------------------------------

def _clean_sql(raw: str) -> str:
    """Remove markdown fences and common dialect issues from generated SQL."""
    sql = raw.replace("```sql", "").replace("```", "").strip()

    sql = sql.replace("CURRENT_DATE()", "GETDATE()").replace("CURRENT_DATE", "GETDATE()")

    replacements = [
        (r"DATE_TRUNC\('month',\s*",   "DATETRUNC(month, "),
        (r"DATE_TRUNC\('quarter',\s*", "DATETRUNC(quarter, "),
        (r"DATE_TRUNC\('year',\s*",    "DATETRUNC(year, "),
        (r"DATEADD\('month',\s*",      "DATEADD(month, "),
        (r"DATEADD\('year',\s*",       "DATEADD(year, "),
        (r"DATEADD\('day',\s*",        "DATEADD(day, "),
    ]
    for pattern, repl in replacements:
        sql = re.sub(pattern, repl, sql, flags=re.IGNORECASE)

    # LIMIT N -> TOP N
    m = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
    if m:
        limit_n = m.group(1)
        sql = re.sub(r"\bLIMIT\s+\d+\b", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bSELECT\b", f"SELECT TOP {limit_n}", sql, count=1, flags=re.IGNORECASE)

    # Wrap ORDER BY CASE in MIN() when inside aggregate context
    if re.search(r"ORDER\s+BY\s+CASE\s+WHEN", sql, re.IGNORECASE):
        def _wrap_case(match: re.Match) -> str:
            prefix = match.group(1)
            expr   = match.group(2)
            if not re.match(r"^\s*(MIN|MAX|AVG|SUM|COUNT)\s*\(", expr, re.IGNORECASE):
                return f"{prefix}MIN({expr})"
            return match.group(0)

        sql = re.sub(
            r"(ORDER\s+BY\s+)(CASE\s+WHEN.*?END)(?=\s*(?:ASC|DESC)?\s*(?:,|$))",
            _wrap_case,
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )

    sql = re.sub(r"\bCOUNT\s*\(\s*\)", "COUNT(*)", sql, flags=re.IGNORECASE)
    sql = _remove_cte_order_by(sql)
    return sql


def _remove_cte_order_by(sql: str) -> str:
    """Strip ORDER BY clauses that appear immediately before a closing CTE paren."""
    lines  = sql.split("\n")
    result = []
    for idx, line in enumerate(lines):
        stripped = line.strip().upper()
        if stripped.startswith("ORDER BY"):
            is_cte = False
            for j in range(idx + 1, len(lines)):
                nxt = lines[j].strip()
                if nxt:
                    is_cte = nxt.startswith(")")
                    break
            if is_cte:
                continue
        result.append(line)
    return "\n".join(result)


def generate_sql(user_question: str, temperature: float | None = None) -> str:
    """
    Convert a natural language question into a T-SQL query.

    Flow:
      1. Check session cache (Warehouse).
      2. If no hit, call Azure OpenAI.
      3. Store result in cache.
    """
    temperature = temperature if temperature is not None else float(
        Config._settings.get("ai", {}).get("sql_generation_temperature", 0.1)
        if hasattr(Config, "_settings") else 0.1
    )

    # Cache lookup (req 8)
    cached = cache_get(user_question)
    if cached and cached.get("sql"):
        logger.info("SQL cache hit for question: %s", user_question[:80])
        return cached["sql"]

    try:
        response = _client.chat.completions.create(
            model=Config.AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_question},
            ],
            temperature=temperature,
            max_tokens=Config._settings.get("ai", {}).get("max_tokens", 4096)
            if hasattr(Config, "_settings") else 4096,
        )
        raw_sql = response.choices[0].message.content.strip()
        sql     = _clean_sql(raw_sql)

        # Guard: if the model returned prose instead of SQL, return a safe fallback
        sql_upper = sql.lstrip(";").strip().upper()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            logger.warning(
                "generate_sql returned non-SQL response (first 80 chars): %s",
                sql[:80]
            )
            # Try to extract any SELECT/WITH block from the prose
            import re as _re
            match = _re.search(r"(WITH\s+\w|SELECT\s+)", sql, _re.IGNORECASE)
            if match:
                sql = sql[match.start():].strip()
                logger.info("Extracted SQL from prose starting at position %d", match.start())
            else:
                sql = (
                    "SELECT 'Please ask a specific question about invoices, "
                    "vendors, spend, or payments.' AS MESSAGE"
                )

        return sql

    except Exception as exc:
        raise RuntimeError(f"SQL generation failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Generic AI completion
# ---------------------------------------------------------------------------

def cortex_complete(
    prompt: str,
    model: Optional[str] = None,
    temperature: float = 0.3,
) -> str:
    """
    General-purpose Azure OpenAI completion.
    Replaces SNOWFLAKE.CORTEX.COMPLETE.
    """
    try:
        response = _client.chat.completions.create(
            model=model or Config.PRESCRIPTIVE_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("AI completion failed: %s", exc)
        return f"AI completion unavailable: {exc}"


# ---------------------------------------------------------------------------
# Prescriptive insights
# ---------------------------------------------------------------------------

def generate_prescriptive_insights(
    data_summary: str,
    question: str,
    temperature: float = 0.3,
) -> str:
    """Generate 3-5 prescriptive recommendations from query result data."""
    prompt = textwrap.dedent(f"""
        You are a procurement analytics expert.

        USER QUESTION: {question}

        DATA SUMMARY:
        {data_summary}

        Provide 3-5 prescriptive recommendations.
        For each recommendation:
        1. State the finding with exact numbers from the data.
        2. State a concrete action to take.
        3. Assign a priority: High, Medium, or Low.
        4. Quantify expected impact where possible.

        Format each recommendation as:

        [PRIORITY] - [TITLE]
          Finding: <specific data point with numbers>
          Action: <concrete next step>
          Expected Impact: <quantified benefit>

        Focus on: cost reduction, risk mitigation, vendor management, cash flow.
    """).strip()

    return cortex_complete(prompt, temperature=temperature)


# ---------------------------------------------------------------------------
# Invoice AI suggestion
# ---------------------------------------------------------------------------

def generate_ai_invoice_suggestion(
    invoice_number: str,
    invoice_data: Dict,
    status_history: str = "",
) -> str:
    """Return a 2-3 sentence actionable suggestion for a single invoice."""
    prompt = textwrap.dedent(f"""
        You are a procurement specialist. Review the invoice below and provide a brief,
        actionable suggestion in 2-3 sentences maximum.

        INVOICE: {invoice_number}
        STATUS:  {invoice_data.get('INVOICE_STATUS', 'Unknown')}
        AMOUNT:  {invoice_data.get('INVOICE_AMOUNT_LOCAL', 0):,.2f}
        VENDOR:  {invoice_data.get('VENDOR_ID', 'Unknown')}
        DUE DATE: {invoice_data.get('DUE_DATE', 'Unknown')}
        AGING DAYS: {invoice_data.get('AGING_DAYS', 0)}
        {f'STATUS HISTORY: {status_history}' if status_history else ''}

        Provide a specific, actionable recommendation.
    """).strip()

    return cortex_complete(prompt, temperature=0.3)


# ---------------------------------------------------------------------------
# Data Vault YAML enrichment (req 4, 7)
# ---------------------------------------------------------------------------

def _infer_data_vault_objects(
    table_name: str,
    columns: List[Dict],
    primary_keys: List[str],
) -> Dict:
    """
    Use Azure OpenAI to infer Data Vault 2.0 Hub, Satellite, Link, and
    hierarchy definitions for a given table schema.

    Returns a dict suitable for merging into schema_metadata.yaml.
    """
    col_descriptions = "\n".join(
        f"  - {c['COLUMN_NAME']} ({c['DATA_TYPE']}, nullable={c['IS_NULLABLE']})"
        for c in columns
    )
    pk_list = ", ".join(primary_keys) if primary_keys else "unknown"

    prompt = textwrap.dedent(f"""
        You are a Data Vault 2.0 architect.

        Given the table schema below, produce JSON with the following structure:

        {{
          "data_vault": {{
            "hub": {{
              "name": "<{Config.DATA_VAULT_HUB_PREFIX}tablename>",
              "business_keys": ["<col1>", ...],
              "description": "<one sentence>"
            }},
            "satellite": {{
              "name": "<{Config.DATA_VAULT_SAT_PREFIX}tablename>",
              "descriptive_attributes": ["<col1>", ...],
              "description": "<one sentence>"
            }},
            "links": [
              {{
                "name": "<{Config.DATA_VAULT_LINK_PREFIX}tablename_othertable>",
                "hubs": ["<hub1>", "<hub2>"],
                "description": "<one sentence>"
              }}
            ],
            "hierarchy": {{
              "parent_column": "<col>",
              "child_column":  "<col>",
              "description": "<one sentence or null>"
            }}
          }},
          "sample_questions": [
            "<natural language question 1>",
            "<natural language question 2>",
            "<natural language question 3>"
          ]
        }}

        TABLE NAME: {table_name}
        PRIMARY KEYS: {pk_list}
        COLUMNS:
        {col_descriptions}

        Return only valid JSON, no markdown, no explanation.
    """).strip()

    try:
        raw = cortex_complete(
            prompt,
            temperature=float(
                Config._settings.get("ai", {}).get("yaml_generation_temperature", 0.2)
                if hasattr(Config, "_settings") else 0.2
            ),
        )
        # Strip accidental markdown fences
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Data Vault inference failed for %s: %s", table_name, exc)
        return {}


def enrich_yaml_for_table(
    table_name: str,
    schema: str = "INFORMATION_MART",
    yaml_path: str | None = None,
) -> bool:
    """
    AI-driven YAML enrichment for a single table (req 4, 7).

    Steps:
      1. Fetch column metadata from the Fabric Lakehouse.
      2. Call Azure OpenAI to infer Data Vault objects and sample questions.
      3. Merge the result into schema_metadata.yaml under the matching table
         entry (or create a new entry if absent).

    Returns True on success.
    """
    if not Config.ENABLE_DATA_VAULT_AI:
        logger.info("Data Vault AI enrichment is disabled in app_settings.yaml.")
        return False

    yaml_path = yaml_path or Config.SCHEMA_METADATA_FILE

    # Load existing YAML
    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        schema_data = {"tables": [], "verified_queries": []}

    tables = schema_data.get("tables", [])

    # Check if this table is already fully enriched
    existing = next(
        (t for t in tables if t.get("base_table", {}).get("table", "").upper() == table_name.upper()),
        None,
    )
    if existing and existing.get("data_vault"):
        logger.info("Table %s already has Data Vault metadata. Skipping.", table_name)
        return True

    # Fetch schema from Fabric
    cols_df = get_table_columns(table_name, schema)
    if cols_df.empty:
        logger.warning("No columns found for %s.%s", schema, table_name)
        return False

    pks      = get_primary_keys(table_name, schema)
    columns  = cols_df.to_dict("records")

    # AI inference
    enrichment = _infer_data_vault_objects(table_name, columns, pks)
    if not enrichment:
        return False

    # Merge into YAML
    if existing is None:
        # Build a new minimal table entry
        new_entry: Dict = {
            "name": table_name.lower(),
            "description": f"Auto-discovered table: {schema}.{table_name}",
            "base_table": {"database": Config.FABRIC_DATABASE, "schema": schema, "table": table_name},
        }
        if pks:
            new_entry["primary_key"] = {"columns": [k.lower() for k in pks]}
        # Add inferred columns as dimensions/measures
        dims, measures = [], []
        for col in columns:
            col_name = col["COLUMN_NAME"]
            col_type = col["DATA_TYPE"].lower()
            if any(t in col_type for t in ("int", "float", "decimal", "numeric", "money")):
                measures.append({"name": col_name.lower(), "expr": col_name, "data_type": "number",
                                 "default_aggregation": "sum"})
            else:
                dims.append({"name": col_name.lower(), "expr": col_name, "data_type": col_type})
        if dims:
            new_entry["dimensions"] = dims
        if measures:
            new_entry["measures"] = measures
        tables.append(new_entry)
        existing = tables[-1]

    # Attach Data Vault metadata
    if "data_vault" in enrichment:
        existing["data_vault"] = enrichment["data_vault"]

    # Attach sample questions
    if "sample_questions" in enrichment:
        existing["sample_questions"] = enrichment["sample_questions"]

    schema_data["tables"] = tables

    # Write back
    try:
        with open(yaml_path, "w", encoding="utf-8") as fh:
            yaml.dump(schema_data, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)
        logger.info("YAML enriched for table: %s", table_name)
        return True
    except Exception as exc:
        logger.error("Failed to write enriched YAML: %s", exc)
        return False


def auto_discover_and_enrich_yaml(
    schema: str = "INFORMATION_MART",
    yaml_path: str | None = None,
) -> List[str]:
    """
    Scan the schema for tables not yet in the YAML and enrich each one.
    Returns a list of table names that were processed.

    This function is the entry point for req 4 and req 7:
    'If a new table is added, the YAML should be updated automatically.'
    """
    yaml_path = yaml_path or Config.SCHEMA_METADATA_FILE
    processed = []

    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        schema_data = {"tables": []}

    known_tables = {
        t.get("base_table", {}).get("table", "").upper()
        for t in schema_data.get("tables", [])
    }

    try:
        all_tables_df = list_tables_in_schema(schema)
    except Exception as exc:
        logger.error("Could not list tables in schema %s: %s", schema, exc)
        return processed

    for _, row in all_tables_df.iterrows():
        tname = str(row.get("TABLE_NAME", "")).strip().upper()
        if tname and tname not in known_tables:
            logger.info("New table detected: %s. Running AI enrichment.", tname)
            ok = enrich_yaml_for_table(tname, schema, yaml_path)
            if ok:
                processed.append(tname)

    return processed


# ---------------------------------------------------------------------------
# Module self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Testing SQL generation...")
    q   = "Show monthly procurement spend for the last 6 months"
    sql = generate_sql(q)
    print(f"Question : {q}")
    print(f"SQL      :\n{sql}")

    print("\nTesting prescriptive insights...")
    summary = "Top vendor: VENDOR_001 with $4.2M spend (27% of total). Total: $15.5M."
    insights = generate_prescriptive_insights(summary, "Show top vendors by spend")
    print(f"Insights:\n{insights}")
