"""
Enhanced LLM service for ProcureIQ Analytics with contextual memory.

Enhancements:
  - Cache-first strategy: session cache is checked before every AI call
  - Contextual memory: Short-term and long-term context for better queries
  - Data Vault YAML enrichment: AI generates Hub/Satellite metadata
  - No emojis in any output or log messages
  - AI model and temperature from app_settings.yaml
  - Multi-turn conversation support with memory management
"""

from __future__ import annotations

import json
import logging
import re
import textwrap
from typing import Dict, List, Optional
import streamlit as st

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
from genie_contextual_memory import ContextualMemoryManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Azure OpenAI client
# ---------------------------------------------------------------------------

_client = AzureOpenAI(
    azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
    api_key=Config.AZURE_OPENAI_API_KEY,
    api_version=Config.AZURE_OPENAI_API_VERSION,
)

# Initialize context memory manager
_memory_manager = None

def get_memory_manager():
    """Get or initialize the contextual memory manager."""
    global _memory_manager
    if _memory_manager is None:
        _memory_manager = ContextualMemoryManager(
            session_state=st.session_state if 'st' in globals() else None
        )
    return _memory_manager


# ---------------------------------------------------------------------------
# Schema prompt builder with contextual awareness
# ---------------------------------------------------------------------------

def load_schema_from_yaml(yaml_path: str | None = None) -> str:
    """
    Build the SQL-generation system prompt from schema_metadata.yaml.
    Includes contextual memory awareness for multi-turn conversations.
    Falls back to a minimal prompt if the file is missing.
    """
    path = yaml_path or Config.SCHEMA_METADATA_FILE
    try:
        with open(path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh)
    except FileNotFoundError:
        logger.warning("Schema metadata file not found: %s. Using minimal prompt.", path)
        return _build_minimal_schema_prompt()

    prompt = textwrap.dedent(f"""
        You are a SQL expert for Microsoft Fabric SQL (T-SQL).
        You have access to conversation history and previous successful queries.

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

    prompt += _build_sql_rules_section()
    
    # Add contextual memory instructions
    if Config.ENABLE_CONTEXTUAL_MEMORY:
        prompt += _build_context_memory_section()

    return prompt.replace("{schema}", Config.SCHEMA)


def _build_minimal_schema_prompt() -> str:
    """Build a minimal schema prompt when metadata file is missing."""
    return textwrap.dedent(f"""
        You are a SQL expert for Microsoft Fabric SQL (T-SQL).
        DATABASE: {Config.FABRIC_DATABASE}
        SCHEMA: {Config.SCHEMA}
        Generate T-SQL SELECT queries only. Use DATETRUNC, GETDATE, TOP N syntax.
        
        {_build_sql_rules_section()}
    """)


def _build_sql_rules_section() -> str:
    """Build the critical SQL generation rules section."""
    return textwrap.dedent("""
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
            SELECT 'Please ask a specific question about purchases, vendors, or spend.' AS MESSAGE
          - The very first character of your response must be S (for SELECT) or W (for WITH).
    """)


def _build_context_memory_section() -> str:
    """Build the contextual memory instructions section."""
    return textwrap.dedent("""
        CONTEXTUAL MEMORY AND CONVERSATION HISTORY:
        
        You have access to previous successful queries in this conversation.
        Use them as patterns for generating new queries when the context is similar.
        
        If a similar question was asked before:
        - Reuse the same table structure and joins
        - Apply the same filtering patterns
        - Use consistent column naming and aggregations
        
        Previous contexts are provided in the prompt as reference.
        Prefer simple, straightforward queries over complex ones.
        
        IMPORTANT:
        - Do NOT reference conversation history in the SQL output
        - Still generate the complete query independently
        - Use history only as a pattern reference
    """)


# Preload system prompt at module import time
SYSTEM_PROMPT = load_schema_from_yaml()


# ---------------------------------------------------------------------------
# Generate SQL with contextual memory
# ---------------------------------------------------------------------------

def generate_sql(
    question: str,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    include_context: bool = True,
) -> str:
    """
    Generate SQL from a natural language question.
    
    Incorporates:
      - Short-term session memory
      - Long-term persistent memory
      - Cache lookup
      - Schema information
    
    Args:
        question: Natural language question
        user_id: Optional user identifier
        session_id: Optional session identifier
        include_context: Whether to include memory context in prompt
        
    Returns:
        Generated T-SQL query or error message
    """
    
    # Step 1: Check session cache
    cache_key = f"genie_sql:{question}"
    cached_sql = cache_get(cache_key) if Config.CACHE_ENABLED else None
    if cached_sql:
        logger.info("SQL cache hit for question")
        return cached_sql
    
    # Step 2: Initialize memory manager
    memory = get_memory_manager()
    if Config.ENABLE_CONTEXTUAL_MEMORY:
        memory.ensure_long_term_memory_table()
    
    # Step 3: Build LLM prompt with context
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    
    # Add short-term context
    if Config.SHORT_TERM_MEMORY_ENABLED and include_context:
        short_context = memory.get_short_term_context()
        if short_context:
            messages.append({"role": "system", "content": short_context})
    
    # Add long-term context
    if Config.LONG_TERM_MEMORY_ENABLED and include_context:
        relevant_contexts = memory.retrieve_relevant_contexts(question, limit=3)
        if relevant_contexts:
            long_context = memory.format_long_term_context_for_prompt(relevant_contexts)
            messages.append({"role": "system", "content": long_context})
    
    # Step 4: Add user question
    messages.append({"role": "user", "content": question})
    
    # Step 5: Call Azure OpenAI
    try:
        response = _client.chat.completions.create(
            model=Config.AZURE_OPENAI_DEPLOYMENT,
            messages=messages,
            temperature=0.2,  # Low temperature for deterministic SQL
            max_tokens=2000,
            top_p=0.95,
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Step 6: Cache the result
        if Config.CACHE_ENABLED and sql.upper().startswith(("SELECT", "WITH")):
            cache_set(cache_key, sql, ttl_seconds=Config.CACHE_TTL_SECONDS)
        
        # Step 7: Add to short-term memory
        if Config.SHORT_TERM_MEMORY_ENABLED:
            memory.add_message_to_short_term(
                question=question,
                answer=sql,
                sql=sql,
                context_data={"model": Config.AZURE_OPENAI_DEPLOYMENT}
            )
        
        logger.info("SQL generated successfully")
        return sql
        
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return f"Error: Could not generate SQL. {str(e)}"


def generate_sql_with_memory(
    question: str,
    memory_context: Optional[str] = None,
    **kwargs
) -> str:
    """
    Generate SQL with explicit memory context.
    
    This is a convenience function for when you want to
    provide custom memory context instead of auto-retrieving it.
    
    Args:
        question: Natural language question
        memory_context: Custom memory context to include
        **kwargs: Additional arguments for generate_sql
        
    Returns:
        Generated T-SQL query
    """
    
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    
    if memory_context:
        messages.append({"role": "system", "content": memory_context})
    
    messages.append({"role": "user", "content": question})
    
    try:
        response = _client.chat.completions.create(
            model=Config.AZURE_OPENAI_DEPLOYMENT,
            messages=messages,
            temperature=0.2,
            max_tokens=2000,
            top_p=0.95,
        )
        
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Error generating SQL with memory: {e}")
        return f"Error: {str(e)}"


# ---------------------------------------------------------------------------
# Other LLM functions (maintain compatibility)
# ---------------------------------------------------------------------------

def cortex_complete(
    prompt: str,
    temperature: float = 0.7,
    max_tokens: int = 500,
) -> str:
    """
    General-purpose text completion using Azure OpenAI.
    
    Args:
        prompt: Text prompt
        temperature: Sampling temperature
        max_tokens: Maximum tokens in response
        
    Returns:
        Generated text
    """
    try:
        response = _client.chat.completions.create(
            model=Config.AZURE_OPENAI_DEPLOYMENT,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Error in cortex_complete: {e}")
        return f"Error: {str(e)}"


def generate_prescriptive_insights(
    question: str,
    data: Optional[pd.DataFrame] = None,
) -> str:
    """
    Generate prescriptive insights using Azure OpenAI.
    
    Args:
        question: Analytical question
        data: Optional data context
        
    Returns:
        Generated insights text
    """
    
    prompt = f"Based on the question '{question}'"
    
    if data is not None and not data.empty:
        # Include data summary
        summary = data.describe().to_string()
        prompt += f"\n\nData Summary:\n{summary}"
    
    prompt += "\n\nProvide actionable insights and recommendations."
    
    return cortex_complete(prompt, temperature=0.8, max_tokens=1000)


def generate_ai_invoice_suggestion(
    invoice_data: Dict,
) -> str:
    """
    Generate AI suggestion for invoice processing.
    
    Args:
        invoice_data: Invoice information dictionary
        
    Returns:
        Generated suggestion
    """
    
    prompt = f"""
    Analyze this invoice data and provide processing suggestions:
    
    Invoice: {json.dumps(invoice_data, indent=2)}
    
    Provide:
    1. Categorization recommendation
    2. Vendor classification
    3. Any anomalies or risks detected
    4. Processing priority
    """
    
    return cortex_complete(prompt, temperature=0.7, max_tokens=800)


# ---------------------------------------------------------------------------
# Memory management helpers
# ---------------------------------------------------------------------------

def save_verified_query(
    question: str,
    sql: str,
    tables_used: List[str],
    filters: Dict,
) -> Optional[int]:
    """
    Save a verified query to long-term memory.
    
    Args:
        question: User question
        sql: Verified SQL query
        tables_used: List of tables used
        filters: Applied filters
        
    Returns:
        Context ID if successful
    """
    memory = get_memory_manager()
    return memory.add_to_long_term_memory(
        question=question,
        answer="Verified query",
        sql=sql,
        tables_used=tables_used,
        filters=filters,
        is_verified=True,
    )


def get_memory_stats() -> Dict:
    """Get current memory statistics."""
    memory = get_memory_manager()
    return memory.get_memory_stats()


def cleanup_memory(days_old: int = 30) -> int:
    """
    Clean up old memory contexts.
    
    Args:
        days_old: Delete contexts older than this many days
        
    Returns:
        Number of contexts deleted
    """
    memory = get_memory_manager()
    return memory.cleanup_old_contexts(days_old=days_old)
