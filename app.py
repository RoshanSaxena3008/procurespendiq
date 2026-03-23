st.session_state["inv_processed_set"] = set()
if "inv_ai_suggestion_cache" not in st.session_state:
st.session_state["inv_ai_suggestion_cache"] = {}
                    already_processed = selected_inv in st.session_state["inv_processed_set"]

                    # ── BUG-FIX (Bug 3): fetch live DB status FIRST so that
                    # already_processed is durable across page refreshes (not only
                    # the in-memory session set which is lost on reload). ───────────
inv_detail_sql = f"""
SELECT INVOICE_STATUS, DUE_DATE, AGING_DAYS, INVOICE_AMOUNT_LOCAL,
COMPANY_CODE, FISCAL_YEAR
FROM {DB}.{SCHEMA}.fact_all_sources_vw
WHERE INVOICE_NUMBER = '{selected_inv.replace("'", "''")}'
"""
inv_df = run_df(inv_detail_sql)

                    _live_status = ""
                    if inv_df is not None and not inv_df.empty:
                        _live_status = str(inv_df.iloc[0].get("INVOICE_STATUS", "")).upper()

                    # Treat as already processed if: session flag set OR DB says Paid/Cleared
                    already_processed = (
                        selected_inv in st.session_state["inv_processed_set"]
                        or _live_status in ("PAID", "CLEARED")
                    )
suggestion = ""
if inv_df is not None and not inv_df.empty:
inv_row = inv_df.iloc[0].to_dict()
@@ -3151,8 +3162,29 @@ def reset_all():
_wh_conn.commit()
_cursor.close()

                                st.session_state["inv_processed_set"] = st.session_state.get("inv_processed_set", set()) | {selected_inv}
                                # ── BUG-FIX (Bug 2 & 3): bust every stale cache for this
                                # invoice so the re-query after rerun reflects the new
                                # 'Paid' status written by the stored procedure, rather than
                                # serving a Snowflake or session-state cached result. ──────
                                st.session_state["inv_processed_set"] = (
                                    st.session_state.get("inv_processed_set", set()) | {selected_inv}
                                )
                                # Evict AI suggestion so it regenerates with Paid status
st.session_state.get("inv_ai_suggestion_cache", {}).pop(selected_inv, None)
                                # Evict any query-result cache keyed on this invoice
                                for _cache_key in [
                                    f"inv_df_{selected_inv}",
                                    f"common_df_{selected_inv}",
                                    f"status_df_{selected_inv}",
                                ]:
                                    st.session_state.pop(_cache_key, None)
                                # Disable Snowflake result cache for the next session query
                                # so the post-rerun status read hits live data, not cache
                                try:
                                    session.sql("ALTER SESSION SET USE_CACHED_RESULT = FALSE").collect()
                                except Exception:
                                    pass  # non-fatal; best-effort cache bypass

st.session_state.pop("_inv_pay_status", None)
st.session_state.pop("_inv_pay_invoice", None)
st.session_state.pop("_inv_pay_comp_code", None)
