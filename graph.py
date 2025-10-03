import os
import re
import json
import logging
import boto3
from typing import Callable, TypedDict, Any, List, Tuple

from langchain_aws.chat_models import ChatBedrock
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langgraph.graph import StateGraph, END

from .schema import build_schema_block, borrow_conn
from psycopg2.extras import RealDictCursor  # for dict rows + named server cursor

REGION = os.environ["AWS_REGION"]
MODEL  = os.environ["MODEL_ID"]

# streaming knobs (optional)
STREAM_CHUNK_CHARS = int(os.getenv("STREAM_CHUNK_CHARS", "64"))   # "token-like" chunk
STREAM_BATCH_ROWS  = int(os.getenv("STREAM_BATCH_ROWS", "50"))    # fetchmany size

# sample rows we pass to LLM for INSIGHTS cost control
SUMMARY_SAMPLE = int(os.environ.get("SUMMARY_SAMPLE", "20"))

# per-pass timeouts (schema.borrow_conn sets a default; these can override per query)
STRICT_TIMEOUT  = os.environ.get("STRICT_TIMEOUT",  "25s")
RELAXED_TIMEOUT = os.environ.get("RELAXED_TIMEOUT", "55s")

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# ------------------------ LLM (global, reused) ------------------------
llm = ChatBedrock(
    client=boto3.client("bedrock-runtime", region_name=REGION),
    model_id=MODEL,
    model_kwargs={"temperature": 0}
)

SYSTEM = open(os.path.join(os.path.dirname(__file__),
                           "system_prompt.txt")).read().strip()
SCHEMA_TEXT = build_schema_block()

sql_prompt = PromptTemplate.from_template(
    "{system}\n\n### SCHEMA\n{schema}\n\n### USER QUESTION\n{question}\n\n### ASSISTANT\n```sql"
)
SQL_CHAIN = LLMChain(
    llm=llm,
    prompt=sql_prompt.partial(system=SYSTEM, schema=SCHEMA_TEXT),
    output_key="sql"
)

summary_prompt = PromptTemplate.from_template(
    "User question: {question}\n\n"
    "SQL executed:\n```sql\n{sql}\n```\n"
    "Sample rows (JSON):\n{rows}\n\n"
    "INSIGHTS:"
)
SUMMARY_CHAIN = LLMChain(llm=llm, prompt=summary_prompt, output_key="answer")


# ------------------------------ state ---------------------------------
class AgentState(TypedDict, total=False):
    question: str
    sql: str
    rows: str
    answer: str
    publish: Callable[[Any], None]


# ----------------------------- helpers --------------------------------
FENCE_RE  = re.compile(r"```sql(.*?)```", flags=re.S | re.I)
SELECT_RE = re.compile(r"(?is)(select\s+.+)")

_HYPHENS = re.compile(r"[\u2010-\u2015]")  # weird unicode hyphens

DATE_FILTER_RE_1 = re.compile(
    r"\s*AND\s*\(\s*(?:\w+\.)?net_price_valid_to_date\s+IS\s+NULL\s+OR\s+(?:\w+\.)?net_price_valid_to_date\s*>=\s*CURRENT_DATE\s*\)",
    re.I | re.S
)
DATE_FILTER_RE_2 = re.compile(
    r"\s*AND\s*COALESCE\(\s*(?:\w+\.)?net_price_valid_to_date\s*,\s*DATE\s*'9999-12-31'\s*\)\s*>=\s*CURRENT_DATE",
    re.I | re.S
)

def _extract_sql(txt: str) -> str:
    m = FENCE_RE.search(txt)
    if m:
        return m.group(1).strip()
    m = SELECT_RE.search(txt)
    return m.group(1).strip() if m else txt.strip()

def _normalize_hyphens(sql: str) -> str:
    return _HYPHENS.sub("-", sql)

def _strip_date_filter(sql: str) -> str:
    sql2 = DATE_FILTER_RE_1.sub("", sql)
    sql2 = DATE_FILTER_RE_2.sub("", sql2)
    sql2 = sql2.replace("WHERE  AND", "WHERE ")
    return sql2

def _chunk_emit(text: str, publish: Callable[[Any], None], size: int = STREAM_CHUNK_CHARS):
    # send small slices to simulate token streaming
    for i in range(0, len(text), max(1, size)):
        publish({"token": text[i:i+size]})

def _emit_line(line: str, publish: Callable[[Any], None]):
    publish({"token": line if line.endswith("\n") else line + "\n"})

def _md_table_header(columns: List[str]) -> Tuple[str, str]:
    head = "|" + "|".join(columns) + "|\n"
    sep  = "|" + "|".join(["---"] * len(columns)) + "|\n"
    return head, sep

def _md_row(row: dict, columns: List[str]) -> str:
    vals = []
    for c in columns:
        v = row.get(c, "")
        # flatten commas/spaces a bit for display; leave raw text
        vals.append(str(v))
    return "|" + "|".join(vals) + "|\n"


# ------------------------------- nodes --------------------------------
def gen_sql(state: AgentState) -> AgentState:
    publish = state.get("publish") or (lambda *_: None)
    out = SQL_CHAIN({"question": state["question"]})["sql"]
    sql = _normalize_hyphens(_extract_sql(out))
    log.info("Generated SQL:\n%s", sql)

    # stream SQL gradually
    _emit_line("SQL Query", publish)
    _emit_line("sql", publish)
    _emit_line("```", publish)
    _chunk_emit(sql, publish)
    _emit_line("\n```", publish)
    _emit_line("", publish)  # blank line

    return {"sql": sql}

def _columns_from_desc(desc) -> List[str]:
    if not desc:
        return []
    cols = []
    for d in desc:
        # psycopg2 usually gives a tuple, new versions may expose .name
        name = getattr(d, "name", None)
        if not name:
            try:
                name = d[0]  # first element is column name
            except Exception:
                name = str(d)
        cols.append(name)
    return cols

def _run_and_stream(sql: str, timeout: str, publish: Callable[[Any], None]) -> Tuple[List[dict], List[str], int]:
    """Execute SQL with named server cursor, stream rows, and collect results."""
    rows_acc: List[dict] = []
    columns: List[str] = []
    emitted = 0

    with borrow_conn() as conn:
        # Set timeout inside this txn before opening the server-side cursor
        with conn.cursor() as cur_cfg:
            cur_cfg.execute(f"SET LOCAL statement_timeout TO '{timeout}';")

        with conn.cursor(name="stream_cur", cursor_factory=RealDictCursor) as cur:
            cur.itersize = STREAM_BATCH_ROWS
            cur.execute(sql)

            # description may be None until we fetch — prefetch one row if needed
            desc = cur.description
            first_row = None
            if not desc:
                batch = cur.fetchmany(1)
                if batch:
                    first_row = dict(batch[0])
                desc = cur.description

            if not desc:
                # still no result set -> surface a friendly message
                raise RuntimeError("Query did not return a rowset (no SELECT result).")

            columns = _columns_from_desc(desc)

            # stream table header now that we have columns
            _emit_line("Rows", publish)
            head, sep = _md_table_header(columns)
            _emit_line(head.strip("\n"), publish)
            _emit_line(sep.strip("\n"), publish)

            # if we prefetched a row to obtain description, emit it
            if first_row is not None:
                rows_acc.append(first_row)
                emitted += 1
                _emit_line(_md_row(first_row, columns).strip("\n"), publish)

            # continue streaming the rest
            while True:
                batch = cur.fetchmany(STREAM_BATCH_ROWS)
                if not batch:
                    break
                for r in batch:
                    d = dict(r)
                    rows_acc.append(d)
                    emitted += 1
                    _emit_line(_md_row(d, columns).strip("\n"), publish)

    return rows_acc, columns, emitted

def exec_sql(state: AgentState) -> AgentState:
    publish = state.get("publish") or (lambda *_: None)
    strict_sql = state["sql"]

    # 1) Strict run
    try:
        rows_acc, columns, emitted = _run_and_stream(strict_sql, STRICT_TIMEOUT, publish)
    except Exception as e:
        log.error("Strict SQL failed: %s", e)
        # surface an error row to the stream
        _emit_line("Rows", publish)
        _emit_line("|error|", publish)
        _emit_line("|---|", publish)
        _emit_line(f"|{str(e)}|", publish)
        return {"rows": json.dumps([{"error": str(e)}], default=str)}

    if emitted > 0:
        return {
            "rows": json.dumps(rows_acc, default=str)
        }

    # 2) Fallback: strip the date-valid filter and retry (stream again)
    relaxed_sql = _strip_date_filter(strict_sql)
    if relaxed_sql != strict_sql:
        # optional note (will be ignored by main unless STREAM_INCLUDE_FINAL_JSON=true)
        publish({"note": "date_valid_filter_removed"})

        try:
            rows_acc2, columns2, emitted2 = _run_and_stream(relaxed_sql, RELAXED_TIMEOUT, publish)
            return {
                "sql": relaxed_sql,
                "rows": json.dumps(rows_acc2, default=str)
            }
        except Exception as e:
            log.error("Relaxed SQL failed: %s", e)
            _emit_line("Rows", publish)
            _emit_line("|error|", publish)
            _emit_line("|---|", publish)
            _emit_line(f"|{str(e)}|", publish)
            return {"rows": json.dumps([{"error": str(e)}], default=str)}

    # No rows either way
    return {"rows": json.dumps([], default=str)}

def summarise(state: AgentState) -> AgentState:
    publish = state.get("publish") or (lambda *_: None)

    try:
        all_rows = json.loads(state.get("rows", "[]"))
    except Exception:
        all_rows = state.get("rows", [])

    sample = all_rows[:SUMMARY_SAMPLE] if isinstance(all_rows, list) else all_rows

    answer = SUMMARY_CHAIN({
        "question": state["question"],
        "sql":      state["sql"],
        "rows":     json.dumps(sample, default=str)
    })["answer"].strip()

    # THIS LINE FIXES THE "INSIGHTS" IN ROWS ISSUE:
    _emit_line("", publish)

    # stream insights in small chunks
    _emit_line("Insights", publish)
    _chunk_emit(answer, publish)
    _emit_line("", publish)

    return {"answer": answer}

# ------------------------------ graph ---------------------------------
graph = StateGraph(AgentState)
graph.add_node("GenerateSQL", gen_sql)
log.info("Generate SQL node added")
graph.add_node("RunSQL",      exec_sql)
graph.add_node("Summarize",   summarise)

graph.add_edge("GenerateSQL", "RunSQL")
graph.add_edge("RunSQL",      "Summarize")
graph.add_edge("Summarize",   END)

graph.set_entry_point("GenerateSQL")
AGENT = graph.compile()
