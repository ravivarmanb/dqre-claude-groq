"""
Enhanced LangGraph DQ Engine
Nodes: feedback → extract → generate → validate_sql → deduplicate → sync → execute → impact_analysis
"""
import os, json, uuid, re, sqlite3
from datetime import date, datetime
from typing import TypedDict, List, Optional, Annotated
import operator

from dotenv import load_dotenv
load_dotenv()  # load .env into os.environ

from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
import chromadb

from db_setup import get_conn, DB_PATH

# ── Config ──────────────────────────────────────────────────────────────────
CHROMA_PATH = os.path.join(os.path.dirname(__file__), "chroma_store")

def _get_llm():
    api_key = os.environ.get("GROQ_API_KEY", "")
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        groq_api_key=api_key,
        temperature=0.2,
    )

chroma_client   = chromadb.PersistentClient(path=CHROMA_PATH)
rule_collection = chroma_client.get_or_create_collection(
    name="dq_rules", metadata={"hnsw:space": "cosine"}
)

# ── State ────────────────────────────────────────────────────────────────────
class DQState(TypedDict):
    source_folder:     Optional[str]
    source_text:       Optional[str]
    source_type:       str
    run_id:            str
    extracted_text:    str
    raw_rules:         List[dict]
    validated_rules:   List[dict]
    deduped_rules:     List[dict]
    messages:          Annotated[List[str], operator.add]
    execution_results: List[dict]
    feedback_context:  str
    impact_summary:    dict
    error:             Optional[str]

# ── Shared schema context ────────────────────────────────────────────────────
SCHEMA = """
SQLite database schema for UK Health Insurance:

members(member_id TEXT PK, nhs_number TEXT UNIQUE, first_name TEXT, last_name TEXT,
        date_of_birth DATE, gender TEXT[M/F/Other], postcode TEXT, email TEXT, phone TEXT,
        registration_date DATE, status TEXT[ACTIVE/LAPSED/CANCELLED])

policies(policy_id TEXT PK, member_id TEXT FK->members, policy_type TEXT[BASIC/STANDARD/COMPREHENSIVE/ELITE],
         start_date DATE, end_date DATE nullable, premium_monthly REAL, excess_amount REAL nullable,
         status TEXT[ACTIVE/LAPSED/CANCELLED/PENDING], insurer_code TEXT, created_at TIMESTAMP)

claims(claim_id TEXT PK, policy_id TEXT FK->policies, member_id TEXT FK->members,
       claim_date DATE, treatment_date DATE, diagnosis_code TEXT, treatment_type TEXT,
       provider_id TEXT FK->providers, claimed_amount REAL, approved_amount REAL nullable,
       status TEXT[PENDING/APPROVED/REJECTED/INVESTIGATING],
       rejection_reason TEXT nullable, processed_date DATE nullable)

providers(provider_id TEXT PK, provider_name TEXT, provider_type TEXT[GP/HOSPITAL/CLINIC/SPECIALIST/PHARMACY],
          postcode TEXT, nhs_registered INTEGER[0/1], active INTEGER[0/1], contract_start DATE)

premiums(premium_id TEXT PK, policy_id TEXT FK->policies, due_date DATE, amount REAL,
         paid_date DATE nullable, payment_method TEXT[DD/CARD/BACS/CHEQUE] nullable,
         status TEXT[PENDING/PAID/OVERDUE/WAIVED])

waitlists(waitlist_id TEXT PK, member_id TEXT FK->members, treatment_type TEXT,
          referral_date DATE, target_date DATE nullable, actual_date DATE nullable,
          status TEXT[WAITING/COMPLETED/CANCELLED])
"""


# ═══════════════════════════════════════════════════════════════════════════
# NODE 1 – Load Feedback Context
# ═══════════════════════════════════════════════════════════════════════════
def node_load_feedback(state: DQState) -> dict:
    conn = get_conn()
    rows = conn.execute("""
        SELECT ef.feedback_type, ef.comment, ef.suggested_threshold,
               dr.rule_name, dr.rule_type, dr.target_table, dr.threshold_pct
        FROM execution_feedback ef
        JOIN dq_rules dr ON dr.rule_id = ef.rule_id
        ORDER BY ef.created_at DESC LIMIT 30
    """).fetchall()
    conn.close()
    if not rows:
        return {"feedback_context": "", "messages": ["📭 No prior feedback"]}
    parts = [
        f"  Rule='{r['rule_name']}' ({r['rule_type']} on {r['target_table']}): "
        f"feedback={r['feedback_type']}, comment='{r['comment'] or '—'}', "
        f"current_threshold={r['threshold_pct']}%, suggested={r['suggested_threshold'] or '—'}"
        for r in rows
    ]
    return {"feedback_context": "\n".join(parts),
            "messages": [f"📬 Loaded {len(rows)} prior feedback entries for AI"]}


# ═══════════════════════════════════════════════════════════════════════════
# NODE 2 – Extract Knowledge
# ═══════════════════════════════════════════════════════════════════════════
def node_extract_knowledge(state: DQState) -> dict:
    if state["source_type"] == "execute_only":
        return {"extracted_text": "", "messages": ["⚡ Execute-only — skipping extraction"]}

    msgs, parts = [], []

    if state["source_type"] == "folder":
        folder = state.get("source_folder", "")
        if not os.path.isdir(folder):
            return {"extracted_text": "", "messages": [f"⚠️ Folder not found: {folder}"]}
        for fname in sorted(os.listdir(folder)):
            fpath = os.path.join(folder, fname)
            try:
                if fname.lower().endswith(".pdf"):
                    import pdfplumber
                    with pdfplumber.open(fpath) as pdf:
                        for pg in pdf.pages:
                            t = pg.extract_text()
                            if t: parts.append(t)
                    msgs.append(f"📄 PDF: {fname}")
                elif fname.lower().endswith((".html", ".htm")):
                    from bs4 import BeautifulSoup
                    with open(fpath, "r", errors="ignore") as f:
                        parts.append(BeautifulSoup(f.read(), "html.parser").get_text("\n"))
                    msgs.append(f"🌐 HTML: {fname}")
                elif fname.lower().endswith(".txt"):
                    with open(fpath, "r", errors="ignore") as f:
                        parts.append(f.read())
                    msgs.append(f"📝 TXT: {fname}")
            except Exception as exc:
                msgs.append(f"❌ {fname}: {exc}")

    elif state["source_type"] == "text":
        parts = [state.get("source_text", "")]
        msgs.append("📝 Using provided text source")

    extracted = "\n\n".join(parts)[:18000]
    char_count = len(extracted)
    msgs.append(f"📊 Total extracted: {char_count:,} chars from {len(parts)} source(s)")
    return {"extracted_text": extracted, "messages": msgs}


# ═══════════════════════════════════════════════════════════════════════════
# NODE 3 – Generate Rules (Gemini)
# ═══════════════════════════════════════════════════════════════════════════
def node_generate_rules(state: DQState) -> dict:
    if state["source_type"] == "execute_only":
        return {"raw_rules": [], "messages": ["⏭️ Skipping generation (execute-only)"]}

    feedback_section = (
        f"\n\nUSER FEEDBACK FROM PREVIOUS RUNS — incorporate learnings:\n{state['feedback_context']}"
        if state.get("feedback_context") else ""
    )

    conn = get_conn()
    existing = [r[0] for r in conn.execute("SELECT rule_name FROM dq_rules").fetchall()]
    conn.close()
    existing_hint = ("Existing rules (do NOT recreate):\n" +
                     "\n".join(f"  - {n}" for n in existing[:25])) if existing else ""

    knowledge = (state.get("extracted_text") or "")[:10000]

    # If no meaningful content was provided, bail out early
    if not knowledge.strip():
        msg = "⚠️ No knowledge source provided. Please enter DQ-related text or select a folder."
        return {"raw_rules": [], "messages": [msg]}

    prompt = f"""You are a Senior Data Quality Engineer specialising in UK Private Health Insurance.
Deep expertise in FCA COBS, NHS data standards, and insurance operational data.

DATABASE SCHEMA:
{SCHEMA}

USER-PROVIDED KNOWLEDGE SOURCE:
{knowledge}
{feedback_section}

{existing_hint}

TASK — Generate DQ rules strictly grounded in the knowledge source above.

IMPORTANT FILTERING RULES (read carefully before generating):
1. ONLY generate rules where the knowledge source explicitly or implicitly describes a data quality
   requirement, business constraint, validation rule, or operational standard that maps to one or
   more columns in the schema above.
2. DO NOT generate rules for content that is irrelevant to data quality — e.g. marketing text,
   general descriptions, process narratives, legal boilerplate, or anything that cannot be expressed
   as a measurable SQL check on the schema.
3. DO NOT invent rules that are not supported by the knowledge source. If the source mentions
   "premiums must be rounded to 2 decimal places", generate that rule. If the source talks about
   company history or HR policies, ignore it entirely.
4. The number of rules should reflect the actual DQ signals in the input — do NOT pad to a fixed
   count. If 3 rules are supported, return 3. If 12 are supported, return 12.
5. Rule types (COMPLETENESS, VALIDITY, UNIQUENESS, CONSISTENCY, TIMELINESS, ACCURACY), target
   tables, and severity should be chosen based on what the knowledge source implies — do NOT force
   an artificial distribution.
6. Severity guidance: CRITICAL = regulatory/financial risk, HIGH = operational impact,
   MEDIUM = data hygiene, LOW = minor quality concern.

SQL CONSTRAINTS (mandatory):
1. Valid SQLite only — no CTEs, no window functions, no LATERAL joins
2. Each query must return exactly 2 columns: failed_records (INT), total_records (INT)
3. failed_records = count of rows VIOLATING the rule
4. total_records = total rows in scope for that rule
5. Use date('now'), julianday(), strftime() for date arithmetic
6. Subqueries are allowed for cross-table checks

Return ONLY a raw JSON array — no markdown fences, no explanation, no text before or after.
If NO rules can be derived from the knowledge source, return an empty array: []

[{{"rule_name":"...","description":"...","target_table":"...","target_column":"...or null","rule_type":"COMPLETENESS|VALIDITY|UNIQUENESS|CONSISTENCY|TIMELINESS|ACCURACY","sql_template":"SELECT COUNT(*) as failed_records,(SELECT COUNT(*) FROM ...) as total_records FROM ... WHERE ...","threshold_pct":95.0,"severity":"LOW|MEDIUM|HIGH|CRITICAL","source_doc":"quote the exact phrase from the knowledge source that justifies this rule"}}]"""

    if not os.environ.get("GROQ_API_KEY", ""):
        msg = "❌ No Groq API key found. Please set the GROQ_API_KEY environment variable and restart the server."
        return {"raw_rules": [], "error": msg, "messages": [msg]}

    try:
        llm = _get_llm()
        resp = llm.invoke(prompt)
        raw  = re.sub(r"```(?:json)?|```", "", resp.content.strip()).strip()
        # Trim trailing text after closing bracket
        end = raw.rfind("]")
        if end != -1: raw = raw[:end+1]
        rules = json.loads(raw)
        return {"raw_rules": rules, "messages": [f"🤖 Llama generated {len(rules)} candidate rules"]}
    except Exception as exc:
        msg = f"❌ Groq error: {str(exc)[:120]}. Please check your GROQ_API_KEY and try again."
        return {"raw_rules": [], "error": msg, "messages": [msg]}


# ═══════════════════════════════════════════════════════════════════════════
# NODE 4 – Validate SQL
# ═══════════════════════════════════════════════════════════════════════════
def node_validate_sql(state: DQState) -> dict:
    if state["source_type"] == "execute_only":
        return {"validated_rules": [], "messages": []}

    conn  = get_conn()
    valid = []
    log   = []

    for rule in state.get("raw_rules", []):
        sql = rule.get("sql_template", "")
        try:
            row = conn.execute(sql).fetchone()
            if row is None or len(row) < 2:
                raise ValueError("query returned < 2 columns")
            int(row[0]); int(row[1])
            valid.append(rule)
        except Exception as exc:
            # Attempt Gemini fix
            fixed = _try_fix_sql(rule, str(exc))
            if fixed:
                try:
                    row = conn.execute(fixed["sql_template"]).fetchone()
                    int(row[0]); int(row[1])
                    valid.append(fixed)
                    log.append(f"  🔧 Fixed: {rule['rule_name']}")
                except:
                    log.append(f"  ❌ Dropped: {rule['rule_name']} ({str(exc)[:60]})")
            else:
                log.append(f"  ❌ Dropped: {rule['rule_name']} ({str(exc)[:60]})")

    conn.close()
    msgs = [f"✅ SQL validation: {len(valid)} valid, {len(state.get('raw_rules',[]))-len(valid)} dropped/fixed"]
    msgs.extend(log)
    return {"validated_rules": valid, "messages": msgs}


def _try_fix_sql(rule: dict, error: str) -> Optional[dict]:
    try:
        llm = _get_llm()
        prompt = f"""Fix this broken SQLite DQ rule SQL.

Schema:
{SCHEMA}

Rule: {rule.get('rule_name','')}
Description: {rule.get('description','')}
Broken SQL: {rule.get('sql_template','')}
Error: {error}

Return ONLY the corrected sql_template as a plain string.
Must return failed_records and total_records columns. No explanation. No quotes."""
        resp = llm.invoke(prompt)
        fixed = resp.content.strip().strip("'\"")
        r = dict(rule); r["sql_template"] = fixed
        return r
    except:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# NODE 5 – Deduplicate vs ChromaDB
# ═══════════════════════════════════════════════════════════════════════════
def node_deduplicate_rules(state: DQState) -> dict:
    if state["source_type"] == "execute_only":
        return {"deduped_rules": [], "messages": []}

    source   = state.get("validated_rules") or state.get("raw_rules", [])
    deduped  = []
    skipped  = 0

    for rule in source:
        fp = (f"{rule['rule_name']}|{rule['target_table']}|"
              f"{rule.get('target_column','')}|{rule['rule_type']}")
        try:
            res = rule_collection.query(query_texts=[fp], n_results=1, include=["distances"])
            if res["distances"] and res["distances"][0] and res["distances"][0][0] < 0.10:
                skipped += 1
                continue
        except Exception:
            pass
        deduped.append(rule)

    return {"deduped_rules": deduped,
            "messages": [f"🔍 Chroma dedup: {len(deduped)} new rules, {skipped} near-duplicates removed"]}


# ═══════════════════════════════════════════════════════════════════════════
# NODE 6 – Sync to SQLite + ChromaDB
# ═══════════════════════════════════════════════════════════════════════════
def node_sync_rules(state: DQState) -> dict:
    if state["source_type"] == "execute_only" or not state.get("deduped_rules"):
        return {"messages": ["⏭️ No new rules to sync"]}

    conn = get_conn()
    c    = conn.cursor()
    c.execute("SELECT COALESCE(MAX(rule_seq),0) FROM dq_rules")
    max_seq = c.fetchone()[0]
    saved   = 0

    for i, rule in enumerate(state["deduped_rules"]):
        seq     = max_seq + i + 1
        rule_id = f"RUL{seq:04d}"
        vec_id  = str(uuid.uuid4())
        try:
            c.execute("""
                INSERT OR IGNORE INTO dq_rules
                (rule_id,rule_seq,rule_name,description,target_table,target_column,
                 rule_type,sql_template,threshold_pct,severity,
                 active_from,active_to,status,source_doc,vector_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rule_id, seq, rule["rule_name"][:100], rule.get("description",""),
                rule["target_table"], rule.get("target_column"),
                rule["rule_type"], rule["sql_template"],
                float(rule.get("threshold_pct",95.0)), rule.get("severity","MEDIUM"),
                date.today().isoformat(), None,
                "PENDING_APPROVAL", rule.get("source_doc","auto"), vec_id,
            ))
            fp = (f"{rule['rule_name']}|{rule['target_table']}|"
                  f"{rule.get('target_column','')}|{rule['rule_type']}")
            rule_collection.add(documents=[fp], ids=[vec_id],
                                metadatas={"rule_id": rule_id})
            saved += 1
        except Exception:
            pass

    conn.commit()
    conn.close()
    return {"messages": [f"💾 Saved {saved} rules → SQLite + ChromaDB (PENDING_APPROVAL)"]}


# ═══════════════════════════════════════════════════════════════════════════
# NODE 7 – Execute Active Rules
# ═══════════════════════════════════════════════════════════════════════════
def node_execute_rules(state: DQState) -> dict:
    conn  = get_conn()
    today = date.today().isoformat()

    rules = [dict(r) for r in conn.execute("""
        SELECT rule_id,rule_name,sql_template,threshold_pct,
               target_table,target_column,rule_type,severity
        FROM dq_rules
        WHERE status='ACTIVE'
          AND active_from<=? AND (active_to IS NULL OR active_to>=?)
        ORDER BY rule_seq
    """, (today, today)).fetchall()]

    run_id  = state["run_id"]
    results = []
    p = f = e = 0

    for rule in rules:
        exec_id = f"EXC-{uuid.uuid4().hex[:10]}"
        try:
            row      = conn.execute(rule["sql_template"]).fetchone()
            fail_cnt = int(row[0]) if row else 0
            tot_cnt  = int(row[1]) if row else 0
            rate     = 100.0 if tot_cnt == 0 else round((1 - fail_cnt/tot_cnt)*100, 2)
            status   = "PASS" if rate >= rule["threshold_pct"] else "FAIL"
            if status == "PASS": p += 1
            else: f += 1

            detail = "All records pass." if fail_cnt == 0 else (
                f"{fail_cnt} record(s) violating rule. "
                f"Sample: {_sample_ids(conn, rule)}"
            )

            conn.execute("""
                INSERT INTO rule_executions
                (execution_id,rule_id,run_id,total_records,failed_records,
                 pass_rate,status,details,sql_used)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (exec_id, rule["rule_id"], run_id, tot_cnt, fail_cnt,
                  rate, status, detail, rule["sql_template"]))

            results.append({**rule,
                "execution_id": exec_id, "total_records": tot_cnt,
                "failed_records": fail_cnt, "pass_rate": rate,
                "threshold_pct": rule["threshold_pct"],
                "status": status, "detail": detail, "sql_used": rule["sql_template"],
            })
        except Exception as ex:
            e += 1
            conn.execute("""
                INSERT INTO rule_executions
                (execution_id,rule_id,run_id,status,error_message,sql_used)
                VALUES (?,?,?,?,?,?)
            """, (exec_id, rule["rule_id"], run_id, "ERROR", str(ex), rule["sql_template"]))
            results.append({**rule, "execution_id": exec_id,
                "status": "ERROR", "error_message": str(ex)})

    conn.execute("""
        INSERT OR IGNORE INTO rule_runs
        (run_id,completed_at,total_rules,passed,failed,errors)
        VALUES (?,?,?,?,?,?)
    """, (run_id, datetime.now().isoformat(), len(rules), p, f, e))
    conn.commit()
    conn.close()

    return {"execution_results": results,
            "messages": [f"⚡ Executed {len(rules)} rules — ✅{p} PASS  ❌{f} FAIL  💥{e} ERROR"]}


def _sample_ids(conn, rule: dict, limit=3) -> str:
    pk_map = {"members":"member_id","policies":"policy_id","claims":"claim_id",
              "providers":"provider_id","premiums":"premium_id","waitlists":"waitlist_id"}
    pk  = pk_map.get(rule["target_table"], "rowid")
    sql = rule["sql_template"]
    m   = re.search(r'\bWHERE\b(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', sql, re.I | re.DOTALL)
    if not m: return "N/A"
    try:
        rows = conn.execute(
            f"SELECT {pk} FROM {rule['target_table']} WHERE {m.group(1).strip()} LIMIT {limit}"
        ).fetchall()
        return ", ".join(str(r[0]) for r in rows) if rows else "none"
    except:
        return "N/A"


# ═══════════════════════════════════════════════════════════════════════════
# NODE 8 – AI Impact Analysis
# ═══════════════════════════════════════════════════════════════════════════
def node_impact_analysis(state: DQState) -> dict:
    results  = state.get("execution_results", [])
    failures = [r for r in results if r.get("status") == "FAIL"]

    if not failures:
        summary = {"assessment": "All rules passed. Data quality is excellent.",
                   "overall_risk": "LOW", "risks": [], "recommendations": [],
                   "regulatory_flags": []}
        return {"impact_summary": summary,
                "messages": ["🌟 All rules passed — data quality is excellent"]}

    fail_txt = "\n".join([
        f"- {r['rule_name']} ({r.get('rule_type','?')}, {r.get('severity','?')}): "
        f"{r.get('failed_records',0)}/{r.get('total_records',0)} failed "
        f"({100-r.get('pass_rate',0):.1f}% failure rate) — {r.get('detail','')}"
        for r in failures
    ])

    prompt = f"""You are a UK Health Insurance DQ Analyst. These rules FAILED in latest run:

{fail_txt}

Return a JSON impact analysis:
{{
  "assessment": "2-3 sentence summary of the data quality situation",
  "overall_risk": "LOW|MEDIUM|HIGH|CRITICAL",
  "risks": [
    {{"area": "short area", "description": "business impact", "affected_rules": ["name1"]}}
  ],
  "recommendations": [
    {{"priority": "IMMEDIATE|SHORT_TERM|LONG_TERM", "action": "specific action", "rules": ["name1"]}}
  ],
  "regulatory_flags": ["FCA/GDPR/NHS compliance concerns"]
}}
Return ONLY valid JSON. No markdown."""

    try:
        llm  = _get_llm()
        resp = llm.invoke(prompt)
        raw  = re.sub(r"```(?:json)?|```", "", resp.content.strip()).strip()
        summary = json.loads(raw)
        conn = get_conn()
        conn.execute("UPDATE rule_runs SET ai_adjustments=? WHERE run_id=?",
                     (json.dumps(summary), state["run_id"]))
        conn.commit()
        conn.close()
        risk = summary.get("overall_risk","?")
        return {"impact_summary": summary,
                "messages": [f"🧠 AI Impact Analysis complete — Overall risk: {risk}"]}
    except Exception as exc:
        summary = {"assessment": f"Analysis unavailable ({exc})",
                   "overall_risk": "UNKNOWN", "risks": [], "recommendations": [],
                   "regulatory_flags": []}
        return {"impact_summary": summary,
                "messages": [f"⚠️ Impact analysis failed: {str(exc)[:80]}"]}





# ═══════════════════════════════════════════════════════════════════════════
# Graph Builders
# ═══════════════════════════════════════════════════════════════════════════
def _blank_state(source_type="text", source_text="", source_folder="") -> DQState:
    return DQState(
        source_type=source_type, source_text=source_text, source_folder=source_folder,
        run_id=f"RUN-{uuid.uuid4().hex[:8]}", extracted_text="",
        raw_rules=[], validated_rules=[], deduped_rules=[],
        messages=[], execution_results=[], feedback_context="", impact_summary={},
        error=None,
    )


def build_generation_graph():
    g = StateGraph(DQState)
    for name, fn in [
        ("load_feedback",  node_load_feedback),
        ("extract",        node_extract_knowledge),
        ("generate",       node_generate_rules),
        ("validate_sql",   node_validate_sql),
        ("deduplicate",    node_deduplicate_rules),
        ("sync",           node_sync_rules),
    ]:
        g.add_node(name, fn)
    g.set_entry_point("load_feedback")
    for a, b in [("load_feedback","extract"),("extract","generate"),
                 ("generate","validate_sql"),("validate_sql","deduplicate"),
                 ("deduplicate","sync"),("sync",END)]:
        g.add_edge(a, b)
    return g.compile()


def build_execution_graph():
    g = StateGraph(DQState)
    for name, fn in [
        ("load_feedback",   node_load_feedback),
        ("execute",         node_execute_rules),
        ("impact_analysis", node_impact_analysis),
    ]:
        g.add_node(name, fn)
    g.set_entry_point("load_feedback")
    for a, b in [("load_feedback","execute"),("execute","impact_analysis"),("impact_analysis",END)]:
        g.add_edge(a, b)
    return g.compile()


def run_generation(source_type="text", source_text="", source_folder=""):
    return build_generation_graph().invoke(_blank_state(source_type, source_text, source_folder))


def run_execution():
    return build_execution_graph().invoke(_blank_state("execute_only"))


if __name__ == "__main__":
    from db_setup import init_db
    init_db()
    s = run_execution()
    for m in s["messages"]: print(m)
