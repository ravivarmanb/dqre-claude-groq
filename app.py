"""
Flask API — DQ Engine (Enhanced)
"""
import os, json, uuid
from datetime import date, datetime
from flask import Flask, request, jsonify, send_from_directory

from db_setup import get_conn, init_db
from dq_engine import run_generation, run_execution

app = Flask(__name__, static_folder="static")

# ════════════════════════════════════════════════════════════
# RULES
# ════════════════════════════════════════════════════════════
@app.route("/api/rules")
def get_rules():
    status_filter = request.args.get("status")
    conn  = get_conn()
    where = f"WHERE r.status='{status_filter}'" if status_filter else ""
    rows  = conn.execute(f"""
        SELECT r.*,
               COUNT(CASE WHEN e.status='PASS'  THEN 1 END) as exec_pass,
               COUNT(CASE WHEN e.status='FAIL'  THEN 1 END) as exec_fail,
               COUNT(CASE WHEN e.status='ERROR' THEN 1 END) as exec_error,
               MAX(e.executed_at)  as last_run,
               MAX(e.pass_rate)    as best_pass_rate,
               MIN(e.pass_rate)    as worst_pass_rate
        FROM dq_rules r
        LEFT JOIN rule_executions e ON e.rule_id=r.rule_id
        {where}
        GROUP BY r.rule_id
        ORDER BY r.rule_seq
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/rules/<rule_id>")
def get_rule(rule_id):
    conn = get_conn()
    r = conn.execute("SELECT * FROM dq_rules WHERE rule_id=?", (rule_id,)).fetchone()
    execs = conn.execute("""
        SELECT * FROM rule_executions WHERE rule_id=? ORDER BY executed_at DESC LIMIT 20
    """, (rule_id,)).fetchall()
    fbs = conn.execute("""
        SELECT * FROM execution_feedback WHERE rule_id=? ORDER BY created_at DESC LIMIT 10
    """, (rule_id,)).fetchall()
    conn.close()
    if not r: return jsonify({"error": "not found"}), 404
    return jsonify({
        "rule": dict(r),
        "executions": [dict(e) for e in execs],
        "feedback": [dict(f) for f in fbs],
    })


@app.route("/api/rules/<rule_id>/approve", methods=["POST"])
def approve_rule(rule_id):
    conn = get_conn()
    conn.execute("""
        UPDATE dq_rules SET status='ACTIVE', approved_by='dashboard_user',
        approved_at=CURRENT_TIMESTAMP WHERE rule_id=?
    """, (rule_id,))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/rules/<rule_id>/reject", methods=["POST"])
def reject_rule(rule_id):
    data = request.json or {}
    conn = get_conn()
    conn.execute("UPDATE dq_rules SET status='REJECTED', rejection_reason=? WHERE rule_id=?",
                 (data.get("reason",""), rule_id))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/rules/<rule_id>/deactivate", methods=["POST"])
def deactivate_rule(rule_id):
    conn = get_conn()
    conn.execute("UPDATE dq_rules SET status='DEACTIVATED', active_to=date('now') WHERE rule_id=?",
                 (rule_id,))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/rules/<rule_id>/reactivate", methods=["POST"])
def reactivate_rule(rule_id):
    conn = get_conn()
    conn.execute("""
        UPDATE dq_rules SET status='ACTIVE', active_to=NULL, active_from=date('now'),
        approved_at=CURRENT_TIMESTAMP WHERE rule_id=?
    """, (rule_id,))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/rules/<rule_id>/update", methods=["POST"])
def update_rule(rule_id):
    data = request.json or {}
    conn = get_conn()
    conn.execute("""
        UPDATE dq_rules SET
            rule_name=COALESCE(?,rule_name),
            description=COALESCE(?,description),
            threshold_pct=COALESCE(?,threshold_pct),
            severity=COALESCE(?,severity),
            active_from=COALESCE(?,active_from),
            active_to=?
        WHERE rule_id=?
    """, (
        data.get("rule_name"), data.get("description"),
        data.get("threshold_pct"), data.get("severity"),
        data.get("active_from"), data.get("active_to"),
        rule_id,
    ))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/rules/bulk", methods=["POST"])
def bulk_action():
    data    = request.json or {}
    action  = data.get("action")
    ids     = data.get("rule_ids", [])
    conn    = get_conn()
    results = {"ok": [], "error": []}
    for rid in ids:
        try:
            if action == "approve":
                conn.execute("UPDATE dq_rules SET status='ACTIVE', approved_by='bulk', approved_at=CURRENT_TIMESTAMP WHERE rule_id=?", (rid,))
            elif action == "reject":
                conn.execute("UPDATE dq_rules SET status='REJECTED', rejection_reason='Bulk rejected' WHERE rule_id=?", (rid,))
            elif action == "deactivate":
                conn.execute("UPDATE dq_rules SET status='DEACTIVATED', active_to=date('now') WHERE rule_id=?", (rid,))
            results["ok"].append(rid)
        except Exception as ex:
            results["error"].append({"id": rid, "error": str(ex)})
    conn.commit(); conn.close()
    return jsonify(results)


# ════════════════════════════════════════════════════════════
# GENERATION
# ════════════════════════════════════════════════════════════
@app.route("/api/generate", methods=["POST"])
def generate_rules():
    data = request.json or {}
    state = run_generation(
        source_type   = data.get("source_type", "text"),
        source_text   = data.get("source_text", ""),
        source_folder = data.get("source_folder", ""),
    )
    if state.get("error"):
        return jsonify({"error": state["error"], "messages": state.get("messages", [])}), 400
    return jsonify({
        "messages":        state.get("messages", []),
        "new_rules_count": len(state.get("deduped_rules", [])),
        "generated":       len(state.get("raw_rules", [])),
        "validated":       len(state.get("validated_rules", [])),
        "deduped":         len(state.get("deduped_rules", [])),
    })


# ════════════════════════════════════════════════════════════
# EXECUTION
# ════════════════════════════════════════════════════════════
@app.route("/api/execute", methods=["POST"])
def execute_rules():
    state = run_execution()
    return jsonify({
        "messages":        state.get("messages", []),
        "results":         state.get("execution_results", []),
        "impact_summary":  state.get("impact_summary", {}),
        "run_id":          state.get("run_id", ""),
    })


@app.route("/api/executions")
def get_executions():
    run_id = request.args.get("run_id")
    limit  = int(request.args.get("limit", 200))
    conn   = get_conn()
    if run_id:
        rows = conn.execute("""
            SELECT e.*,r.rule_name,r.rule_type,r.severity,r.target_table,r.threshold_pct
            FROM rule_executions e JOIN dq_rules r ON r.rule_id=e.rule_id
            WHERE e.run_id=? ORDER BY e.executed_at
        """, (run_id,)).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT e.*,r.rule_name,r.rule_type,r.severity,r.target_table,r.threshold_pct
            FROM rule_executions e JOIN dq_rules r ON r.rule_id=e.rule_id
            ORDER BY e.executed_at DESC LIMIT {limit}
        """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/runs")
def get_runs():
    conn = get_conn()
    rows = conn.execute("""
        SELECT run_id, started_at, completed_at, triggered_by,
               total_rules, passed, failed, errors, ai_adjustments
        FROM rule_runs ORDER BY started_at DESC LIMIT 30
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("ai_adjustments"):
            try: d["ai_adjustments"] = json.loads(d["ai_adjustments"])
            except: pass
        result.append(d)
    return jsonify(result)


@app.route("/api/runs/<run_id>")
def get_run_detail(run_id):
    conn  = get_conn()
    run   = conn.execute("SELECT * FROM rule_runs WHERE run_id=?", (run_id,)).fetchone()
    execs = conn.execute("""
        SELECT e.*,r.rule_name,r.rule_type,r.severity,r.target_table,r.threshold_pct
        FROM rule_executions e JOIN dq_rules r ON r.rule_id=e.rule_id
        WHERE e.run_id=? ORDER BY e.executed_at
    """, (run_id,)).fetchall()
    conn.close()
    if not run: return jsonify({"error":"not found"}), 404
    d = dict(run)
    if d.get("ai_adjustments"):
        try: d["ai_adjustments"] = json.loads(d["ai_adjustments"])
        except: pass
    return jsonify({"run": d, "executions": [dict(e) for e in execs]})


# ════════════════════════════════════════════════════════════
# FEEDBACK
# ════════════════════════════════════════════════════════════
@app.route("/api/feedback", methods=["POST"])
def submit_feedback():
    data = request.json or {}
    fid  = f"FBK-{uuid.uuid4().hex[:8]}"
    conn = get_conn()
    conn.execute("""
        INSERT INTO execution_feedback
        (feedback_id,execution_id,rule_id,feedback_type,comment,suggested_threshold)
        VALUES (?,?,?,?,?,?)
    """, (fid, data["execution_id"], data["rule_id"],
          data["feedback_type"], data.get("comment",""),
          data.get("suggested_threshold")))
    conn.commit(); conn.close()
    return jsonify({"ok": True, "feedback_id": fid})


@app.route("/api/feedback")
def get_feedback():
    conn = get_conn()
    rows = conn.execute("""
        SELECT ef.*, dr.rule_name, dr.target_table
        FROM execution_feedback ef JOIN dq_rules dr ON dr.rule_id=ef.rule_id
        ORDER BY ef.created_at DESC LIMIT 50
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ════════════════════════════════════════════════════════════
# STATS & ANALYTICS
# ════════════════════════════════════════════════════════════
@app.route("/api/stats")
def get_stats():
    conn = get_conn()

    rules_by_status = {r[0]: r[1] for r in conn.execute(
        "SELECT status, COUNT(*) FROM dq_rules GROUP BY status"
    ).fetchall()}
    rules_by_type = {r[0]: r[1] for r in conn.execute(
        "SELECT rule_type, COUNT(*) FROM dq_rules GROUP BY rule_type"
    ).fetchall()}
    rules_by_severity = {r[0]: r[1] for r in conn.execute(
        "SELECT severity, COUNT(*) FROM dq_rules GROUP BY severity"
    ).fetchall()}
    rules_by_table = {r[0]: r[1] for r in conn.execute(
        "SELECT target_table, COUNT(*) FROM dq_rules GROUP BY target_table"
    ).fetchall()}

    # Last run stats
    last_run = conn.execute(
        "SELECT * FROM rule_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()

    recent_pass = conn.execute("""
        SELECT AVG(pass_rate) FROM rule_executions
        WHERE run_id=(SELECT run_id FROM rule_runs ORDER BY started_at DESC LIMIT 1)
          AND status != 'ERROR'
    """).fetchone()[0]

    # Trend: pass rate over last 10 runs
    trend = conn.execute("""
        SELECT rr.run_id, rr.started_at,
               AVG(CASE WHEN e.status!='ERROR' THEN e.pass_rate END) as avg_pass,
               SUM(CASE WHEN e.status='PASS' THEN 1 ELSE 0 END) as passed,
               SUM(CASE WHEN e.status='FAIL' THEN 1 ELSE 0 END) as failed
        FROM rule_runs rr
        JOIN rule_executions e ON e.run_id=rr.run_id
        GROUP BY rr.run_id ORDER BY rr.started_at DESC LIMIT 10
    """).fetchall()

    # Worst performing rules
    worst = conn.execute("""
        SELECT r.rule_name, r.rule_type, r.severity, r.target_table,
               AVG(e.pass_rate) as avg_pass, COUNT(*) as run_count
        FROM rule_executions e JOIN dq_rules r ON r.rule_id=e.rule_id
        WHERE e.status IN ('PASS','FAIL')
        GROUP BY e.rule_id
        ORDER BY avg_pass ASC LIMIT 5
    """).fetchall()

    total_feedback = conn.execute("SELECT COUNT(*) FROM execution_feedback").fetchone()[0]

    conn.close()
    return jsonify({
        "rules_by_status":   rules_by_status,
        "rules_by_type":     rules_by_type,
        "rules_by_severity": rules_by_severity,
        "rules_by_table":    rules_by_table,
        "recent_pass_rate":  round(recent_pass or 0, 1),
        "total_executions":  conn.execute("SELECT COUNT(*) FROM rule_executions").fetchone()[0] if False else _count("rule_executions"),
        "total_feedback":    total_feedback,
        "last_run":          dict(last_run) if last_run else None,
        "trend":             [dict(r) for r in trend],
        "worst_rules":       [dict(r) for r in worst],
    })


def _count(table):
    conn = get_conn()
    n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.close()
    return n


@app.route("/api/drilldown/<table>")
def drilldown(table):
    """Return actual failing rows for a given table."""
    allowed = {"members","policies","claims","providers","premiums","waitlists"}
    if table not in allowed:
        return jsonify({"error":"unknown table"}), 400
    conn = get_conn()
    rows = conn.execute(f"SELECT * FROM {table} LIMIT 100").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5050)
