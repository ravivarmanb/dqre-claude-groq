from db_setup import get_conn

conn = get_conn()

# Test approve query
try:
    conn.execute("""
        UPDATE dq_rules SET status='ACTIVE', approved_by='test',
        approved_at=CURRENT_TIMESTAMP WHERE rule_id='RUL0001'
    """)
    conn.commit()
    print("Approve query: OK")
except Exception as e:
    print("Approve error:", e)

# Show all rules
try:
    row = conn.execute("SELECT COALESCE(MAX(rule_seq),0) FROM dq_rules").fetchone()
    print("Max rule_seq:", row[0])
    rules = conn.execute("SELECT rule_id, rule_seq, rule_name, status FROM dq_rules").fetchall()
    for r in rules:
        print(dict(r))
except Exception as e:
    print("Query error:", e)

# Test a sync INSERT to see the real error
try:
    import uuid
    from datetime import date
    test_rule = {
        "rule_name": "Test Rule",
        "description": "Test",
        "target_table": "members",
        "target_column": None,
        "rule_type": "COMPLETENESS",
        "sql_template": "SELECT COUNT(*) as failed_records, (SELECT COUNT(*) FROM members) as total_records FROM members WHERE first_name IS NULL",
        "threshold_pct": 95.0,
        "severity": "MEDIUM",
        "source_doc": "test"
    }
    vec_id = str(uuid.uuid4())
    conn.execute("""
        INSERT OR IGNORE INTO dq_rules
        (rule_id,rule_seq,rule_name,description,target_table,target_column,
         rule_type,sql_template,threshold_pct,severity,
         active_from,active_to,status,source_doc,vector_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        "RUL9999", 9999, test_rule["rule_name"][:100], test_rule.get("description",""),
        test_rule["target_table"], test_rule.get("target_column"),
        test_rule["rule_type"], test_rule["sql_template"],
        float(test_rule.get("threshold_pct",95.0)), test_rule.get("severity","MEDIUM"),
        date.today().isoformat(), None,
        "PENDING_APPROVAL", test_rule.get("source_doc","auto"), vec_id,
    ))
    conn.commit()
    print("Test INSERT: OK - rule saved!")
    # Cleanup
    conn.execute("DELETE FROM dq_rules WHERE rule_id='RUL9999'")
    conn.commit()
except Exception as e:
    print("INSERT error:", e)

conn.close()
