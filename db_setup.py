"""
DB Setup: Synthetic UK Health Insurance SQLite database + DQ Rules tables
"""
import sqlite3
import os
from datetime import date, datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "health_insurance.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    # ─── UK Health Insurance Domain Tables ─────────────────────────────────────

    c.executescript("""
    CREATE TABLE IF NOT EXISTS members (
        member_id       TEXT PRIMARY KEY,
        nhs_number      TEXT UNIQUE,
        first_name      TEXT NOT NULL,
        last_name       TEXT NOT NULL,
        date_of_birth   DATE NOT NULL,
        gender          TEXT CHECK(gender IN ('M','F','Other')),
        postcode        TEXT,
        email           TEXT,
        phone           TEXT,
        registration_date DATE NOT NULL,
        status          TEXT DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','LAPSED','CANCELLED'))
    );

    CREATE TABLE IF NOT EXISTS policies (
        policy_id       TEXT PRIMARY KEY,
        member_id       TEXT NOT NULL REFERENCES members(member_id),
        policy_type     TEXT CHECK(policy_type IN ('BASIC','STANDARD','COMPREHENSIVE','ELITE')),
        start_date      DATE NOT NULL,
        end_date        DATE,
        premium_monthly REAL NOT NULL,
        excess_amount   REAL DEFAULT 100.0,
        status          TEXT DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','LAPSED','CANCELLED','PENDING')),
        insurer_code    TEXT,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS claims (
        claim_id        TEXT PRIMARY KEY,
        policy_id       TEXT NOT NULL REFERENCES policies(policy_id),
        member_id       TEXT NOT NULL REFERENCES members(member_id),
        claim_date      DATE NOT NULL,
        treatment_date  DATE NOT NULL,
        diagnosis_code  TEXT,
        treatment_type  TEXT,
        provider_id     TEXT,
        claimed_amount  REAL NOT NULL,
        approved_amount REAL,
        status          TEXT DEFAULT 'PENDING' CHECK(status IN ('PENDING','APPROVED','REJECTED','INVESTIGATING')),
        rejection_reason TEXT,
        processed_date  DATE
    );

    CREATE TABLE IF NOT EXISTS providers (
        provider_id     TEXT PRIMARY KEY,
        provider_name   TEXT NOT NULL,
        provider_type   TEXT CHECK(provider_type IN ('GP','HOSPITAL','CLINIC','SPECIALIST','PHARMACY')),
        postcode        TEXT,
        nhs_registered  INTEGER DEFAULT 1,
        active          INTEGER DEFAULT 1,
        contract_start  DATE
    );

    CREATE TABLE IF NOT EXISTS premiums (
        premium_id      TEXT PRIMARY KEY,
        policy_id       TEXT NOT NULL REFERENCES policies(policy_id),
        due_date        DATE NOT NULL,
        amount          REAL NOT NULL,
        paid_date       DATE,
        payment_method  TEXT CHECK(payment_method IN ('DD','CARD','BACS','CHEQUE')),
        status          TEXT DEFAULT 'PENDING' CHECK(status IN ('PENDING','PAID','OVERDUE','WAIVED'))
    );

    CREATE TABLE IF NOT EXISTS waitlists (
        waitlist_id     TEXT PRIMARY KEY,
        member_id       TEXT NOT NULL REFERENCES members(member_id),
        treatment_type  TEXT NOT NULL,
        referral_date   DATE NOT NULL,
        target_date     DATE,
        actual_date     DATE,
        status          TEXT DEFAULT 'WAITING' CHECK(status IN ('WAITING','COMPLETED','CANCELLED'))
    );
    """)

    # ─── DQ Rules Tables ───────────────────────────────────────────────────────

    c.executescript("""
    CREATE TABLE IF NOT EXISTS dq_rules (
        rule_id         TEXT PRIMARY KEY,
        rule_seq        INTEGER UNIQUE NOT NULL,
        rule_name       TEXT NOT NULL,
        description     TEXT,
        target_table    TEXT NOT NULL,
        target_column   TEXT,
        rule_type       TEXT CHECK(rule_type IN ('COMPLETENESS','VALIDITY','UNIQUENESS','CONSISTENCY','TIMELINESS','ACCURACY')),
        sql_template    TEXT NOT NULL,
        threshold_pct   REAL DEFAULT 95.0,
        severity        TEXT DEFAULT 'MEDIUM' CHECK(severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
        active_from     DATE NOT NULL,
        active_to       DATE,
        status          TEXT DEFAULT 'PENDING_APPROVAL' CHECK(status IN ('PENDING_APPROVAL','ACTIVE','REJECTED','DEACTIVATED')),
        source_doc      TEXT,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        approved_by     TEXT,
        approved_at     TIMESTAMP,
        rejection_reason TEXT,
        vector_id       TEXT
    );

    CREATE TABLE IF NOT EXISTS rule_executions (
        execution_id    TEXT PRIMARY KEY,
        rule_id         TEXT NOT NULL REFERENCES dq_rules(rule_id),
        run_id          TEXT NOT NULL,
        executed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_records   INTEGER,
        failed_records  INTEGER,
        pass_rate       REAL,
        status          TEXT CHECK(status IN ('PASS','FAIL','ERROR','WARNING')),
        details         TEXT,
        sql_used        TEXT,
        error_message   TEXT
    );

    CREATE TABLE IF NOT EXISTS execution_feedback (
        feedback_id     TEXT PRIMARY KEY,
        execution_id    TEXT NOT NULL REFERENCES rule_executions(execution_id),
        rule_id         TEXT NOT NULL,
        feedback_type   TEXT CHECK(feedback_type IN ('CORRECT','FALSE_POSITIVE','FALSE_NEGATIVE','RULE_TOO_STRICT','RULE_TOO_LAX','OTHER')),
        comment         TEXT,
        suggested_threshold REAL,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS rule_runs (
        run_id          TEXT PRIMARY KEY,
        started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at    TIMESTAMP,
        triggered_by    TEXT DEFAULT 'MANUAL',
        total_rules     INTEGER,
        passed          INTEGER,
        failed          INTEGER,
        errors          INTEGER,
        ai_adjustments  TEXT
    );
    """)

    # ─── Seed Data ─────────────────────────────────────────────────────────────
    _seed_data(c)
    conn.commit()
    conn.close()
    print(f"✅ Database initialised at {DB_PATH}")


def _seed_data(c):
    # Check if already seeded
    c.execute("SELECT COUNT(*) FROM members")
    if c.fetchone()[0] > 0:
        return

    # Providers
    providers = [
        ("PRV001","St Thomas' Hospital","HOSPITAL","SE1 7EH",1,1,"2010-01-01"),
        ("PRV002","King's College Hospital","HOSPITAL","SE5 9RS",1,1,"2010-01-01"),
        ("PRV003","Harley Street Clinic","CLINIC","W1G 7HJ",1,1,"2015-03-01"),
        ("PRV004","Boots Pharmacy","PHARMACY","EC1A 1BB",1,1,"2012-06-01"),
        ("PRV005","Dr. Ahmed GP Surgery","GP","N1 9AG",1,1,"2008-01-01"),
        ("PRV006","Bupa Cromwell","HOSPITAL","SW5 0TU",1,1,"2011-01-01"),
        ("PRV007","Unregistered Clinic","CLINIC","XX1 1XX",0,1,"2023-01-01"),  # intentionally bad
    ]
    c.executemany("INSERT OR IGNORE INTO providers VALUES (?,?,?,?,?,?,?)", providers)

    # Members — includes intentional DQ issues
    members = [
        ("MEM001","9434765872","Alice","Thompson","1985-03-12","F","SW1A 1AA","alice.t@email.co.uk","07700900001","2020-01-15","ACTIVE"),
        ("MEM002","9434765873","Bob","Williams","1972-07-22","M","EC1A 1BB","bob.w@email.co.uk","07700900002","2019-06-01","ACTIVE"),
        ("MEM003","9434765874","Carol","Davies","1990-11-05","F","M1 1AE","carol.d@email.co.uk","07700900003","2021-03-20","ACTIVE"),
        ("MEM004","9434765875","David","Jones","1965-09-14","M","LS1 1BA","david.j@email.co.uk","07700900004","2018-01-10","ACTIVE"),
        ("MEM005",None,"Emma","Wilson","1998-02-28","F","B1 1BB",None,"07700900005","2022-05-01","ACTIVE"),  # missing NHS number
        ("MEM006","9434765876","Frank","Brown","1955-12-01","M","G1 1AA","frank.b@email.co.uk",None,"2017-09-15","LAPSED"),
        ("MEM007","9434765877","Grace","Taylor","2050-01-01","F","BS1 1AA","grace.t@email.co.uk","07700900007","2023-01-01","ACTIVE"),  # future DOB
        ("MEM008","9434765878","Henry","Martin","1980-04-18","M","CF10 1AA","henry.m@email.co.uk","07700900008","2020-08-12","ACTIVE"),
    ]
    c.executemany("INSERT OR IGNORE INTO members VALUES (?,?,?,?,?,?,?,?,?,?,?)", members)

    # Policies
    policies = [
        ("POL001","MEM001","COMPREHENSIVE","2020-01-15",None,89.50,250.0,"ACTIVE","AXA","2020-01-15 10:00:00"),
        ("POL002","MEM002","STANDARD","2019-06-01",None,54.00,100.0,"ACTIVE","BUPA","2019-06-01 09:00:00"),
        ("POL003","MEM003","BASIC","2021-03-20",None,32.00,200.0,"ACTIVE","VITALITY","2021-03-20 11:00:00"),
        ("POL004","MEM004","ELITE","2018-01-10","2024-01-10",120.00,500.0,"LAPSED","AXA","2018-01-10 08:00:00"),
        ("POL005","MEM005","BASIC","2022-05-01",None,-10.00,100.0,"ACTIVE","BUPA","2022-05-01 12:00:00"),  # negative premium
        ("POL006","MEM006","STANDARD","2017-09-15","2023-09-15",54.00,100.0,"CANCELLED","VITALITY","2017-09-15 09:00:00"),
        ("POL007","MEM007","COMPREHENSIVE","2023-01-01",None,89.50,250.0,"ACTIVE","AXA","2023-01-01 10:00:00"),
        ("POL008","MEM008","STANDARD","2020-08-12",None,54.00,100.0,"ACTIVE","BUPA","2020-08-12 09:00:00"),
        ("POL009","MEM001","BASIC","2024-01-01",None,32.00,None,"ACTIVE","VITALITY","2024-01-01 10:00:00"),  # missing excess
    ]
    c.executemany("INSERT OR IGNORE INTO policies VALUES (?,?,?,?,?,?,?,?,?,?)", policies)

    # Claims
    claims = [
        ("CLM001","POL001","MEM001","2023-06-01","2023-05-28","J18.0","Chest X-Ray","PRV001",450.00,400.00,"APPROVED",None,"2023-06-10"),
        ("CLM002","POL002","MEM002","2023-07-15","2023-07-10","M54.5","Physiotherapy","PRV003",320.00,320.00,"APPROVED",None,"2023-07-20"),
        ("CLM003","POL003","MEM003","2023-08-01","2023-07-30","K29.0","Gastroscopy","PRV002",1200.00,None,"PENDING",None,None),
        ("CLM004","POL001","MEM001","2023-09-12","2023-09-10","Z00.0","Annual Check","PRV005",150.00,0.00,"REJECTED","Below excess","2023-09-18"),
        ("CLM005","POL007","MEM007","2024-01-15","2023-12-20","I10","Consultation","PRV007",500.00,None,"PENDING",None,None),  # unregistered provider
        ("CLM006","POL002","MEM002","2022-01-01","2022-01-15","M54.5","Physio","PRV003",300.00,None,"PENDING",None,None),  # claim before treatment
        ("CLM007","POL008","MEM008","2023-11-01","2023-10-28","E11.0","Diabetes Review","PRV005",180.00,180.00,"APPROVED",None,"2023-11-08"),
        ("CLM008","POL004","MEM004","2025-01-01","2025-01-05","J45.0","Asthma Review","PRV001",200.00,None,"PENDING",None,None),  # claim on lapsed policy
    ]
    c.executemany("INSERT OR IGNORE INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", claims)

    # Premiums
    premiums = [
        ("PRM001","POL001","2024-01-01",89.50,"2024-01-03","DD","PAID"),
        ("PRM002","POL001","2024-02-01",89.50,"2024-02-02","DD","PAID"),
        ("PRM003","POL002","2024-01-01",54.00,"2024-01-05","DD","PAID"),
        ("PRM004","POL002","2024-02-01",54.00,None,None,"OVERDUE"),
        ("PRM005","POL003","2024-01-01",32.00,"2024-01-10","CARD","PAID"),
        ("PRM006","POL005","2024-01-01",-10.00,None,None,"PENDING"),  # negative premium
        ("PRM007","POL008","2024-01-01",54.00,"2024-01-07","DD","PAID"),
    ]
    c.executemany("INSERT OR IGNORE INTO premiums VALUES (?,?,?,?,?,?,?)", premiums)

    # Waitlists
    waitlists = [
        ("WL001","MEM001","Hip Replacement","2023-01-01","2023-07-01","2023-08-15","COMPLETED"),
        ("WL002","MEM002","Cataract Surgery","2023-03-01","2023-09-01",None,"WAITING"),
        ("WL003","MEM003","Knee Arthroscopy","2023-06-01","2023-12-01",None,"WAITING"),
        ("WL004","MEM004","Cardiac Angiogram","2022-01-01","2022-07-01","2022-06-20","COMPLETED"),
    ]
    c.executemany("INSERT OR IGNORE INTO waitlists VALUES (?,?,?,?,?,?,?)", waitlists)

    print("✅ Seed data inserted")


if __name__ == "__main__":
    init_db()
