from flask import Flask, render_template, request, redirect, url_for, session, send_file, jsonify, abort
import sqlite3, os, io, pandas as pd
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.permanent_session_lifetime = timedelta(days=7)

DB = "finance.db"

# =========================
# DB
# =========================
def connect():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = connect()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY,
        password TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS financials(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        industry TEXT,
        year INTEGER,

        sales REAL,
        gross_profit REAL,
        net_income REAL,

        total_assets REAL,
        equity REAL,
        current_assets REAL,
        current_liabilities REAL,
        liabilities REAL,

        employees INTEGER,

        gross_profit_margin REAL,
        roe REAL,
        current_ratio REAL,
        debt_ratio REAL,
        sales_per_employee REAL,
        productivity REAL,

        user_id TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        financial_id INTEGER,
        user_id TEXT,
        content TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    con.commit()
    con.close()

# =========================
# Helpers
# =========================
MONEY_FIELDS = [
    "sales", "gross_profit", "net_income",
    "total_assets", "equity",
    "current_assets", "current_liabilities",
    "liabilities"
]

def _to_float(v) -> float:
    if v is None:
        return 0.0
    s = str(v).strip().replace(",", "").replace("，", "").replace(" ", "")
    if s == "":
        return 0.0
    return float(s)

def _to_int(v) -> int:
    if v is None:
        return 0
    s = str(v).strip().replace(",", "").replace("，", "").replace(" ", "")
    if s == "":
        return 0
    return int(float(s))  # "12.0" 対策

def parse_financial_form_with_unit(form) -> dict:
    """
    入力単位(unit)を反映して、DB保存用「円」に統一したdictを返す
    unit: 1(円) / 1000(千円) / 1000000(百万円)
    """
    unit = _to_float(form.get("unit", "1"))
    if unit not in (1.0, 1000.0, 1000000.0):
        unit = 1.0

    d = {}
    for k in MONEY_FIELDS:
        d[k] = _to_float(form.get(k)) * unit  # ← unit反映（円換算）
    d["employees"] = _to_int(form.get("employees"))
    return d

# =========================
# Indicator calc
# =========================
def calc(d):
    return {
        "gross_profit_margin": d["gross_profit"]/d["sales"] if d["sales"] else 0,
        "roe": d["net_income"]/d["equity"] if d["equity"] else 0,
        "current_ratio": d["current_assets"]/d["current_liabilities"] if d["current_liabilities"] else 0,
        "debt_ratio": d["liabilities"]/d["total_assets"] if d["total_assets"] else 0,
        "sales_per_employee": d["sales"]/d["employees"] if d["employees"] else 0,
        "productivity": d["net_income"]/d["employees"] if d["employees"] else 0,
    }

# =========================
# Auth
# =========================
@app.route("/register", methods=["GET","POST"])
def register():
    if request.method == "POST":
        u, p = request.form["username"], request.form["password"]
        con = connect()
        cur = con.cursor()
        cur.execute("SELECT 1 FROM users WHERE username=?", (u,))
        if cur.fetchone():
            con.close()
            return "既に存在します"
        cur.execute("INSERT INTO users VALUES(?,?)", (u, generate_password_hash(p)))
        con.commit()
        con.close()
        return redirect(url_for("login"))
    return render_template("register.html")

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        u, p = request.form["username"], request.form["password"]
        con = connect()
        cur = con.cursor()
        cur.execute("SELECT * FROM users WHERE username=?", (u,))
        user = cur.fetchone()
        con.close()
        if user and check_password_hash(user["password"], p):
            session["user_id"] = u
            return redirect(url_for("index"))
        return "ログイン失敗"
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# =========================
# Main input
# =========================
@app.route("/", methods=["GET","POST"])
def index():
    if "user_id" not in session:
        return redirect(url_for("login"))

    current_year = datetime.now().year

    if request.method == "POST":
        f = request.form

        # 1) unitを反映して円換算
        d = parse_financial_form_with_unit(f)

        # 2) 文字項目
        company_name = f.get("company_name", "").strip()
        industry = f.get("industry", "").strip()
        year = _to_int(f.get("year"))

        # 3) 指標計算
        d.update(calc(d))

        con = connect()
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO financials(
                company_name, industry, year,
                sales, gross_profit, net_income,
                total_assets, equity, current_assets, current_liabilities, liabilities,
                employees,
                gross_profit_margin, roe, current_ratio, debt_ratio, sales_per_employee, productivity,
                user_id
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                company_name, industry, year,
                d["sales"], d["gross_profit"], d["net_income"],
                d["total_assets"], d["equity"], d["current_assets"], d["current_liabilities"], d["liabilities"],
                d["employees"],
                d["gross_profit_margin"], d["roe"], d["current_ratio"], d["debt_ratio"],
                d["sales_per_employee"], d["productivity"],
                session["user_id"],
            )
        )
        con.commit()
        con.close()
        return redirect(url_for("view_data"))

    return render_template("index.html", current_year=current_year)

# =========================
# Industry list (JSON)
# =========================
@app.route("/industry_list")
def industry_list():
    if "user_id" not in session:
        return jsonify([])

    con = connect()
    cur = con.cursor()
    cur.execute(
        "SELECT DISTINCT industry FROM financials WHERE user_id=? AND industry IS NOT NULL AND industry != ''",
        (session["user_id"],)
    )
    data = [r[0] for r in cur.fetchall()]
    con.close()
    return jsonify(data)

# =========================
# View
# =========================
@app.route("/view_data")
def view_data():
    if "user_id" not in session:
        return redirect(url_for("login"))

    company_name = request.args.get("company_name", "").strip()
    industry = request.args.get("industry", "").strip()

    con = connect()
    cur = con.cursor()

    # 動的WHERE条件
    where = ["user_id = ?"]
    params = [session["user_id"]]

    if company_name:
        where.append("company_name LIKE ?")
        params.append(f"%{company_name}%")

    if industry:
        where.append("industry LIKE ?")
        params.append(f"%{industry}%")

    sql = f"""
        SELECT * FROM financials
        WHERE {' AND '.join(where)}
        ORDER BY company_name, year
    """

    cur.execute(sql, params)
    financials = cur.fetchall()

    # コメント取得
    cur.execute("SELECT * FROM comments WHERE user_id=?", (session["user_id"],))
    com = cur.fetchall()
    con.close()

    comments = {}
    for c in com:
        comments.setdefault(c["financial_id"], []).append(c)

    return render_template(
        "view_data.html",
        financial_data=financials,
        comments_by_id=comments,
        company_name=company_name,
        industry=industry
    )

# =========================
# Edit
# =========================
@app.route("/edit/<int:id>", methods=["GET","POST"])
def edit_data(id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    con = connect()
    cur = con.cursor()

    # 本人のデータか確認
    cur.execute("SELECT * FROM financials WHERE id=? AND user_id=?", (id, session["user_id"]))
    data = cur.fetchone()
    if data is None:
        con.close()
        abort(404)

    if request.method == "POST":
        f = request.form

        d = {k: _to_float(f.get(k)) for k in MONEY_FIELDS}
        d["employees"] = _to_int(f.get("employees"))

        year = _to_int(f.get("year"))
        company_name = f.get("company_name", "").strip()
        industry = f.get("industry", "").strip()

        d.update(calc(d))

        cur.execute(
            """
            UPDATE financials SET
                company_name=?, industry=?, year=?,
                sales=?, gross_profit=?, net_income=?,
                total_assets=?, equity=?, current_assets=?, current_liabilities=?, liabilities=?,
                employees=?,
                gross_profit_margin=?, roe=?, current_ratio=?, debt_ratio=?, sales_per_employee=?, productivity=?
            WHERE id=? AND user_id=?
            """,
            (
                company_name, industry, year,
                d["sales"], d["gross_profit"], d["net_income"],
                d["total_assets"], d["equity"], d["current_assets"], d["current_liabilities"], d["liabilities"],
                d["employees"],
                d["gross_profit_margin"], d["roe"], d["current_ratio"], d["debt_ratio"],
                d["sales_per_employee"], d["productivity"],
                id, session["user_id"],
            )
        )

        con.commit()
        con.close()
        return redirect(url_for("edit_data", id=id))

    cur.execute(
        "SELECT * FROM comments WHERE financial_id=? AND user_id=? ORDER BY id DESC",
        (id, session["user_id"])
    )
    comments = cur.fetchall()
    con.close()

    return render_template("edit_data.html", data=data, comments=comments)

# =========================
# Comment
# =========================
@app.route("/add_comment/<int:id>", methods=["POST"])
def add_comment(id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    con = connect()
    cur = con.cursor()

    cur.execute("SELECT id FROM financials WHERE id=? AND user_id=?", (id, session["user_id"]))
    if cur.fetchone() is None:
        con.close()
        abort(404)

    content = request.form.get("content", "").strip()
    if content == "":
        con.close()
        return redirect(url_for("edit_data", id=id))

    cur.execute(
        "INSERT INTO comments(financial_id, user_id, content) VALUES(?,?,?)",
        (id, session["user_id"], content)
    )
    con.commit()
    con.close()
    return redirect(url_for("edit_data", id=id))

@app.route("/edit_comment/<int:id>", methods=["POST"])
def edit_comment(id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    con = connect()
    cur = con.cursor()

    cur.execute("SELECT financial_id FROM comments WHERE id=? AND user_id=?", (id, session["user_id"]))
    row = cur.fetchone()
    if row is None:
        con.close()
        abort(404)

    fid = row["financial_id"]

    content = request.form.get("content", "").strip()
    if content == "":
        con.close()
        return redirect(url_for("edit_data", id=fid))

    cur.execute("UPDATE comments SET content=? WHERE id=? AND user_id=?", (content, id, session["user_id"]))
    con.commit()
    con.close()
    return redirect(url_for("edit_data", id=fid))

@app.route("/delete_comment/<int:id>", methods=["POST"])
def delete_comment(id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    con = connect()
    cur = con.cursor()

    cur.execute("SELECT financial_id FROM comments WHERE id=? AND user_id=?", (id, session["user_id"]))
    row = cur.fetchone()
    if row is None:
        con.close()
        abort(404)

    fid = row["financial_id"]

    cur.execute("DELETE FROM comments WHERE id=? AND user_id=?", (id, session["user_id"]))
    con.commit()
    con.close()
    return redirect(url_for("edit_data", id=fid))

# =========================
# Excel
# =========================
@app.route("/download_excel")
def download_excel():
    if "user_id" not in session:
        return redirect(url_for("login"))

    con = connect()
    cur = con.cursor()

    cur.execute("""
        SELECT f.*,
               GROUP_CONCAT(comments.content, ' / ') AS comments
        FROM financials AS f
        LEFT JOIN comments
               ON f.id = comments.financial_id
              AND comments.user_id = ?
        WHERE f.user_id = ?
        GROUP BY f.id
        ORDER BY f.company_name, f.year
    """, (session["user_id"], session["user_id"]))

    rows = cur.fetchall()
    con.close()

    df = pd.DataFrame([dict(r) for r in rows])
    df = df.drop(columns=["id", "user_id"], errors="ignore")

    out = io.BytesIO()
    df.to_excel(out, index=False)
    out.seek(0)

    return send_file(out, download_name="financial_data.xlsx", as_attachment=True)

# =========================
# Graph
# =========================
@app.route("/graph_view")
def graph_view():
    if "user_id" not in session:
        return redirect(url_for("login"))

    con = connect()
    cur = con.cursor()

    cur.execute("""
        SELECT company_name, year, sales, roe, productivity
        FROM financials
        WHERE user_id=?
        ORDER BY company_name, year
    """, (session["user_id"],))

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return render_template("graph.html", rows=rows)

@app.route("/get_companies")
def get_companies():
    if "user_id" not in session:
        return jsonify([])

    q = request.args.get("query", "").strip()
    con = connect()
    cur = con.cursor()
    cur.execute("""
        SELECT DISTINCT company_name
        FROM financials
        WHERE user_id=? AND company_name LIKE ?
        ORDER BY company_name
        LIMIT 10
    """, (session["user_id"], f"%{q}%"))
    rows = [r[0] for r in cur.fetchall()]
    con.close()
    return jsonify(rows)


# =========================
# Run
# =========================
if __name__ == "__main__":
    init_db()
    app.run(debug=True)
