import json
import sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler

BASE_DIR = Path(__file__).resolve().parent.parent
ZIP_DB = BASE_DIR / "zipcode.db"
JIG_DB = BASE_DIR / "jigyosyo.db"

class handler(BaseHTTPRequestHandler):
    def _send_json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        out = {
            "zip_db_exists": ZIP_DB.exists(),
            "jig_db_exists": JIG_DB.exists(),
            "zip_db_path": str(ZIP_DB),
            "jig_db_path": str(JIG_DB),
            "zip_tables": [],
            "jig_tables": [],
            "zip_error": "",
            "jig_error": ""
        }

        try:
            conn = sqlite3.connect(str(ZIP_DB))
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name")
            out["zip_tables"] = [r[0] for r in cur.fetchall()]
            conn.close()
        except Exception as e:
            out["zip_error"] = str(e)

        try:
            conn = sqlite3.connect(str(JIG_DB))
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name")
            out["jig_tables"] = [r[0] for r in cur.fetchall()]
            conn.close()
        except Exception as e:
            out["jig_error"] = str(e)

        self._send_json(200, out)
