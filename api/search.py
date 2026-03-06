from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

BASE_DIR = Path(__file__).resolve().parent.parent
ZIP_DB = BASE_DIR / "zipcode.db"
JIG_DB = BASE_DIR / "jigyosyo.db"

DEFAULT_LIMIT = 10
MAX_LIMIT = 50


def _normalize_postal(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if status != 204:
            self.wfile.write(body)

    def do_OPTIONS(self):
        self._send_json(204, {})

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query or "")

        q = (qs.get("q", [""])[0] or "").strip()

        limit_raw = (qs.get("limit", [str(DEFAULT_LIMIT)])[0] or str(DEFAULT_LIMIT)).strip()
        try:
            limit = max(1, min(MAX_LIMIT, int(limit_raw)))
        except Exception:
            limit = DEFAULT_LIMIT

        if not q:
            self._send_json(200, [])
            return

        results = []

        # まず zipcode.db
        try:
            conn = sqlite3.connect(str(ZIP_DB))
            cur = conn.cursor()

            if q.isdigit():
                kw = f"%{_normalize_postal(q)}%"
                cur.execute(
                    """
                    SELECT zipcode, pref, city, town
                    FROM zipcode
                    WHERE zipcode LIKE ?
                    LIMIT ?
                    """,
                    (kw, limit),
                )
            else:
                kw = f"%{q}%"
                cur.execute(
                    """
                    SELECT zipcode, pref, city, town
                    FROM zipcode
                    WHERE town LIKE ? OR city LIKE ? OR pref LIKE ?
                    LIMIT ?
                    """,
                    (kw, kw, kw, limit),
                )

            for r in cur.fetchall():
                results.append({
                    "zipcode": r[0],
                    "address": f"{r[1]}{r[2]}{r[3]}",
                    "source": "zipcode"
                })

            conn.close()
        except Exception:
            pass

        # zipcodeで0件のときだけ jigyosyo.db
        if not results:
            try:
                conn = sqlite3.connect(str(JIG_DB))
                cur = conn.cursor()

                if q.isdigit():
                    kw = f"%{_normalize_postal(q)}%"
                    cur.execute(
                        """
                        SELECT zipcode, name, address
                        FROM jigyosyo
                        WHERE zipcode LIKE ?
                        LIMIT ?
                        """,
                        (kw, limit),
                    )
                else:
                    kw = f"%{q}%"
                    cur.execute(
                        """
                        SELECT zipcode, name, address
                        FROM jigyosyo
                        WHERE name LIKE ? OR address LIKE ?
                        LIMIT ?
                        """,
                        (kw, kw, limit),
                    )

                for r in cur.fetchall():
                    results.append({
                        "zipcode": r[0],
                        "address": f"{r[1]} {r[2]}",
                        "source": "jigyosyo"
                    })

                conn.close()
            except Exception:
                pass

        self._send_json(200, results)
