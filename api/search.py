import json
import sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

BASE_DIR = Path(__file__).resolve().parent.parent
ZIP_DB = BASE_DIR / "zipcode.db"
JIG_DB = BASE_DIR / "jigyosyo.db"


def _normalize_keyword(s: str) -> str:
    return (s or "").strip()


def _connect_zip_and_attach_jigyosyo():
    """
    zipcode.db に接続し、存在すれば jigyosyo.db を ATTACH する
    """
    con = sqlite3.connect(str(ZIP_DB))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    if JIG_DB.exists():
        try:
            cur.execute("ATTACH DATABASE ? AS jig", (str(JIG_DB),))
        except Exception:
            pass

    return con, cur


def query(keyword: str, limit: int = 300):
    kw = _normalize_keyword(keyword)
    if not kw:
        return []

    con, cur = _connect_zip_and_attach_jigyosyo()

    jig_ok = False
    if JIG_DB.exists():
        try:
            cur.execute("SELECT name FROM jig.sqlite_master WHERE type='table' AND name='jigyosyo_zip'")
            jig_ok = cur.fetchone() is not None
        except Exception:
            jig_ok = False

    if kw.isdigit():
        sql_main = """
            SELECT postal, pref, city, town,
                   (pref_kana||' '||city_kana||' '||town_kana) AS kana,
                   (pref_en||' '||city_en||' '||town_en) AS rome
            FROM vw_zip
            WHERE postal LIKE ? || '%'
        """

        if jig_ok:
            sql_jig = """
                SELECT postal, pref, city,
                       (town || ' ' || IFNULL(addr,'') || '（' || IFNULL(office_name,'') || '）') AS town,
                       (IFNULL(office_kana,'')) AS kana,
                       '' AS rome
                FROM jig.jigyosyo_zip
                WHERE postal LIKE ? || '%'
            """
            sql = f"""
                SELECT * FROM (
                    {sql_main}
                    UNION ALL
                    {sql_jig}
                )
                LIMIT ?;
            """
            params = [kw, kw, limit]
        else:
            sql = f"{sql_main} LIMIT ?;"
            params = [kw, limit]

        cur.execute(sql, params)

    else:
        like = f"%{kw}%"

        sql_main = """
            SELECT postal, pref, city, town,
                   (pref_kana||' '||city_kana||' '||town_kana) AS kana,
                   (pref_en||' '||city_en||' '||town_en) AS rome
            FROM vw_zip
            WHERE pref LIKE ?
               OR city LIKE ?
               OR town LIKE ?
               OR pref_kana LIKE ?
               OR city_kana LIKE ?
               OR town_kana LIKE ?
               OR pref_en LIKE ?
               OR city_en LIKE ?
               OR town_en LIKE ?
        """

        if jig_ok:
            sql_jig = """
                SELECT postal, pref, city,
                       (town || ' ' || IFNULL(addr,'') || '（' || IFNULL(office_name,'') || '）') AS town,
                       (IFNULL(office_kana,'')) AS kana,
                       '' AS rome
                FROM jig.jigyosyo_zip
                WHERE pref LIKE ?
                   OR city LIKE ?
                   OR town LIKE ?
                   OR IFNULL(addr,'') LIKE ?
                   OR IFNULL(office_name,'') LIKE ?
                   OR IFNULL(office_kana,'') LIKE ?
            """
            sql = f"""
                SELECT * FROM (
                    {sql_main}
                    UNION ALL
                    {sql_jig}
                )
                LIMIT ?;
            """
            params = [like] * 9 + [like] * 6 + [limit]
        else:
            sql = f"{sql_main} LIMIT ?;"
            params = [like] * 9 + [limit]

        cur.execute(sql, params)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, obj: dict) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            q = (
                self._first(params, "q")
                or self._first(params, "keyword")
                or self._first(params, "query")
                or self._first(params, "zip")
                or self._first(params, "zipcode")
                or ""
            ).strip()

            if not q:
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "query": "",
                        "count": 0,
                        "results": [],
                        "message": "q / keyword / zip / zipcode を指定してください"
                    },
                )
                return

            rows = query(q, limit=300)

            self._send_json(
                200,
                {
                    "ok": True,
                    "query": q,
                    "count": len(rows),
                    "results": rows
                },
            )

        except Exception as e:
            self._send_json(
                500,
                {
                    "ok": False,
                    "error": str(e),
                    "results": []
                },
            )

    def _first(self, params: dict, key: str) -> str:
        vals = params.get(key, [])
        return vals[0] if vals else ""
