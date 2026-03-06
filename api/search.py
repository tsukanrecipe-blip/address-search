import json
import re
import sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

BASE_DIR = Path(__file__).resolve().parent.parent
ZIP_DB = BASE_DIR / "zipcode.db"
JIG_DB = BASE_DIR / "jigyosyo.db"


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

            query = (
                self._first(params, "q")
                or self._first(params, "keyword")
                or self._first(params, "query")
                or self._first(params, "zip")
                or self._first(params, "zipcode")
                or ""
            ).strip()

            if not query:
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "query": "",
                        "mode": "",
                        "source": "",
                        "count": 0,
                        "results": [],
                        "message": "q / keyword / zip / zipcode を指定してください"
                    },
                )
                return

            mode = "zipcode" if self._is_zipcode_query(query) else "keyword"

            # 先に zipcode.db
            zip_results = self._search_db(ZIP_DB, query, mode, source_name="zipcode.db")
            if zip_results["count"] > 0:
                self._send_json(200, zip_results)
                return

            # 見つからなければ jigyosyo.db
            jig_results = self._search_db(JIG_DB, query, mode, source_name="jigyosyo.db")
            self._send_json(200, jig_results)

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

    def _is_zipcode_query(self, s: str) -> bool:
        digits = self._normalize_zip(s)
        return len(digits) >= 3 and digits.isdigit()

    def _normalize_zip(self, s: str) -> str:
        return re.sub(r"\D", "", s or "")

    def _normalize_keyword(self, s: str) -> str:
        s = (s or "").strip()
        s = s.replace("　", " ")
        s = re.sub(r"\s+", " ", s)
        return s

    def _search_db(self, db_path: Path, query: str, mode: str, source_name: str) -> dict:
        if not db_path.exists():
            return {
                "ok": True,
                "query": query,
                "mode": mode,
                "source": source_name,
                "count": 0,
                "results": [],
                "message": f"{source_name} が見つかりません"
            }

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        try:
            objects = self._list_tables_and_views(conn)

            all_results = []
            seen = set()

            for obj_name in objects:
                cols = self._get_columns(conn, obj_name)
                if not cols:
                    continue

                rows = self._search_table(conn, obj_name, cols, query, mode)
                for row in rows:
                    item = self._row_to_result(obj_name, cols, row)
                    sig = json.dumps(item, ensure_ascii=False, sort_keys=True)
                    if sig not in seen:
                        seen.add(sig)
                        all_results.append(item)

            return {
                "ok": True,
                "query": query,
                "mode": mode,
                "source": source_name,
                "count": len(all_results),
                "results": all_results[:100]
            }
        finally:
            conn.close()

    def _list_tables_and_views(self, conn: sqlite3.Connection) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND name NOT LIKE 'sqlite_%'
            ORDER BY
              CASE
                WHEN lower(name) LIKE '%ken_all%' THEN 1
                WHEN lower(name) LIKE '%jigyosyo%' THEN 2
                ELSE 9
              END,
              name
            """
        )
        return [r[0] for r in cur.fetchall()]

    def _get_columns(self, conn: sqlite3.Connection, table_name: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(f'PRAGMA table_info("{table_name}")')
        return [r[1] for r in cur.fetchall()]

    def _search_table(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        cols: list[str],
        query: str,
        mode: str
    ) -> list[sqlite3.Row]:
        if mode == "zipcode":
            return self._search_table_by_zipcode(conn, table_name, cols, query)
        return self._search_table_by_keyword(conn, table_name, cols, query)

    def _search_table_by_zipcode(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        cols: list[str],
        query: str
    ) -> list[sqlite3.Row]:
        digits = self._normalize_zip(query)
        if not digits:
            return []

        zip_cols = self._find_zip_columns(cols)
        if not zip_cols:
            return []

        where_parts = []
        params = []

        for c in zip_cols:
            qc = f'"{c}"'
            where_parts.append(f"REPLACE(REPLACE(CAST({qc} AS TEXT), '-', ''), '〒', '') = ?")
            params.append(digits)

            if len(digits) >= 3:
                where_parts.append(f"REPLACE(REPLACE(CAST({qc} AS TEXT), '-', ''), '〒', '') LIKE ?")
                params.append(f"{digits}%")

        sql = f'''
            SELECT *
            FROM "{table_name}"
            WHERE {" OR ".join(where_parts)}
            LIMIT 50
        '''
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()

    def _search_table_by_keyword(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        cols: list[str],
        query: str
    ) -> list[sqlite3.Row]:
        q = self._normalize_keyword(query)
        if not q:
            return []

        text_cols = self._find_keyword_columns(cols)
        if not text_cols:
            return []

        where_parts = []
        params = []

        for c in text_cols:
            qc = f'"{c}"'
            where_parts.append(f"CAST({qc} AS TEXT) LIKE ?")
            params.append(f"%{q}%")

        sql = f'''
            SELECT *
            FROM "{table_name}"
            WHERE {" OR ".join(where_parts)}
            LIMIT 50
        '''
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()

    def _find_zip_columns(self, cols: list[str]) -> list[str]:
        result = []
        for c in cols:
            lc = c.lower()
            if (
                "zip" in lc
                or "postal" in lc
                or "postcode" in lc
                or "郵便番号" in c
            ):
                result.append(c)
        return result

    def _find_keyword_columns(self, cols: list[str]) -> list[str]:
        preferred = []
        fallback = []

        for c in cols:
            lc = c.lower()

            if any(x in lc for x in ["zip", "postal", "postcode"]):
                continue
            if "郵便番号" in c:
                continue

            if any(x in lc for x in [
                "pref", "city", "town", "addr", "address",
                "name", "kana", "office", "company", "jigyosyo"
            ]):
                preferred.append(c)
                continue

            if any(x in c for x in [
                "都道府県", "市区町村", "町域", "住所", "所在地",
                "名称", "事業所", "会社", "カナ"
            ]):
                preferred.append(c)
                continue

            fallback.append(c)

        return preferred if preferred else fallback[:10]

    def _row_to_result(self, table_name: str, cols: list[str], row: sqlite3.Row) -> dict:
        raw = {col: row[col] for col in cols}

        zipcode = self._pick_first(raw, [
            "zipcode", "zip", "postal_code", "postal", "郵便番号",
            "zip_code", "post_code"
        ])

        name = self._pick_first(raw, [
            "name", "office_name", "company_name", "jigyosyo_name",
            "事業所名", "名称", "会社名"
        ])

        pref = self._pick_first(raw, ["pref", "prefecture", "都道府県"])
        city = self._pick_first(raw, ["city", "市区町村"])
        town = self._pick_first(raw, ["town", "町域"])
        addr = self._pick_first(raw, ["address", "addr", "所在地", "住所"])

        if not addr:
            parts = [x for x in [pref, city, town] if x]
            addr = "".join(parts)

        return {
            "table": table_name,
            "zipcode": "" if zipcode is None else str(zipcode),
            "name": "" if name is None else str(name),
            "address": "" if addr is None else str(addr),
            "raw": self._stringify_raw(raw)
        }

    def _pick_first(self, raw: dict, candidates: list[str]):
        raw_keys = list(raw.keys())

        # 完全一致
        for cand in candidates:
            for k in raw_keys:
                if k == cand:
                    return raw[k]

        # 小文字一致
        for cand in candidates:
            cand_l = cand.lower()
            for k in raw_keys:
                if k.lower() == cand_l:
                    return raw[k]

        # 部分一致
        for cand in candidates:
            cand_l = cand.lower()
            for k in raw_keys:
                if cand in k or cand_l in k.lower():
                    return raw[k]

        return None

    def _stringify_raw(self, raw: dict) -> dict:
        out = {}
        for k, v in raw.items():
            if v is None:
                out[k] = ""
            else:
                out[k] = str(v)
        return out
