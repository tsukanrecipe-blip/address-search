import sqlite3
from urllib.parse import parse_qs

def handler(request):
    q = request.query.get("q", "")

    if not q:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": "[]"
        }

    results = []

    try:
        conn = sqlite3.connect("zipcode.db")
        cur = conn.cursor()

        cur.execute(
            "SELECT zipcode, pref, city, town FROM zipcode WHERE zipcode LIKE ? OR town LIKE ? LIMIT 10",
            (f"%{q}%", f"%{q}%")
        )

        for r in cur.fetchall():
            results.append({
                "zipcode": r[0],
                "address": f"{r[1]}{r[2]}{r[3]}"
            })

        conn.close()

    except:
        pass

    if not results:
        try:
            conn = sqlite3.connect("jigyosyo.db")
            cur = conn.cursor()

            cur.execute(
                "SELECT zipcode, name, address FROM jigyosyo WHERE name LIKE ? LIMIT 10",
                (f"%{q}%",)
            )

            for r in cur.fetchall():
                results.append({
                    "zipcode": r[0],
                    "address": f"{r[1]} {r[2]}"
                })

            conn.close()

        except:
            pass

    import json

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results, ensure_ascii=False)
    }
