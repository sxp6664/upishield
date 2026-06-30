"""FastAPI: read API over the alerts table, plus a small stats endpoint and
a static dashboard. This is what makes the system demoable (Week 3)."""
import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PG_DSN = os.getenv(
    "PG_DSN",
    "dbname=upishield user=upishield password=upishield host=postgres port=5432",
)

app = FastAPI(title="UPIShield API", version="0.1.0")


def db():
    return psycopg2.connect(PG_DSN)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/alerts")
def alerts(limit: int = 50):
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT txn_id, card_id, amount, score, reasons, created_at "
            "FROM alerts ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


@app.get("/stats")
def stats():
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(avg(score),0), coalesce(sum(amount),0) FROM alerts")
        total, avg_score, exposure = cur.fetchone()
    return {
        "total_alerts": total,
        "avg_score": round(float(avg_score), 3),
        "flagged_exposure": round(float(exposure), 2),
    }


# serve the dashboard
app.mount("/static", StaticFiles(directory="/app/dashboard"), name="static")


@app.get("/")
def index():
    return FileResponse("/app/dashboard/index.html")
