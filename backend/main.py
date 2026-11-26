from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from database import get_postgres_connection, get_mongo_database

app = FastAPI(
    title="Smart Traffic Management API",
    description="Real-Time Big Data Traffic Analytics",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/")
def home():
    return {
        "project": "Real-Time Smart Traffic Management",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/api/health")
def health():
    result = {
        "api": "connected",
        "postgresql": "disconnected",
        "mongodb": "disconnected"
    }

    try:
        conn = get_postgres_connection()
        conn.close()
        result["postgresql"] = "connected"
    except Exception as exc:
        result["postgresql_error"] = str(exc)

    try:
        db = get_mongo_database()
        db.client.admin.command("ping")
        result["mongodb"] = "connected"
    except Exception as exc:
        result["mongodb_error"] = str(exc)

    return result


@app.get("/api/metrics")
def get_metrics(limit: int = Query(default=100, ge=1, le=1000)):
    conn = None

    try:
        conn = get_postgres_connection()

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT *
                FROM traffic_metrics
                ORDER BY recorded_at DESC
                LIMIT %s
            """, (limit,))

            rows = cursor.fetchall()

        return {
            "count": len(rows),
            "data": rows
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    finally:
        if conn:
            conn.close()


@app.get("/api/summary")
def get_summary():
    conn = None

    try:
        conn = get_postgres_connection()

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) AS total_records,
                    COALESCE(SUM(traffic_volume), 0) AS total_vehicles,
                    ROUND(AVG(avg_vehicle_speed)::numeric, 2)
                        AS average_speed,
                    COUNT(DISTINCT location_id) AS monitored_locations
                FROM traffic_metrics
            """)

            summary = cursor.fetchone()

            cursor.execute("""
                SELECT
                    congestion_status,
                    COUNT(*) AS total
                FROM traffic_metrics
                GROUP BY congestion_status
            """)

            congestion = cursor.fetchall()

        return {
            "summary": summary,
            "congestion": congestion
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    finally:
        if conn:
            conn.close()


@app.get("/api/alerts")
def get_alerts(limit: int = Query(default=100, ge=1, le=1000)):
    try:
        db = get_mongo_database()

        alerts = list(
            db.traffic_alerts.find(
                {},
                {"_id": 0}
            ).sort("_id", -1).limit(limit)
        )

        for alert in alerts:
            for key, value in alert.items():
                if isinstance(value, datetime):
                    alert[key] = value.isoformat()

        return {
            "count": len(alerts),
            "data": alerts
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/locations")
def get_locations():
    conn = None

    try:
        conn = get_postgres_connection()

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    location_id,
                    location,
                    AVG(traffic_volume) AS avg_traffic_volume,
                    AVG(avg_vehicle_speed) AS avg_speed,
                    MAX(recorded_at) AS last_updated
                FROM traffic_metrics
                GROUP BY location_id, location
                ORDER BY location_id
            """)

            return {
                "data": cursor.fetchall()
            }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    finally:
        if conn:
            conn.close()
