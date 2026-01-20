from fastapi import FastAPI, HTTPException, Path
from typing import List
from uuid import UUID

from app.db import get_connection
from app.models import UserActivitySummary

app = FastAPI(
    title="User Activity Summary API",
    version="1.0.0"
)

# -----------------------------
# Health Check
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -----------------------------
# API Endpoint
# -----------------------------
@app.get(
    "/api/user-activity-summary/{user_id}",
    response_model=List[UserActivitySummary]
)
def get_user_activity_summary(
    user_id: UUID = Path(..., description="User UUID")
):
    query = """
    SELECT
        item_id,
        date,
        view_count,
        click_count,
        purchase_count
    FROM daily_user_item_summary
    WHERE user_id = %s
    ORDER BY date DESC;
    """

    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, (str(user_id),))
                rows = cur.fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No activity found for user_id {user_id}"
            )

        return rows

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )

    finally:
        if "conn" in locals():
            conn.close()
