from datetime import datetime
import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )

def upsert_daily_summary(conn, event: dict):
    activity = event["activity_type"]
    # Handle 'Z' which python's fromisoformat didn't like in older versions, 
    # ensuring robust parsing.
    ts = event["timestamp"].replace("Z", "+00:00")
    event_date = datetime.fromisoformat(ts).date()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_user_item_summary (
                user_id, item_id, date,
                view_count, click_count, purchase_count
            )
            VALUES (%s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (user_id, item_id, date)
            DO UPDATE SET
                view_count = daily_user_item_summary.view_count
                    + EXCLUDED.view_count,
                click_count = daily_user_item_summary.click_count
                    + EXCLUDED.click_count,
                purchase_count = daily_user_item_summary.purchase_count
                    + EXCLUDED.purchase_count,
                last_updated = NOW()
            """,
            (
                event["user_id"],
                event["item_id"],
                event_date,
                1 if activity == "view" else 0,
                1 if activity == "click" else 0,
                1 if activity == "purchase" else 0,
            )
        )
        conn.commit()


# import os
# import psycopg2
# from psycopg2.extras import RealDictCursor

# DATABASE_URL = os.getenv("DATABASE_URL")

# if not DATABASE_URL:
#     raise RuntimeError("DATABASE_URL not set")

# def get_connection():
#     return psycopg2.connect(DATABASE_URL)

# def upsert_daily_summary(
#     user_id,
#     item_id,
#     date,
#     activity_type
# ):
#     column_map = {
#         "view": "view_count",
#         "click": "click_count",
#         "purchase": "purchase_count"
#     }

#     column = column_map[activity_type]

#     query = f"""
#     INSERT INTO daily_user_item_summary (
#         user_id, item_id, date, {column}
#     )
#     VALUES (%s, %s, %s, 1)
#     ON CONFLICT (user_id, item_id, date)
#     DO UPDATE SET
#         {column} = daily_user_item_summary.{column} + 1,
#         last_updated = NOW();
#     """

#     conn = get_connection()
#     try:
#         with conn:
#             with conn.cursor() as cur:
#                 cur.execute(
#                     query,
#                     (user_id, item_id, date)
#                 )
#     finally:
#         conn.close()
