-- ===============================
-- Seed Data for API Testing
-- ===============================

-- User WITH activity (used for 200 OK API test)
INSERT INTO daily_user_item_summary (
    user_id,
    item_id,
    date,
    view_count,
    click_count,
    purchase_count
) VALUES (
    '11111111-1111-1111-1111-111111111111',
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    CURRENT_DATE,
    5,
    2,
    1
)
ON CONFLICT (user_id, item_id, date)
DO NOTHING;

-- User WITHOUT activity
-- Used to verify API returns 404
-- user_id: 22222222-2222-2222-2222-222222222222
