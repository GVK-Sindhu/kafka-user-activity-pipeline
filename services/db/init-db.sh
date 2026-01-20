#!/bin/bash
set -e

echo "Initializing database schema..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS daily_user_item_summary (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    item_id UUID NOT NULL,
    date DATE NOT NULL,
    view_count INTEGER NOT NULL DEFAULT 0,
    click_count INTEGER NOT NULL DEFAULT 0,
    purchase_count INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, item_id, date)
);
EOSQL

echo "Database schema created successfully."
