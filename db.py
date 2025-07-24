import sqlite3
import os

os.makedirs("logs", exist_ok=True)
# Initialize or connect
conn = sqlite3.connect("data/llm_usage.db")
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS usage_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    topic TEXT,
    prompt TEXT,
    summary TEXT,
    tokens_in INTEGER,
    tokens_out INTEGER,
    total_tokens INTEGER,
    cost_usd_input REAL,
    cost_usd_output REAL
);
""")
conn.commit()
