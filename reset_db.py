#!/usr/bin/env python3
"""
Reset the database and move sample CSV back to inbox for a fresh run.
Usage: python3 reset_db.py
"""
import os
import shutil

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PIPELINE_DIR, "db", "sme_intelligence.db")
SAMPLE_SRC = os.path.join(PIPELINE_DIR, "data", "sample_fibu.csv")
INBOX_DIR = os.path.join(PIPELINE_DIR, "data", "inbox")
PROCESSED_DIR = os.path.join(PIPELINE_DIR, "data", "processed")
OUTPUT_DIR = os.path.join(PIPELINE_DIR, "output")

print("Resetting SME Data Intelligence Pipeline...")

# 1. Delete database
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"  Deleted database: {DB_PATH}")
else:
    print(f"  No database to delete.")

# 2. Clear processed folder
if os.path.exists(PROCESSED_DIR):
    for f in os.listdir(PROCESSED_DIR):
        fp = os.path.join(PROCESSED_DIR, f)
        if os.path.isfile(fp):
            os.remove(fp)
    print(f"  Cleared processed folder.")

# 3. Copy sample CSV to inbox
if os.path.exists(SAMPLE_SRC):
    os.makedirs(INBOX_DIR, exist_ok=True)
    dest = os.path.join(INBOX_DIR, "sample_fibu.csv")
    shutil.copy2(SAMPLE_SRC, dest)
    print(f"  Copied sample CSV to inbox: {dest}")
else:
    print(f"  No sample CSV found at {SAMPLE_SRC}")

# 4. Clear output folder
if os.path.exists(OUTPUT_DIR):
    for f in os.listdir(OUTPUT_DIR):
        fp = os.path.join(OUTPUT_DIR, f)
        if os.path.isfile(fp) and f.endswith(".docx"):
            os.remove(fp)
    print(f"  Cleared output folder.")

print("\nDone! You can now run: python3 run_pipeline.py")
