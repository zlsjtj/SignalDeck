from datetime import datetime, timezone

def utc_day_key():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
