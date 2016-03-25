#schema of the loger
session.execute("""
CREATE TABLE IF NOT EXISTS "logData" (
    "logId" varchar,
    "userId" varchar,
    "eventType" varchar,
    "eventTime" timestamp,
    "eventSource" varchar,
    "eventSection" varchar,
    "itemId" varchar,
    "itemType" varchar,
    quantity int,
    price float,
    PRIMARY KEY (logId)
)
""")
