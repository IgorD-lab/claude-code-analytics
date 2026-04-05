import json
from collections import Counter
from pathlib import Path

path = Path("data/raw/telemetry_logs.jsonl")

messageTypes = Counter()
owners = Counter()
logGroups = Counter()
logStreams = Counter()
year_types = Counter()
month_types = Counter()
day_types = Counter()
empty_logEvents = 0
total = 0

with path.open() as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        e = json.loads(line)
        total += 1
        messageTypes[e.get("messageType")] += 1
        owners[e.get("owner")] += 1
        logGroups[e.get("logGroup")] += 1
        logStreams[e.get("logStream")] += 1
        year_types[type(e.get("year")).__name__] += 1
        month_types[type(e.get("month")).__name__] += 1
        day_types[type(e.get("day")).__name__] += 1
        le = e.get("logEvents")
        if not le:
            empty_logEvents += 1

print(f"Total envelopes: {total:,}\n")
print(f"messageType  ({len(messageTypes)} unique): {dict(messageTypes)}")
print(f"owner        ({len(owners)} unique): {dict(owners)}")
print(f"logGroup     ({len(logGroups)} unique): {dict(logGroups)}")
print(f"logStream    ({len(logStreams)} unique): {dict(logStreams)}")
print(f"\nyear  types : {dict(year_types)}")
print(f"month types : {dict(month_types)}")
print(f"day   types : {dict(day_types)}")
print(f"\nBatches with empty/missing logEvents: {empty_logEvents}")