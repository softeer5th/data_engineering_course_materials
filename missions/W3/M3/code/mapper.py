#!/usr/bin/env python
import sys
import json

for line in sys.stdin:
    try:
        record = json.loads(line.strip())
        id = record.get("asin")
        rating = record.get("rating")

        if id and rating is not None:
            print(f"{id}\t{rating}\t{1}")
    except json.JSONDecodeError:
        continue
