#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    try:
        data = json.loads(line.strip())
        product_id = data.get('asin')
        rating = data.get('rating')
        if product_id and rating is not None:
            print(f"{product_id}\t{rating}")
    except json.JSONDecodeError:
        continue
