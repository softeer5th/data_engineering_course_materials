#!/usr/bin/env python3

import sys
import json

for line in sys.stdin :
    row = json.loads(line.strip())
    product_id = row.get('asin').strip()
    rating = row.get('rating')

    print(f"{product_id}\t{rating}\t1")