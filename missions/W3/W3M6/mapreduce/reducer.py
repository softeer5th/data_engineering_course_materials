#!/usr/bin/env python3
import sys

current_product_id = None
total_rating = 0
review_count = 0

for line in sys.stdin:
    line = line.strip()
    product_id, rating = line.split('\t')
    rating = float(rating)
    
    if product_id == current_product_id:
        total_rating += rating
        review_count += 1
    else:
        if current_product_id:
            avg_rating = total_rating / review_count
            print(f"{current_product_id}\t{review_count}\t{avg_rating:.1f}")
        current_product_id = product_id
        total_rating = rating
        review_count = 1

if current_product_id:
    avg_rating = total_rating / review_count
    print(f"{current_product_id}\t{review_count}\t{avg_rating:.1f}")