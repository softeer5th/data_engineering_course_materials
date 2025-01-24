import sys

current_product_id = None
current_rating = 0
current_count = 0

for line in sys.stdin :
    try :
        product_id, rating, count = line.strip().split("\t")
        rating = float(rating)
        count = int(count)
    except ValueError :
        continue

    if current_product_id == product_id :
        current_rating += rating
        current_count += count
    else :
        if current_product_id :
            print(f"{current_product_id}\t{current_count}\t{current_rating/current_count:.2f}")
        current_product_id = product_id
        current_rating = rating
        current_count = count

if current_product_id :
    print(f"{current_product_id}\t{current_count}\t{current_rating/current_count:.2f}") 
