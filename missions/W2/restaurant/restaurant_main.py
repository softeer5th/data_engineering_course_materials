import restaurant_extrator as e

# Const block
PATH = 'missions/W2/restaurant/data/'
X = 127.06283102249932
Y = 37.514322572335935
R = 1000  # 반경
API_KEY = '15545f98091012bb544fdad53077cee2'
RESTAURANTS_FILENAME = 'restaurant.json'

'''
# Extract
js_restaurants = e.get_restaurants(X, Y, R, API_KEY)
try:
    e.save_to_file(js_restaurants, PATH, RESTAURANTS_FILENAME)
except Exception as e:
    print(e)
'''
decoded = e.get_review_data('https://place.map.kakao.com/25491455')
e.save_to_file(decoded, PATH,'decoded', 'html')
