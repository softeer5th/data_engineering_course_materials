import requests
from bs4 import BeautifulSoup
import json
import logging
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from konlpy.tag import Okt
import concurrent.futures
import time


# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# 불용어 리스트
STOPWORDS = {"회사", "직원", "일", "정도", "경우", "때문", "이런", "그런", "저런", "및", "이것", "저것", "것", "워", "라밸", "라벨", "밸", "벨"}

def fetch_page_content(url, headers):
    """Fetch the HTML content of a given URL."""
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch URL {url}: {e}")
        return None
    
def parse_review(review):
    """Extract relevant data from a single review element."""
    try:
        review_content = review.find("div", class_="review_item_inr")
        
        # Extract rating
        raw_rating = review_content.find("div", class_="rating").find("strong", class_="num").get_text(strip=True)
        rating = float(raw_rating.replace("Rating Score", "").strip())
        
        # Extract title
        raw_title = review_content.find("h3", class_="rvtit").get_text(strip=True)
        title = raw_title[1:-1]
        
        # Extract status
        raw_status = review_content.find("div", class_="auth").find("strong").get_text(strip=True)
        status = raw_status.replace("Verified User", "").strip()
        
        # Extract info
        raw_info = review_content.find("div", class_="auth").get_text(strip=True)
        
        # Extract job and date from info
        parts = raw_info.split()
        job = parts[4].strip()
        date = parts[-1].strip()
        
        return {
            "review": rating,
            "title": title,
            "status": status,
            "job": job,
            "date": date
        }
    except AttributeError as e:
        logging.warning(f"Skipping a review due to missing elements: {e}")
        return None
    
def fetch_reviews_for_page(base_url, page, headers):
    """Fetch reviews for a specific page."""
    url = f"{base_url}{page}"
    logging.info(f"Fetching page {page}: {url}")
    soup = fetch_page_content(url, headers)
    if not soup:
        return []

    reviews = soup.find_all("div", class_="review_item")
    logging.info(f"Found {len(reviews)} reviews on page {page}")

    page_reviews = []
    for index, review in enumerate(reviews, start=1):
        review_data = parse_review(review)
        if review_data:
            page_reviews.append(review_data)
            logging.info(f"Processed review {index} on page {page}: {review_data['title']}")
    return page_reviews

def extract_reviews_multithreaded(base_url, max_pages, headers):
    """Extract reviews using multithreading."""
    all_reviews = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        future_to_page = {
            executor.submit(fetch_reviews_for_page, base_url, page, headers): page
            for page in range(1, max_pages + 1)
        }
        for future in concurrent.futures.as_completed(future_to_page):
            page = future_to_page[future]
            try:
                page_reviews = future.result()
                all_reviews.extend(page_reviews)
            except Exception as e:
                logging.error(f"Error fetching reviews for page {page}: {e}")
    return all_reviews

def save_reviews_to_json(reviews, output_file):
    """Save extracted reviews to a JSON file."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(reviews, f, ensure_ascii=False, indent=4)
    logging.info(f"Saved {len(reviews)} reviews to {output_file}")

def load_reviews_from_json(input_file):
    """Load reviews from a JSON file."""
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            reviews = json.load(f)
            logging.info(f"Loaded {len(reviews)} reviews from {input_file}")
            return reviews
    except FileNotFoundError:
        logging.error(f"File {input_file} not found.")
        return []
    

def divide_reviews(reviews):
    """Divide reviews into positive and negative categories."""
    positive_reviews = [review for review in reviews if review["review"] > 3.0]
    negative_reviews = [review for review in reviews if review["review"] < 3.0]
    return positive_reviews, negative_reviews

def preprocess_text(text, okt, stopwords=STOPWORDS):
    """Preprocess text by removing stopwords and extracting meaningful stems."""
    tokens = okt.nouns(text)
    return [token for token in tokens if token not in stopwords]

def generate_wordcloud(tokens, title, font_path):
    """Generate and display a word cloud."""
    word_counts = Counter(tokens)
    wordcloud = WordCloud(
        width=400, height=400, background_color="white", font_path=font_path
    ).generate_from_frequencies(word_counts)
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(title, fontsize=16)

def visualize_wordclouds(positive_reviews, negative_reviews, font_path):
    """Generate and display word clouds for positive and negative reviews."""
    okt = Okt()
    positive_tokens = preprocess_text(" ".join(review["title"] for review in positive_reviews), okt)
    negative_tokens = preprocess_text(" ".join(review["title"] for review in negative_reviews), okt)

    # Plotting two word clouds side by side
    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    plt.sca(axes[0])
    generate_wordcloud(positive_tokens, "Positive Reviews", font_path)
    plt.sca(axes[1])
    generate_wordcloud(negative_tokens, "Negative Reviews", font_path)
    plt.tight_layout()
    plt.show()

def main(base_url, max_pages, output_file):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/109.0.0.0 Safari/537.36"
        )
    }
    # 1) 직접 Extract 해서 json 저장하는 옵션
    reviews = extract_reviews_multithreaded(base_url, max_pages, headers)
    save_reviews_to_json(reviews, output_file)

    # 2) 생성되어 있는 json 파일을 로드하는 옵션
    # reviews = load_reviews_from_json(output_file)

    positive_reviews, negative_reviews = divide_reviews(reviews)
    visualize_wordclouds(positive_reviews, negative_reviews, font_path="NanumGothic.ttf")

if __name__ == "__main__":
    BASE_URL = "https://www.teamblind.com/kr/company/현대자동차/reviews?page="
    MAX_PAGES = 100  # Number of pages to crawl
    OUTPUT_FILE = "hyundai_reviews.json"
    THREAD_COUNT = 4
    main(BASE_URL, MAX_PAGES, OUTPUT_FILE)

