import csv
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

BASE_URL = "https://news.naver.com/main/ranking/popularMemo.naver"
SAVE_PATH = "data/article_urls.csv"


class ArticleScraper:
    def __init__(
        self,
        base_url: str,
        date: str,
        wait_time: int = 5,
        delay_time: float = 0.1,
    ):
        self._url = base_url + "?date=" + date
        self._wait_time = wait_time
        self._delay_time = delay_time
        self._date = date

    def scrap(self):
        articles = self._get_articles()
        with open(SAVE_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for title, url in articles:
                writer.writerow([title, url, self._date])

    def _init_driver(self):
        driver = webdriver.Chrome()
        driver.implicitly_wait(self._wait_time)
        driver.get(self._url)
        self.driver = driver

    def _get_articles(self):
        self._init_driver()
        while True:
            try:
                more = self.driver.find_element(
                    By.CLASS_NAME, "button_rankingnews_more"
                )
                more.click()
                time.sleep(self._delay_time)
            except:
                break

        html = self.driver.page_source

        self.driver.quit()

        soup = BeautifulSoup(html, "lxml")

        urls = soup.select("a.list_title")

        list_titles = [url.text for url in urls]
        list_urls = [url["href"] for url in urls]

        return zip(list_titles, list_urls)


class CommentScraper:
    pass


if __name__ == "__main__":
    import threading

    threads = []

    for i in range(5):
        date = "202501" + str(17 - i)
        scraper = ArticleScraper(BASE_URL, date)
        t = threading.Thread(target=scraper.scrap)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
