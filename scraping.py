import time
import pickle
from bs4 import BeautifulSoup
from urllib.request import urlopen


def brocardi_scraper(law_source, save_scraping=True, path="data/soups/"):
    """
    Scrapes the specified law source from the Brocardi website.

    Parameters:
    law_source (str): The law source to scrape.
    save_scraping (bool): Whether to save the scraped data.
    path (str): The path to save the scraped data.

    Returns:
    soups (list): List of BeautifulSoup objects for the articles.
    articles (list): List of article URLs.
    """
    print(f"Scraping {law_source} started")
    url_root = "https://www.brocardi.it/"

    books = scrape_books(url_root, law_source)
    articles = scrape_articles(url_root, law_source, books)
    soups, missing = scrape_article_contents(url_root, articles)

    if save_scraping:
        store_scraped_data(soups, missing, articles, law_source, path)
    return soups, articles


def scrape_books(url_root, law_source):
    """
    Scrapes the book links from the Brocardi website.

    Parameters:
    url_root (str): The root URL of the Brocardi website.
    law_source (str): The law source to scrape.

    Returns:
    books (list): List of book URLs.
    """
    html = urlopen(url_root + law_source + "/").read()
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find(
        "div", {"class": "section_content content-box content-ext-guide"}
    )
    books = [link.get("href") for link in content.find_all("a") if link.get("href")]
    print("Book links scraped")
    return books


def scrape_articles(url_root, law_source, books):
    """
    Scrapes the article links from the books.

    Parameters:
    url_root (str): The root URL of the Brocardi website.
    law_source (str): The law source to scrape.
    books (list): List of book URLs.

    Returns:
    articles (list): List of article URLs.
    """
    articles = []
    for book in books:
        time.sleep(0.5)
        html = urlopen(url_root + book).read()
        soup = BeautifulSoup(html, "html.parser")
        links = [
            link.get("href")
            for link in soup.find_all("a")
            if link.get("href") and link.get("href").endswith("html")
        ]
        articles.extend(filter_articles(law_source, links))
    print("Article links scraped")
    return articles


def filter_articles(law_source, articles):
    """
    Filters the article links to include only those that belong to the specified law source.

    Parameters:
    law_source (str): The law source to scrape.
    articles (list): List of article URLs.

    Returns:
    filtered_articles (list): List of filtered article URLs.
    """
    filtered_articles = [link for link in articles if link.startswith(f"/{law_source}")]
    return filtered_articles


def scrape_article_contents(url_root, articles):
    """
    Scrapes the contents of the articles.

    Parameters:
    url_root (str): The root URL of the Brocardi website.
    articles (list): List of article URLs.

    Returns:
    soups (list): List of BeautifulSoup objects for the articles.
    missing (list): List of missing article URLs.
    """
    soups = []
    missing = []
    for article in articles:
        try:
            html = urlopen(url_root + article).read()
            soups.append(BeautifulSoup(html, "html.parser"))
        except:
            print(f"Article {article} not found")
            missing.append(article)
        time.sleep(0.5)
    print("Article contents scraped")
    return soups, missing


def store_scraped_data(soups, missing, articles, law_source, path):
    """
    Stores the scraped data.

    Parameters:
    soups (list): List of BeautifulSoup objects for the articles.
    missing (list): List of missing article URLs.
    articles (list): List of article URLs.
    law_source (str): The law source to scrape.
    path (str): The path to save the scraped data.
    """
    with open(f"{path}{law_source}.pkl", "wb") as f:
        pickle.dump(list(zip(soups, articles)), f)
    print(f"{law_source} data stored")

    if missing:
        with open(f"{path}{law_source}_missing.txt", "w") as m:
            m.write("\n".join(missing))


def source_scraper(url="https://www.brocardi.it/fonti.html", save=True, path="data/"):
    """
    Scrapes the source links from the Brocardi website.

    Parameters:
    url (str): The URL to scrape.
    save (bool): Whether to save the scraped data.
    path (str): The path to save the scraped data.

    Returns:
    sources (list): List of source URLs.
    """
    html = urlopen(url).read()
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", {"class": "content-box content-ext-guide"})
    sources = [
        link.get("href")[1:-1]
        for link in content.find_all("a")
        if link.get("href") and link.get("href").startswith("/")
    ]

    if save:
        with open(f"{path}sources.txt", "w") as f:
            f.write("\n".join(sources))
        print("Sources stored")

    return sources


if __name__ == "__main__":
    sources = source_scraper()
    # for source in sources:
    #     brocardi_scraper(source)
