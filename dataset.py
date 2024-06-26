import pickle
import pandas as pd
import re
import json
import os
import scraping as sc


def dataset_loop(
    loop: bool = True,
    sources_load: bool = True,
    df_load: bool = True,
    save: bool = True,
    path: str = "data/",
    scraping: bool = False,
    save_scraping: bool = True,
    save_dataset: bool = True,
    ref_all: bool = True,
) -> pd.DataFrame:
    """
    Main loop for processing datasets.

    Parameters:
    loop (bool): Whether to run the loop or not.
    sources_load (bool): Whether to load sources from file or scrape them.
    df_load (bool): Whether to load existing datasets or create new ones.
    save (bool): Whether to save the final dataset.
    path (str): Path to the data directory.
    scraping (bool): Whether to perform scraping.
    save_scraping (bool): Whether to save scraped data.
    save_dataset (bool): Whether to save the created dataset.
    ref_all (bool): Whether to include all references or not.

    Returns:
    pd.DataFrame: The final dataset.
    """
    if loop:
        if sources_load:
            with open(os.path.join(path, "sources.txt"), "r") as f:
                law_sources = f.read().replace("'", '"')
            law_sources = json.loads(law_sources)
        else:
            law_sources = sc.source_scraper()

        df = pd.DataFrame()

        for source in law_sources:
            try:
                if df_load:
                    df_json = pd.read_json(os.path.join(path, "dataset", f"{source}.json"))
                    print(f"{source} loaded: length {len(df_json)}")
                    df = pd.concat([df, df_json], ignore_index=True)
                else:
                    df_json = dataset_creation(
                        source, scraping, save_scraping, save_dataset, ref_all
                    )
                    print(f"{source} stored: length {len(df_json)}")
                    df = pd.concat([df, df_json], ignore_index=True)
            except Exception as e:
                print(f"Error with {source}: {e}")
                with open(os.path.join(path, "errors.txt"), "a") as f:
                    f.write(source + "\n")
                continue

        if save:
            df.to_json(os.path.join(path, "dataset/all.json"), orient="records")
    else:
        df = pd.read_json(os.path.join(path, "dataset/all.json"))

    return df


def dataset_creation(
    law_source: str,
    scraping: bool = False,
    save_scraping: bool = True,
    save_dataset: bool = True,
    ref_all: bool = True,
    path: str = "data/dataset/",
    data: str = "data/soups/",
) -> pd.DataFrame:
    """
    Creates a dataset for a given law source.

    Parameters:
    law_source (str): The source of the law data.
    scraping (bool): Whether to perform scraping.
    save_scraping (bool): Whether to save scraped data.
    save_dataset (bool): Whether to save the created dataset.
    ref_all (bool): Whether to include all references or not.
    path (str): Path to save the dataset.
    data (str): Path to load the data.

    Returns:
    pd.DataFrame: The created dataset.
    """
    soups, links = load_data(law_source, scraping, save_scraping, path=data)
    df = dataset_elaboration(law_source, soups, links, save_dataset, ref_all, path)
    return df


def load_data(law_source: str, scraping: bool, save_scraping: bool, path: str = "data/soups/") -> tuple:
    """
    Loads data from pickle files or scrapes if necessary.

    Parameters:
    law_source (str): The source of the law data.
    scraping (bool): Whether to perform scraping.
    save_scraping (bool): Whether to save scraped data.
    path (str): Path to the data directory.

    Returns:
    tuple: Loaded soups and links.
    """
    if scraping:
        soups, links = sc.brocardi_scraper(law_source, save_scraping, path)
    else:
        with open(os.path.join(path, f"{law_source}.pkl"), "rb") as f:
            soups, links = zip(*pickle.load(f))

    print("Data loaded correctly")
    return soups, links


def dataset_elaboration(
    law_source: str, soups: list, links: list, save_dataset: bool, ref_all: bool = True, path: str = "data/"
) -> pd.DataFrame:
    """
    Elaborates dataset from soups and links.

    Parameters:
    law_source (str): The source of the law data.
    soups (list): List of BeautifulSoup objects.
    links (list): List of links corresponding to the soups.
    save_dataset (bool): Whether to save the dataset.
    ref_all (bool): Whether to include all references or not.
    path (str): Path to save the dataset.

    Returns:
    pd.DataFrame: The elaborated dataset.
    """
    data = []

    for soup, link in zip(soups, links):
        name_article = soup.find("h1", class_="hbox-header").text.strip()
        hierarchy = link.split("/")[2:-1]

        body_text = soup.find("div", class_="corpoDelTesto")
        article_text, references = "", []

        if body_text:
            article_text, references = extract_ref(law_source, body_text, ref_all)

        data.append(
            {
                "name": name_article,
                "hierarchy": hierarchy,
                "text": article_text,
                "references": references,
                "link": link,
            }
        )

    df = pd.DataFrame(data)
    print("Dataset created correctly")

    if save_dataset:
        df.to_json(os.path.join(path, f"{law_source}.json"), orient="records")

    return df


def extract_ref(law_source: str, body_text, ref_all: bool = True) -> tuple:
    """
    Extracts references from the body text.

    Parameters:
    law_source (str): The source of the law data.
    body_text (BeautifulSoup): The body text of the law.
    ref_all (bool): Whether to include all references or not.

    Returns:
    tuple: Cleaned paragraph text and references.
    """
    paragraph_text = body_text.text.strip()

    # Remove [word, etc], (numbers, word, etc), \n and double space from text
    paragraph_text = re.sub(r" \[[^\]]+\]|\([^\)]+\]|\([^\)]+\)", "", paragraph_text)
    paragraph_text = re.sub(r"\[[^\)]+\]|\([^\)]+\)", "", paragraph_text)
    paragraph_text = re.sub(r"\n|  ", " ", paragraph_text)

    if ref_all:
        references = [
            ref["href"]
            for ref in body_text.find_all("a", href=True)
            if not ref["href"].startswith("/dizionario")
            and not ref["href"].startswith("#nota_")
        ]
    else:
        references = [
            ref["href"]
            for ref in body_text.find_all("a", href=True)
            if ref["href"].startswith(f"/{law_source}/")
        ]

    return paragraph_text, references

def filter_list(df, filter_list):
    """
    Filter the DataFrame based on a list of prefixes.

    Parameters:
    df (pd.DataFrame): DataFrame to filter.
    filter_list (list): List of prefixes to filter by.

    Returns:
    pd.DataFrame: Filtered DataFrame.
    """
    df = df[df["link"].str.startswith(tuple(filter_list))]
    df.reset_index(drop=True, inplace=True)
    
    return df

if __name__ == "__main__":
    df = dataset_loop(sources_load = False)
    print(df)