import numpy as np
import pandas as pd

from typing import Set, List
from threading import Thread
from queue import Queue
from wiki import WikipediaParser
from argparse import ArgumentParser
from nltk.corpus import brown
from sklearn.feature_extraction.text import CountVectorizer


def crawl(url: str, unvisited: 'Queue[str]', visited: Set[str]) -> None:
    for link in WikipediaParser.get_links(url):
        if link not in visited:
            unvisited.put(link)


def walk(start_url: str, limit: int, wave_limit: int = 10) -> Set[str]:
    visited: Set[str] = set()
    unvisited: 'Queue[str]' = Queue()
    unvisited.put(start_url)
    visited.add(start_url)

    while not unvisited.empty() and len(visited) < limit:
        links: List[str] = []
        while not unvisited.empty() and len(links) < wave_limit:
            links.append(unvisited.get())
        new: 'Queue[str]' = Queue()
        threads = [Thread(target=crawl, args=(link, new, visited)) for link in links]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        unique_new: Set[str] = set()
        while not new.empty():
            unique_new.add(new.get())

        for link in unique_new:
            visited.add(link)
            if len(visited) == limit:
                break

        for link in unique_new:
            unvisited.put(link)

    return visited


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-n', '--count', default=1000, type=int, dest='n')
    parser.add_argument('-u', '--url', default='https://wikipedia.org/wiki/Wikipedia', type=str, dest='url')
    parser.add_argument('-d', '--destination', default='urls.txt', type=str, dest='dest')
    parser.add_argument('-c', '--words_count', default=10, type=int, dest='words_count')
    parser.add_argument('-w', '--words_destination', default='words.txt', type=str, dest='words_dest')
    args = parser.parse_args()

    with open(args.dest, 'w') as output:
        for url in walk(args.url, args.n):
            output.write(f'{url}\t\n')

    text = " ".join(word for word in brown.words() if len(word) > 2)
    vectorizer = CountVectorizer(stop_words='english')
    frequency = vectorizer.fit_transform([text]).toarray()[0]
    counts = pd.DataFrame(frequency, index=vectorizer.get_feature_names(), columns=["count"])
    counts.sort_values(by="count", ascending=False, inplace=True)
    counts['count'] /= counts['count'].sum()
    vocabulary = np.random.choice(counts.index, args.words_count, replace=False, p=counts['count'])

    with open(args.words_dest, 'w') as output:
        for word in vocabulary:
            output.write(f'{word}\t\n')

