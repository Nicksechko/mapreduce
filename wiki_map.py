from wiki import WikipediaParser
from typing import Set, List, Dict
from threading import Thread
from queue import Queue
from argparse import ArgumentParser
from sys import stdin


def get_content(url: str, contents: 'Queue[str]') -> None:
    contents.put((url, WikipediaParser.get_content(url)))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-w', '--words', default='words.txt', type=str, dest='words')
    args = parser.parse_args()

    contents: 'Queue[str]' = Queue()
    urls = [row.split('\t')[0] for row in stdin]

    dictionary: Dict[str, Set[str]] = dict()
    with open(args.words, 'r') as words:
        for key, value in [row.split('\t') for row in words]:
            dictionary[key] = set()

    while urls:
        threads = []
        while urls and len(threads) < 10:
            threads.append(Thread(target=get_content, args=(urls[-1], contents)))
            urls.pop()

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    while not contents.empty():
        url, content = contents.get()
        for word in content.split():
            if word in dictionary:
                dictionary[word].add(url)

    for key, value in dictionary.items():
        print(f'{key}\t{" ".join(value)}')

