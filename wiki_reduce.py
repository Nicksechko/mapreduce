from typing import Set, List, Dict
from collections import defaultdict
from sys import stdin

if __name__ == "__main__":
    dictionary = defaultdict(set)
    for row in stdin:
        word, urls = row.split('\t')
        dictionary[word].update(url for url in urls[:-1].split(' ') if url)

    for key, value in dictionary.items():
        print(f'{key}\t{" ".join(sorted(value))}')

