from typing import Dict, Any, List
from bs4 import BeautifulSoup
from requests import get
from nltk import word_tokenize
from sys import stderr
import re

def normalize(text: str):
    text = ' '.join([word for word in word_tokenize(text.lower()) if len(word) > 3])
    return text

class WikipediaParser:
    @classmethod
    def get_content(cls, url: str):
        html = get(url)
        if html.status_code != 200:
            print(f"Get {html.status_code} on {url}", file=stderr)
            return None

        soup = BeautifulSoup(html.text, 'lxml')

        title = soup.find('h1', attrs={'id': 'firstHeading', 'class': 'firstHeading'})
        if title is not None:
            title = title.text

        content = soup.find('div', attrs={'id': 'mw-content-text'})
        if content is not None:
            content = content.text

        return ' '.join([normalize(title), normalize(content)])


    @classmethod
    def get_links(cls, url: str):
        html = get(url)

        if html.status_code != 200:
            print(f"Get {html.status_code} on {url}", file=stderr)
            return []

        soup = BeautifulSoup(html.text, 'lxml')

        link_tags = soup.findAll('a', attrs={'href': re.compile('(?=^/wiki/.+)'
                                                                '(?!^/wiki/User:.*)'
                                                                '(?!^/wiki/File:.*)'
                                                                '(?!^/wiki/Category:.*)'
                                                                '(?!^/wiki/Special:.*)'
                                                                '(?!^/wiki/Wikipedia:.*)'
                                                                '(?!^/wiki/Talk:.*)'
                                                                '(?!^/wiki/Template:.*)'
                                                                '(?!^/wiki/Template_talk:.*)'
                                                                '(?!^/wiki/Media:.*)'
                                                                '(?!^/wiki/User_talk:.*)'
                                                                '(?!^/wiki/Help:.*)'
                                                                '(?!^/wiki/Module:.*)'
                                                                '(?!^/wiki/Free_Content:.*)'
                                                                '(?!^/wiki/Portal:.*)')})

        return ['https://wikipedia.org' + tag['href'] for tag in link_tags]


if __name__ == "__main__":
    parser = WikipediaParser()
    contents = parser.get_content("https://en.wikipedia.org/wiki/Sviatchenko")

    print(contents)

