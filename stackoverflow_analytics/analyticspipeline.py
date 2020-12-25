import logging
import re
from typing import List
from xml.etree import cElementTree as ET

from post import Post
from post import Word


logger = logging.getLogger("pipeline")


class StackOverFlowAnalyticsPipeline:
    def __init__(self):
        self.posts = None
        self.all_words = None
        self.aggregated_all_words = None

    def read_posts(self, filepath) -> None:
        """
        Чтение постов

        из файла, где каждый пост это xmlline

        Args:
            filepath: путь до файла с постами в формате xml

        Returns: None
        """
        if self.posts is not None:
            logger.info("old post removed")

        self.posts = []

        logger.debug(f"path to file with posts: {filepath}")

        with open(filepath, "r") as f:
            for raw_post in f.readlines():
                raw_post = ET.fromstring(raw_post)
                title = raw_post.attrib['Title']
                score = int(raw_post.attrib['Score'])
                creation_date = raw_post.attrib['CreationDate']
                post_type_id = int(raw_post.attrib['PostTypeId'])

                post = Post(title, score, creation_date, post_type_id)
                self.posts.append(post)

        return None

    def extract_all_words_from_posts(self, stop_words=None):
        if self.posts is None:
            raise ValueError

        self.all_words = []

        for post in self.posts:
            words = re.findall("\w+", post.title.lower())
            words = self._filtering_stop_words(words, stop_words)



            raise NotImplementedError

    def get_words_between_date(self, words, start, end) -> List[Word]:
        raise NotImplementedError

    def aggregate_same_words(self, words) -> List[Word]:
        raise NotImplementedError

    def get_top_n_words(self, words, top_n) -> List[Word]:
        raise NotImplementedError

    def _filtering_stop_words(self, words, stop_words: List[str]) -> List[str]:
        if stop_words is None:
            return words
        else:
            for word in words:
                if word in stop_words:
                    words.remove(word)

            return words
