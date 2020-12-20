import logging
from typing import List
from xml.etree import cElementTree as ET

from post import Post
from post import Word


logger = logging.getLogger("pipeline")


class AnalyticsPipeline:
    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def read_posts(filepath) -> List[Post]:
        """
        Чтение постов

        из файла, где каждый пост это xmlline

        Args:
            filepath: путь до файла с постами в формате xml

        Returns: массив с постами
        """
        posts = []

        logger.debug("path to file with posts: {filepath}")

        with open(filepath, "r") as f:
            for raw_post in f.readlines():
                raw_post = ET.fromstring(raw_post)
                title = raw_post.attrib['Title']
                score = int(raw_post.attrib['Score'])
                creation_date = raw_post.attrib['CreationDate']
                post_type_id = int(raw_post.attrib['PostTypeId'])

                post = Post(title, score, creation_date, post_type_id)
                posts.append(post)

        return posts

    def extract_all_words_from_posts(self, posts) -> List[Word]:
        raise NotImplementedError

    def get_words_between_date(self, words, start, end) -> List[Word]:
        raise NotImplementedError

    def aggregate_same_words(self, words) -> List[Word]:
        raise NotImplementedError

    def get_top_n_words(self, words, top_n) -> List[Word]:
        raise NotImplementedError
