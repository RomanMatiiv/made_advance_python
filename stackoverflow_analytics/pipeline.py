import logging
from typing import List
from xml.etree import cElementTree as ET

from post import Post


logger = logging.getLogger("pipeline")


class Pipeline:
    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def read_posts(filepath) -> List[Post]:
        """
        Чтение постов

        из файла, где каждый пост это строка в формате xml

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

