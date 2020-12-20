import logging
from xml.etree import cElementTree as ET

from post import Post


logger = logging.getLogger("pipeline")


class Pipeline:
    def __init__(self):
        raise NotImplementedError

    def read_posts(self, filepath):
        posts = []

        logger.debug("path to file with posts: {filepath}")

        with open(filepath, "r") as f:
            for raw_post in f.readlines():
                root = ET.fromstring(raw_post)
                for page in list(root):
                    title = page.find('Title').text
                    score = page.find('Score').text
                    creation_date = page.find('CreationDate').text
                    post_type_id = page.find('PostTypeId').text

                post = Post(title, score, creation_date, post_type_id)
                posts.append(post)

        return posts

