import logging
from datetime import datetime

logger = logging.getLogger("post")


class Post:
    def __init__(self, title: str, score: int, date: datetime, post_type_id=1):
        self.title = title
        self.score = score
        self.date = date
        self.post_type_id = post_type_id


class Word:
    def __init__(self, word: str, score: int, date: datetime):
        self.word = word
        self.score = score
        self.date = date

        # TODO добавить операторы для сравнения по году
