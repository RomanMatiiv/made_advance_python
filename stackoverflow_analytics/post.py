
class Post:
    def __init__(self, title, score, creation_date, post_type_id=1):
        self.title = title
        self.score = score
        self.creation_date = creation_date
        self.post_type_id = post_type_id



class Word:
    def __init__(self, word, score):
        self.word = word
        self.score = score
