import pytest

from pipeline import Pipeline

@pytest.fixture()
def stackoverflow_posts():
    posts = """
            <row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="10" Title="Is SEO better better better done with repetition?" />	
            <row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="5" Title="What is SEO?" />	
            <row PostTypeId="1" CreationDate="2020-01-15T20:31:15.640" Score="20" Title="Is Python better than Javascript?" />	
            """

    # TODO сгенерить на основе текста выше временный xml файл