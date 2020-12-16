import pytest

from pipeline import Pipeline

@pytest.fixture(scope='function')
def stackoverflow_posts_path(tmpdir):
    posts =['<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="10" Title="Is SEO better better better done with repetition?" />',
            '<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="5" Title="What is SEO?" />',
            '<row PostTypeId="1" CreationDate="2020-01-15T20:31:15.640" Score="20" Title="Is Python better than Javascript?" />',
            ]
    post_tmp_file = tmpdir.join("posts.txt")

    for post in posts:
        post_tmp_file.write(post+"\n","a")

    return post_tmp_file.strpath