import pytest

from pipeline import Pipeline


@pytest.fixture(scope='function')
def stackoverflow_posts(tmpdir):
    posts = [{"xml": '<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="10" Title="Is SEO better better better done with repetition?" />',
              "PostTypeId": 1,
              "CreationDate": "2019-01-15T20:31:15.640",
              "Score": 10,
              "Title": "Is SEO better better better done with repetition?"},

             {"xml": '<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="5" Title="What is SEO?" />',
             "PostTypeId": 1,
             "CreationDate": "2019-01-15T20:31:15.640",
             "Score": 5,
             "Title": "What is SEO?"},

             {"xml": '<row PostTypeId="1" CreationDate="2020-01-15T20:31:15.640" Score="20" Title="Is Python better than Javascript?" />',
             "PostTypeId": 1,
             "CreationDate": "2020-01-15T20:31:15.640",
             "Score": 20,
             "Title": "Is Python better than Javascript?"}]

    post_tmp_file = tmpdir.join("posts.txt")

    for post in posts:
        post_tmp_file.write(post["xml"]+"\n", "a")

    return {"posts": posts,
            "posts_filepath": post_tmp_file.strpath}


def test_read_post(stackoverflow_posts):

    posts = Pipeline.read_posts(stackoverflow_posts["posts_filepath"])

    assert len(stackoverflow_posts["posts"]) == len(posts)

    for expect_post, post in zip(stackoverflow_posts["posts"], posts):
        assert expect_post["PostTypeId"] == post.post_type_id
        assert expect_post["CreationDate"] == post.creation_date
        assert expect_post["Score"] == post.score
        assert expect_post["Title"] == post.title