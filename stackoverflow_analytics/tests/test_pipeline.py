import pytest

from analyticspipeline import StackOverFlowAnalyticsPipeline


@pytest.fixture(scope='function')
def stackoverflow_posts(tmpdir):
    posts = [{"xml": '<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="10" Title="Is SEO better better better done with repetition?" />',
              "PostTypeId": 1,
              "CreationDate": "2019-01-15T20:31:15.640",
              "Score": 10,
              "Title": "Is SEO better better better done with repetition?",
              "words": ["seo", "better", "done", "with", "repetition"]},

             {"xml": '<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="5" Title="What is SEO?" />',
             "PostTypeId": 1,
             "CreationDate": "2019-01-15T20:31:15.640",
             "Score": 5,
             "Title": "What is SEO?",
             "words": ["what", "seo"]},

             {"xml": '<row PostTypeId="1" CreationDate="2020-01-15T20:31:15.640" Score="20" Title="Is Python better than Javascript?" />',
             "PostTypeId": 1,
             "CreationDate": "2020-01-15T20:31:15.640",
             "Score": 20,
             "Title": "Is Python better than Javascript?",
             "words": ["python", "better", "javascript"]}]

    post_tmp_file = tmpdir.join("posts.txt")

    for post in posts:
        post_tmp_file.write(post["xml"]+"\n", "a")

    return {"posts": posts,
            "posts_filepath": post_tmp_file.strpath}


@pytest.fixture()
def stop_words():
    stop_words = ["is", "than"]

    return stop_words


def test_read_post(stackoverflow_posts):

    pipeline_sof = StackOverFlowAnalyticsPipeline()

    pipeline_sof.read_posts(stackoverflow_posts["posts_filepath"])

    assert len(stackoverflow_posts["posts"]) == len(pipeline_sof.posts)

    for expect_post, post in zip(stackoverflow_posts["posts"], pipeline_sof.posts):
        assert expect_post["PostTypeId"] == post.post_type_id
        assert expect_post["CreationDate"] == post.date
        assert expect_post["Score"] == post.score
        assert expect_post["Title"] == post.title


def test_extract_all_words_from_posts(stackoverflow_posts, stop_words):

    pipeline_sof = StackOverFlowAnalyticsPipeline()

    pipeline_sof.read_posts(stackoverflow_posts["posts_filepath"])

    pipeline_sof.extract_all_words_from_posts(stop_words, stop_words)

    posts = stackoverflow_posts["posts"]
    for post in posts:
        for except_word in post["word"]:
            assert except_word in pipeline_sof.all_words





