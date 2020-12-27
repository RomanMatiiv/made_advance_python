from datetime import datetime

import pytest

from analyticspipeline import StackOverFlowAnalyticsPipeline


@pytest.fixture(scope='function')
def stackoverflow_posts(tmpdir):
    posts = [{"xml": '<row PostTypeId="1" CreationDate="2018-03-15T20:31:15.640" Score="10" Title="Is SEO better better better done with repetition?" />',
              "PostTypeId": 1,
              "CreationDate": datetime(year=2018,
                                       month=3,
                                       day=15,
                                       hour=20,
                                       minute=31,
                                       second=15,
                                       microsecond=640),
              "Score": 10,
              "Title": "Is SEO better better better done with repetition?",
              "words": ["seo", "better", "done", "with", "repetition"]},

             {"xml": '<row PostTypeId="1" CreationDate="2019-01-15T20:31:15.640" Score="5" Title="What is SEO?" />',
             "PostTypeId": 1,
              "CreationDate": datetime(year=2019,
                                       month=1,
                                       day=15,
                                       hour=20,
                                       minute=31,
                                       second=15,
                                       microsecond=640),
             "Score": 5,
             "Title": "What is SEO?",
             "words": ["what", "seo"]},

             {"xml": '<row PostTypeId="1" CreationDate="2020-01-15T20:32:15.640" Score="20" Title="Is Python better than Javascript?" />',
             "PostTypeId": 1,
              "CreationDate": datetime(year=2020,
                                       month=1,
                                       day=15,
                                       hour=20,
                                       minute=32,
                                       second=15,
                                       microsecond=640),
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

    pipeline_sof.extract_all_words_from_posts(stop_words)

    all_expect_word = []
    for post in stackoverflow_posts["posts"]:
        all_expect_word.extend(post["words"])

    all_extract_word = []
    for word in pipeline_sof.all_words:
        all_extract_word.append(word.word)

    for expect_word in all_expect_word:
        assert expect_word in all_extract_word


def test_extract_all_words_check_stop_words(stackoverflow_posts, stop_words):
    pipeline_sof = StackOverFlowAnalyticsPipeline()

    pipeline_sof.read_posts(stackoverflow_posts["posts_filepath"])

    pipeline_sof.extract_all_words_from_posts(stop_words)

    all_extract_word = []
    for word in pipeline_sof.all_words:
        all_extract_word.append(word.word)

    for stop_w in stop_words:
        assert stop_w not in all_extract_word
