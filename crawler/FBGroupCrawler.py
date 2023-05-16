import pandas as pd
from selenium import webdriver
import bs4
import facebook_crawler
import pandas as pd
# import typing
# import functools
# import asyncio


# def to_thread(func: typing.Callable) -> typing.Coroutine:
#     @functools.wraps(func)
#     async def wrapper(*args, **kwargs):
#         return await asyncio.to_thread(func, *args, **kwargs)
#     return wrapper


class FBGroupCrawler:
    def __init__(self, group_id):
        self.group_id = group_id
        self.url = 'https://www.facebook.com/groups/' + group_id

    # @to_thread
    def crawl_posts_after_date(self, date):
        is_crawl_success = False
        while True:
            try:
                posts = facebook_crawler.Crawl_GroupPosts(self.url, until_date=date)
                posts = posts[posts['TIME'] > date]
                posts = posts.sort_values(by='TIME', ascending=True)
                posts['CONTENT'] = posts['CONTENT'].apply(
                    lambda x: '\n'.join(x.split()))
                return posts
            except Exception as e:
                # print(e)
                print('Retrying...')
                continue

    def crawl_posts_after_post(self, post_id, post_date):
        posts = self.crawl_posts_after_date(post_date)
        posts = posts[posts['POSTID'] > post_id]
        return posts