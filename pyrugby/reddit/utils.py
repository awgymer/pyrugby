import logging
import re
import time
import datetime

import requests
import markdown
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)
PUSHSHIFT_URL = "https://api.pushshift.io/reddit/{search_type}/search"


def get_all_pushshift_comments(submission_id):
    start = time.time()
    url = PUSHSHIFT_URL.format(search_type='comment')
    params = {
        'link_id': submission_id,
        'sort_type': "created_utc",
        'sort': "desc",
        'size': 1000,
        'before': int(
            datetime.datetime.now(datetime.timezone.utc).timestamp()
        )
    }
    all_comments = []
    while True:
        r = requests.get(url, params=params)
        data = r.json()['data']
        if not data:
            break
        all_comments.extend(data)
        if len(data) < params['size']:
            break
        params['before'] = data[-1]['created_utc'] + 1
    end = time.time()
    log.debug(
        'Retrieved %d total comments in %d seconds',
        len(all_comments), end-start
    )
    return all_comments


def flair_id_from_template(flairdikt):
    if not flairdikt['css_class']:
        emojis = [
            d['a'] for
            d in flairdikt['richtext']
            if d.get('e') == 'emoji'
        ]
        if not emojis:
            print('WARNING: Flair with richtext but no emoji!')
            print(flairdikt)
            return False
        if len(emojis) > 1:
            print(f'WARNING: More than one emoji found! {emojis}')
        return emojis[0]
    else:
        return flairdikt['css_class']


def get_flair_identifier(comm):
    if not comm.get('author_flair_css_class'):
        if not comm.get('author_flair_richtext'):
            return None
        emojis = [
            d['a'] for
            d in comm['author_flair_richtext']
            if d.get('e') == 'emoji'
        ]
        if not emojis:
            print('WARNING: Comment with richtext but no emoji!')
            return None
        if len(emojis) > 1:
            print(f'WARNING: More than one emoji found! {emojis}')
        return emojis[0]
    else:
        return comm['author_flair_css_class']


def comment_md_to_plaintext(mdtext):
    html = markdown.markdown(mdtext)
    soup = BeautifulSoup(html, features="html.parser")
    comment = "".join(soup.find_all(text=True))
    comment = re.sub(r'\s+', ' ', comment).strip()
    return comment


def praw_comment_to_dict(comm):
    author_name = None if comm.author is None else comm.author.name
    dikt = {
        'id': comm.id,
        'author': author_name,
        'controversiality': comm.controversiality,
        'score_praw': comm.score,
        'score_hidden': comm.score_hidden,
        'depth': comm.depth,
        'created_utc': comm.created_utc,
        'markdown': comm.body,
        'html': comm.body_html,
        'removed_praw': comm.body in ("[removed]", "[deleted]"),
        'praw': True
    }
    return dikt
