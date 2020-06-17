import logging
import pathlib
import time
import json
from argparse import ArgumentParser
import threading
import multiprocessing.dummy

from tqdm import tqdm
import nltk
import pandas as pd
from profanity_filter import ProfanityFilter
import praw

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.cloud import language

import pyrugby.reddit


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(name)s | %(levelname)7s | %(message)s",
    "%Y-%m-%d %H:%M:%S"
)
ch.setFormatter(formatter)
log.addHandler(ch)


# Initialise the tqdm pandas instance
tqdm.pandas()


def get_parser():
    parser = ArgumentParser(
        description=(
            'Scrape comments from /r/rugbyunion Match Threads'
            ' and perform sentiment analysis'
        )
    )
    subparsers = parser.add_subparsers(
        title='Sub-commands', dest='command'
    )
    scraper = subparsers.add_parser('scrape')
    scraper.add_argument(
        '-u', '--url', action='store_true',
        help='ID is a Reddit Submission URL'
    )
    scraper.add_argument(
        '-o', '--outdir',
        help='Optional output directory for final CSV '
        '(Defaults to current working directory'
    )
    scraper.add_argument('subid', help='URL or Submission ID')

    processer = subparsers.add_parser('process')
    processer.add_argument('input', help='CSV of comments from "scrape"')
    processer.add_argument(
        'update', choices=['google', 'vader', 'profanity', 'flair'],
        nargs='+', help='Which fields to add/update'
    )
    processer.add_argument(
        '-p', '--profanities',
        help='A JSON file containing profanities indexed by their "root"'
    )
    return parser


def get_vader_sentiment(comment, analyzer):
    vs = analyzer.polarity_scores(comment)
    return vs


def comment_list_to_pandas(comms, id_col='id'):
    df = pd.DataFrame(comms)
    df.set_index(id_col, inplace=True)
    df = df.loc[~df.index.duplicated(keep='first')]
    return df


def get_profanities(words, custom_profanities=None):
    pf = ProfanityFilter()
    if custom_profanities is not None:
        pf.custom_profane_word_dictionaries = {
            'en': custom_profanities
        }
    swears = []
    for w in words:
        cw = pf.censor_word(w)
        if cw.is_profane:
            swears.append(cw.original_profane_word)
    return swears


# Class to handle mutliple Google Natural Language Requests
# Implements rate-limiting and error handling
class GoogleNaturalLanguageBatch():
    def __init__(self, threads=250, limit=500, every=60):
        self.nthreads = threads
        self.ratelimit = threading.BoundedSemaphore(limit)
        self.every = every
        self.log = logging.getLogger(__name__).getChild(self.__class__.__name__).getChild(str(id(self)))

    def _limited(func):
        def _limited_wrapper(self, *args, **kwargs):
            self.ratelimit.acquire()
            t = threading.Timer(self.every, self.ratelimit.release)
            t.start()
            return func(self, *args, **kwargs)
        return _limited_wrapper

    @_limited
    def _analyze_sentiment(
        self,
        text,
        text_id=None,
        doctype=language.enums.Document.Type.PLAIN_TEXT,
        encoding=language.enums.EncodingType.UTF8,
        doc_language='en'
    ):
        client = language.LanguageServiceClient()
        document = {
            "content": text,
            "type": doctype,
            "language": doc_language,
        }
        sent = client.analyze_sentiment(
            document,
            encoding_type=encoding
        )
        # Must do this to clean up connections and
        # avoid "Too Many Open Files" error
        # https://github.com/googleapis/google-cloud-python/issues/5523
        client.transport.channel.close()
        return (text_id, sent)

    def analyze_sentiment(self, docs):
        with multiprocessing.dummy.Pool(self.nthreads) as tpool:
            results = tpool.starmap_async(self._analyze_sentiment, docs)
            self.track_results(results)
            return results.get()

    def track_results(self, task, interval=60):
        while task._number_left > 0:
            self.log.info(
                "Tasks remaining = %d",
                (task._number_left * task._chunksize)
            )
            time.sleep(interval)


def scrape_and_clean(subid, url=False, outdir=''):
    sub_id = subid

    log.info("Creating Reddit instance")
    # Settings for this 'bot' in praw.ini
    reddit = praw.Reddit("rugby-union-comment-scraper")

    if url:
        log.info("Fetching PRAW submission by url: %s", sub_id)
        submission = reddit.submission(url=sub_id)
        sub_id = submission.id
        log.info("Submission ID determined as: %s", sub_id)
    else:
        log.info("Fetching PRAW submission by id: %s", sub_id)
        submission = reddit.submission(sub_id)

    # Get all comments for submission from Pushshift
    log.info("Fetching comments from Pushshift")
    start = time.time()
    pushshift_comms = pyrugby.reddit.get_all_pushshift_comments(sub_id)
    end = time.time()
    log.info(
        "Pushshift: Fetched %d comments in %d seconds",
        len(pushshift_comms), end-start
    )
    log.info("Processing Pushshift comment flair")
    for comment in pushshift_comms:
        fid = pyrugby.reddit.get_flair_identifier(comment)
        comment['flair_id'] = fid
        if fid not in pyrugby.reddit.FLAIRS:
            log.warning(
                'Unrecognised flair found! | %s / %s / %s',
                comment["id"], comment.get("author_flair_css_class"),
                comment.get("author_flair_richtext")
            )

    log.info("Fetching PRAW comments - approx %d", submission.num_comments)
    start = time.time()
    submission.comments.replace_more(limit=None)
    end = time.time()
    log.info(
        "PRAW Fetched %d comments in %d seconds",
        len(submission.comments.list()), end-start
    )

    praw_comms = []
    log.info("Processing PRAW comments")
    for comment in submission.comments.list():
        p_cmnt = pyrugby.reddit.praw_comment_to_dict(comment)
        praw_comms.append(p_cmnt)

    push_comms = comment_list_to_pandas(pushshift_comms)
    praw_comms = comment_list_to_pandas(praw_comms)

    log.info("Merging comment sets")
    all_comms = push_comms.join(
        praw_comms, how='outer', rsuffix='_praw'
    )

    # Get plaintext comment using pushshift comment body
    log.info("Converting comment to plaintext")
    all_comms['plaintext'] = all_comms.body.apply(
        pyrugby.reddit.comment_md_to_plaintext
    )

    csvname = pathlib.Path(outdir, f"{sub_id}_cleaned.csv")
    log.info("Saving comments to CSV %s", csvname)
    all_comms.to_csv(csvname)


def add_vader_sentiment(df):
    log.info("Calculating VADER comment sentiment")
    vader = SentimentIntensityAnalyzer()
    df['vader_score'] = df.body.progress_apply(
        get_vader_sentiment, analyzer=vader
    ).apply(
        lambda x: x['compound']
    )


def add_google_sentiment(df):
    log.info("Fetching Google NLP sentiment")
    start = time.time()
    google_nlp = GoogleNaturalLanguageBatch()
    google_scores = google_nlp.analyze_sentiment(
        list(zip(df.plaintext, df.id))
    )
    end = time.time()
    log.info("All scores fetched in %d seconds", end-start)
    google_df = pd.DataFrame(google_scores, columns=('id', 'sent'))
    google_df.set_index('id', inplace=True)
    df.drop(
        columns=('google_score', 'google_magnitude'),
        errors='ignore', inplace=True
    )
    df = df.join(google_df.sent.apply(
        lambda x: pd.Series(
            (x.document_sentiment.score, x.document_sentiment.magnitude),
            index=('google_score', 'google_magnitude')
        )
    ))


def add_profanities(df, profanity_json=None):
    # Set up the custom profanity dicts if needed
    custom_profanities = None
    profane_word_roots = {}
    if profanity_json is not None:
        with open(profanity_json, 'r') as pjson:
            _custom_profanities = json.load(pjson)
        custom_profanities = {
            w for sublist
            in _custom_profanities.values()
            for w in sublist
        }
        profane_word_roots = {
            w: root for root, words
            in _custom_profanities.items()
            for w in words
        }
    log.info("Tokenizing comments")
    df['words'] = df.plaintext.progress_apply(nltk.word_tokenize)
    log.info("Detecting swear words")
    df['swears'] = df.words.progress_apply(
        get_profanities, custom_profanities=custom_profanities
    )
    log.info("Calculating swear word roots")
    df['swears_root'] = df.swears.progress_apply(
        lambda x: [profane_word_roots.get(word, word) for word in x]
    )
    df['swears'] = df.swears.str.join(',')
    df['swears_root'] = df.swears_root.str.join(',')
    df['words'] = df.words.str.len()


def add_flair_info(df):
    df[['flair_country', 'flair_league', 'flair_club']] = df.flair_id.apply(
        lambda x: pd.Series(
            pyrugby.reddit.FLAIRS.get(
                x, {'country': None, 'club': None, 'league': None}
            )
        )
    )


PROCESS_FUNCMAP = {
    'google': add_google_sentiment,
    'vader': add_vader_sentiment,
    'profanity': add_profanities,
    'flair': add_flair_info
}


def main(args):
    if args.command == 'scrape':
        scrape_and_clean(args.subid, args.url, args.outdir)
    elif args.command == 'process':
        if 'profanity' in args.update and not args.profanities:
            print(
                'No profanities supplied falling back to'
                ' "profanity_filter" defaults'
            )
        incsv = pathlib.Path(args.input)
        df = pd.read_csv(incsv)
        for field in args.update:
            if field == 'profanity':
                PROCESS_FUNCMAP[field](df, args.profanities)
            else:
                PROCESS_FUNCMAP[field](df)
        df.to_csv(
            incsv.with_name(
                f'{incsv.stem}_{"_".join(args.update)}.csv'
            ),
            index=False
        )
    else:
        print('Unrecognised command!')


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    main(args)
