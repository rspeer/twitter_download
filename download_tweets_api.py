import sys
import os
import re
import time
import datetime
import argparse
import ftfy
from ftfy.fixes import decode_escapes

from twitter import *

parser = argparse.ArgumentParser(description="downloads tweets")
parser.add_argument('--partial', dest='partial', default=None, type=argparse.FileType('r'))
parser.add_argument('--dist', dest='dist', default=None, type=argparse.FileType('r', encoding='utf-8'), required=True)
parser.add_argument('--output', dest='output', default=None, type=argparse.FileType('w', encoding='utf-8'), required=True)

args = parser.parse_args()

CONSUMER_KEY='JEdRRoDsfwzCtupkir4ivQ'
CONSUMER_SECRET='PAbSSmzQxbcnkYYH2vQpKVSq2yPARfKm0Yl6DrLc'

MY_TWITTER_CREDS = os.path.expanduser('~/.my_app_credentials')
if not os.path.exists(MY_TWITTER_CREDS):
    oauth_dance("Semeval sentiment analysis", CONSUMER_KEY, CONSUMER_SECRET, MY_TWITTER_CREDS)
oauth_token, oauth_secret = read_token_file(MY_TWITTER_CREDS)
t = Twitter(auth=OAuth(oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET))

cache = {}
if args.partial != None:
    for line in args.partial:
        fields = line.strip().split("\t")
        text = fields[3]
        sid = fields[0]
        cache[sid] = text

for line in args.dist:
    fields = line.strip().split('\t')
    if len(fields) == 3:
        sid, uid, label = fields
        text = None
    else:
        sid, uid, label, text = fields[:4]

    if uid.startswith('T'):
        # Only try to download things from Twitter that are actually tweets
        while not sid in cache:
            try:
                text = t.statuses.show(_id=sid)['text'].replace('\n', ' ').replace('\r', ' ')
                cache[sid] = text
            except TwitterError as e:
                if e.e.code == 429:
                    rate = t.application.rate_limit_status()
                    reset = rate['resources']['statuses']['/statuses/show/:id']['reset']
                    now = datetime.datetime.today()
                    future = datetime.datetime.fromtimestamp(reset)
                    seconds = (future-now).seconds+1
                    if seconds < 10000:
                        sys.stderr.write("Rate limit exceeded, sleeping for %s seconds until %s\n" % (seconds, future))
                        time.sleep(seconds)
                else:
                    cache[sid] = 'Not Available'
        text = cache[sid]
    else:
        assert text is not None, "Non-Twitter lines must contain text"

        # Some non-Twitter messages were written with messy Pythonesque
        # Unicode escapes, such as "I\u2019ll come after lunch". Fixing these
        # correctly is subtle (the unicode-escape codec is NOT sufficient),
        # but this function from ftfy does much of the job:
        text = decode_escapes(text)

        # Now we need to remove remaining unnecessary backslashes, and the
        # weird labels in the LJ entries:
        text = re.sub(r'\\(.)', r'\1', text).replace(
            'LLLINKKK', ''
        ).replace(
            'IIIMAGEEE', ''
        )

    # Apply ftfy's automatic fixes to the text we received.
    # Some cases where this helps:
    #
    # - Decoding HTML escapes that we were supposed to interpret from Twitter,
    #   such as &amp; -> &
    # - Fixing some of the encoding snafus in the SMS messages, such as
    #   "when are Ã¼ going to book?" -> "when are ü going to book?"
    text = ftfy.fix_text(text)

    args.output.write("\t".join([sid, uid, label, text]) + '\n')

