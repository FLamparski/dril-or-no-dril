from pathlib import Path
from argparse import ArgumentParser
import sqlite3

import tweepy

import secrets

parser = ArgumentParser(description='Downloads a bunch of tweets')
parser.add_argument('account', type=str, help='the account to scrape')
parser.add_argument('-d', '--dry-run', dest='dry_run', action='store_true', default=False, help='do not hit the twitter api')
parser.add_argument('-r', '--resume', dest='resume', action='store_true', default=False, help='try to resume a scrape')
parser.add_argument('--db', dest='db', type=str, default='./tweets.db', help='what database to use (sqlite3 .db file)')

def create_tweets_table_if_not_exists(db):
    c = db.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS tweets (
        status_id INTEGER PRIMARY KEY,
        timestamp DATE,
        user TEXT,
        text TEXT
    )""")
    db.commit()

def get_min_id_for_user(db: sqlite3.Connection, user: str):
    params = (user,)
    c = db.cursor()
    c.execute('SELECT min(status_id) FROM tweets WHERE user = ?', params)
    res = c.fetchone()
    return res[0]

def get_twitter_api():
    auth = tweepy.OAuthHandler(secrets.TW_API_KEY, secrets.TW_API_SECRET)
    auth.set_access_token(secrets.TW_TOKEN, secrets.TW_SECRET)
    return tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def status_to_tuple(status):
    status_id = status.id
    timestamp = status.created_at
    user = status.author.screen_name
    text = status.text
    return status_id, timestamp, user, text

def save_tweet(db: sqlite3.Connection, tweet):
    c = db.cursor()
    c.execute("INSERT OR IGNORE INTO tweets (status_id, timestamp, user, text) VALUES(?, ?, ?, ?)", tweet)
    db.commit()

if __name__ == "__main__":
    args = parser.parse_args()
    if args.dry_run:
        print("** Dry run - one tweet will be loaded and not saved")

    print("* Will collect tweets from user {}".format(args.account))

    outfile_path = Path(args.db).absolute()
    print("* Will save tweets to {}".format(outfile_path))
    db = sqlite3.connect(str(outfile_path))
    create_tweets_table_if_not_exists(db)

    max_id = None
    if args.resume:
        res = get_min_id_for_user(db, args.account)
        if res != None:
            print("* Resuming from id {}".format(res))
            max_id = res

    api = get_twitter_api()

    cursor = tweepy.Cursor(api.user_timeline, id=args.account, max_id=max_id, include_rts=False)
    count = 0
    for status in cursor.items():
        tweet = status_to_tuple(status)

        if args.dry_run:
            print(tweet)
            break
        
        save_tweet(db, tweet)
        count += 1
        print('.', end=('\n' if count % 60 == 0 else ''), flush=True)
    
    print('\n')
    print('* done: {} tweet(s)'.format(count))