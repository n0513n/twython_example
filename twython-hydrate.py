#!/usr/bin/env python3

import json
import logging as log
from argparse import ArgumentParser
from os import environ
from os.path import basename, splitext
from time import sleep, time

from twython import (Twython,
                     TwythonAuthError,
                     TwythonRateLimitError)

log.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=log.INFO,
)

TWITTER_APP_KEY = environ.get("TWITTER_APP_KEY")
TWITTER_APP_SECRET = environ.get("TWITTER_APP_SECRET")

RETRY_INTERVAL = 30


class Twitter():

    def __init__(self, app_key=TWITTER_APP_KEY, app_secret=TWITTER_APP_SECRET):
        self.__app_key = app_key
        self.__app_secret = app_secret

    def dehydrate(self, input_name, output_name=None, interval=0, json_key=None, tweet_mode=None):
        '''
        Reads tweet IDs from text or JSON file and dump to output.
        '''
        if not output_name:
            output_name = f"{splitext(basename(input_name))[0]}_{json_key}.txt"
        with open(input_name, "r") as input_file:
            with open(output_name, "w") as output_file:
                while True:
                    try:
                        line = input_file.readline().strip()
                        if line:
                            output_file.write(f"{json.loads(line)[json_key]}\n")
                            continue
                        break
                    except Exception as e:
                        log.warning(e)

    def hydrate(self, input_name, output_name=None, interval=0, json_key=None, tweet_mode=None):
        '''
        Request data from tweet IDs and dump to output.
        '''
        def authenticate(app_key, app_secret):
            twitter = Twython(app_key, app_secret, oauth_version=2)
            try:
                twitter = Twython(app_key, access_token=twitter.obtain_access_token())
            except TwythonAuthError as e:
                log.error(f"Authentication error: {e}")
                raise SystemExit
            return twitter

        def get_app_rate_limit_status(resource="statuses", endpoint="/statuses/lookup"):
            rate_limit_status = twitter.get_application_rate_limit_status(resources=resource)
            return rate_limit_status["resources"][resource][endpoint]

        def read_batch(input_file, batchsize=100, key="id"):
            batch = []
            for _ in range(batchsize):
                try:
                    line = input_file.readline().strip()
                    if line:
                        batch.append(int(json.loads(line)[json_key] if json_key is not None else line))
                except Exception as e:
                    log.warning(e)
            return batch

        total = 0
        captured = 0
        time_to_print = time()

        if not output_name:
            output_name = f"{splitext(basename(input_name))[0]}_full.json"
        errors_name = f"{splitext(basename(input_name))[0]}_errors.txt"

        twitter = authenticate(self.__app_key, self.__app_secret)

        with open(input_name, "r") as input_file:
            with open(output_name, "w") as output_file:
                with open(errors_name, "w") as errors_file:
                    while True:
                        tts = RETRY_INTERVAL
                        batch = read_batch(input_file)
                        total += len(batch)

                        while batch:
                            try:
                                response = twitter.lookup_status(id=batch, tweet_mode=tweet_mode)

                                for status in response:
                                    json.dump(status, output_file, sort_keys=True)
                                    output_file.write('\n')
                                    batch.remove(status["id"])
                                    captured += 1

                                if len(batch) > 0:
                                    for status in batch:
                                        errors_file.write(f"{status}\n")

                                break

                            except TwythonRateLimitError:
                                rate_limit_status = get_app_rate_limit_status()
                                log.info(f"Requests left: {rate_limit_status['remaining']}/{rate_limit_status['limit']}.")
                                tts = rate_limit_status['reset'] - time() + 1

                            except Exception as e:
                                log.error(e)

                            log.info(f"Sleeping for {tts} seconds...")

                        if batch == []:
                            break

                        if (time() - time_to_print) > 10:
                            log.info(f"Captured {captured}/{total} tweets.")
                            time_to_print = time()

                        sleep(interval) if float(interval) > 0 else None

        log.info(f"Captured {captured}/{total} total tweets.")


def args() -> dict:
    argparser = ArgumentParser()

    argparser.add_argument("input_name",
                           help="Required text or JSON file path")

    argparser.add_argument("-o", "--output-name",
                           dest="output_name",
                           help="Write returned data to JSON file")

    argparser.add_argument("-j", "--json-key",
                           action="store",
                           help='Specify field to read from JSON records (default: "id")')

    argparser.add_argument("-d", "--dehydrate",
                           action="store_const",
                           const="dehydrate",
                           default="hydrate",
                           dest="method",
                           help='Extract JSON values from specified key')

    argparser.add_argument("-e", "--extended",
                           action="store_const",
                           const="extended",
                           default=None,
                           dest="tweet_mode",
                           help='Set tweet mode as "extended"')

    argparser.add_argument("--interval",
                           action="store",
                           default=1,
                           help='Interval inbetween requests',
                           type=float)

    return vars(argparser.parse_args())


def main(**args) -> None:
    twitter = Twitter()
    getattr(twitter, args.pop("method", "hydrate"))(**args)


if __name__ == "__main__":
    main(**args())
