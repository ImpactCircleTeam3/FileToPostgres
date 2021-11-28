import psycopg2
import os
import json
import ast
import csv
from psycopg2.extras import execute_values
from typing import Optional, List
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


class Settings:
    BASE_DIR = os.path.dirname(os.path.realpath(__file__))

    @staticmethod
    def get_db_kwargs() -> dict:
        return {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "database": os.getenv("DB_NAME")
        }


class DB:
    DB: Optional["DB"] = None

    @classmethod
    def get_instance(cls) -> "DB":
        if not cls.DB:
            cls.DB = cls()
        return cls.DB

    def __init__(self):
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**Settings.get_db_kwargs())
        self.cur = self.conn.cursor()
        self.conn.autocommit = True


@dataclass
class Sink:
    status_id: int
    text: str
    url: str
    favorite_count: int
    retweet_count: int
    trend: str
    normalized_trend: str
    language_code: str
    hashtags: List[str]
    tagged_persons: List[str]
    time_collected: datetime
    date_label: datetime


@dataclass
class Source:
    index: str
    status_id: str
    status_url: str
    tweet_query: str
    text: str
    favorite_count: str
    retweet_count: str
    trend: str
    normalized_trend: str
    time_collected: str
    date_label: str
    language_code: str
    hashtags: str
    tagged_persons: str

    def parse_to_sink(self) -> Sink:
        return Sink(
            status_id=json.loads(self.status_id),
            text=self.text,
            url=self.status_url,
            favorite_count=int(self.favorite_count),
            retweet_count=int(self.retweet_count),
            trend=self.trend,
            normalized_trend=self.normalized_trend,
            language_code=self.language_code,
            hashtags=[] if self.hashtags == "" else ast.literal_eval(self.hashtags),
            tagged_persons=[] if self.tagged_persons == "" else ast.literal_eval(self.tagged_persons),
            time_collected=datetime.strptime(self.time_collected, "%Y-%m-%dT%H:%M:%SZ"),
            date_label=datetime.strptime(self.date_label, "%b %d %Y")
        )


def extract_and_transform_source() -> List[Sink]:
    data_path = os.path.join(Settings.BASE_DIR, "data", "greener_tweets_cleaned.csv")
    data = []
    with open(data_path, mode="r") as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        for row in reader:
            source = Source(*row)
            data.append(source.parse_to_sink())
    return data


def write_to_postgres(tweets: List[Sink]):
    db = DB.get_instance().DB
    sql = f"""
        INSERT INTO tweet ({','.join(Sink.__annotations__.keys())})  VALUES %s
        ON CONFLICT (status_id) DO NOTHING
    """
    execute_values(db.cur, sql, [tuple(tweet.__dict__.values()) for tweet in tweets])


if __name__ == "__main__":
    tweets = extract_and_transform_source()
    for i in range(0, len(tweets) - 500, 500):
        write_to_postgres(tweets[i: i+500])
