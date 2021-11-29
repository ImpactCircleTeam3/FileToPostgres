CREATE TABLE IF NOT EXISTS tweet (
    id SERIAL,
    status_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    url VARCHAR(2048),
    favorite_count INTEGER,
    retweet_count INTEGER,
    trend VARCHAR(80),
    normalized_trend VARCHAR(80),
    language_code VARCHAR(10),
    author VARCHAR(120),
    hashtags TEXT[],
    tagged_persons TEXT[],
    date_label DATE,
    time_collected TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (status_id)
);