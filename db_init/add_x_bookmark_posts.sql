-- x_bookmark_posts テーブルのマイグレーション
-- 実行方法: psql -U sv_admin -d sv_portal_db -f db_init/add_x_bookmark_posts.sql

CREATE TABLE IF NOT EXISTS x_bookmark_posts (
    id               SERIAL PRIMARY KEY,
    tweet_id         VARCHAR(50)  UNIQUE NOT NULL,
    tweet_text       TEXT,
    author_username  VARCHAR(255),
    tweet_url        VARCHAR(500),
    commentary       TEXT,
    post_tweet_id    VARCHAR(50),
    status           VARCHAR(50)  DEFAULT 'posted',
    processed_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_x_bookmark_posts_tweet_id
    ON x_bookmark_posts (tweet_id);

CREATE INDEX IF NOT EXISTS idx_x_bookmark_posts_processed_at
    ON x_bookmark_posts (processed_at DESC);
