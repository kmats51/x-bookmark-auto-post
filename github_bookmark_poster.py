"""
Xブックマーク自動引用投稿（GitHub Actions版）

Playwrightを使わず、X API v2 と tweepy のみで動作する。

動作フロー:
1. x_tokens.json から OAuth 2.0 トークンを読み込み（期限切れなら自動リフレッシュ）
2. X API v2 GET /users/:id/bookmarks でブックマーク一覧を取得
3. processed_tweets.json で未処理のブックマークを特定（最大 MAX_POSTS_PER_RUN 件）
4. Gemini API で引用コメントを生成
5. tweepy（OAuth 1.0a）で引用ツイートとして投稿
6. x_tokens.json と processed_tweets.json を更新（GitHub Actionsがcommit・pushする）
"""

import os
import json
import asyncio
import httpx
import tweepy
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ── 設定 ──────────────────────────────────────────────
X_CLIENT_ID       = os.environ.get("X_CLIENT_ID", "")
X_CLIENT_SECRET   = os.environ.get("X_CLIENT_SECRET", "")
X_API_KEY         = os.environ.get("X_API_KEY", "")
X_API_SECRET      = os.environ.get("X_API_SECRET", "")
X_ACCESS_TOKEN    = os.environ.get("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET   = os.environ.get("X_ACCESS_SECRET", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL      = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

TOKENS_FILE    = BASE_DIR / "x_tokens.json"
PROCESSED_FILE = BASE_DIR / "processed_tweets.json"
MAX_POSTS_PER_RUN = 3

X_API_BASE  = "https://api.twitter.com/2"
X_TOKEN_URL = "https://api.twitter.com/2/oauth2/token"


# ── OAuth 2.0 トークン管理 ────────────────────────────
def load_tokens() -> dict:
    if not TOKENS_FILE.exists():
        raise RuntimeError(f"{TOKENS_FILE} が見つかりません。")
    return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))


def save_tokens(tokens: dict) -> None:
    TOKENS_FILE.write_text(json.dumps(tokens, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[bookmark] x_tokens.json を更新しました")


async def refresh_access_token(tokens: dict) -> dict:
    """リフレッシュトークンでアクセストークンを更新する"""
    if not tokens.get("refresh_token"):
        raise RuntimeError("refresh_token がありません。ローカルで x_auth_setup.py を実行してください。")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            X_TOKEN_URL,
            data={
                "grant_type":    "refresh_token",
                "client_id":     X_CLIENT_ID,
                "refresh_token": tokens["refresh_token"],
            },
            auth=(X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"トークンリフレッシュ失敗: {data}")

    tokens["access_token"] = data["access_token"]
    if "refresh_token" in data:
        tokens["refresh_token"] = data["refresh_token"]
    tokens["expires_at"] = datetime.now(timezone.utc).timestamp() + data.get("expires_in", 7200)
    save_tokens(tokens)
    print("[bookmark] アクセストークンをリフレッシュしました")
    return tokens


async def get_valid_tokens() -> dict:
    """有効なトークンを返す（期限切れなら自動リフレッシュ）"""
    tokens = load_tokens()
    expires_at = tokens.get("expires_at", 0)
    if datetime.now(timezone.utc).timestamp() > expires_at - 300:
        tokens = await refresh_access_token(tokens)
    return tokens


# ── ブックマーク取得（X API v2）────────────────────────
async def fetch_bookmarks_api(access_token: str, user_id: str, limit: int = 20) -> list:
    """X API v2 GET /users/:id/bookmarks でブックマーク一覧を取得する"""
    url = f"{X_API_BASE}/users/{user_id}/bookmarks"
    params = {
        "max_results": min(limit, 100),
        "tweet.fields": "id,text,author_id,created_at",
        "expansions": "author_id",
        "user.fields": "username,name",
    }
    headers = {"Authorization": f"Bearer {access_token}"}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, headers=headers, params=params)

    if resp.status_code == 401:
        raise RuntimeError("OAuth 2.0 アクセストークンが無効です（401）。")
    if resp.status_code != 200:
        raise RuntimeError(f"ブックマーク取得失敗 ({resp.status_code}): {resp.text}")

    data = resp.json()
    tweets = data.get("data", [])
    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}

    bookmarks = []
    for tweet in tweets:
        author_id = tweet.get("author_id", "")
        author = users.get(author_id, {})
        bookmarks.append({
            "id": tweet["id"],
            "text": tweet["text"],
            "author_username": author.get("username", "unknown"),
            "author_name": author.get("name", ""),
        })

    print(f"[bookmark] ブックマーク {len(bookmarks)} 件取得")
    return bookmarks


# ── 引用ツイート投稿（tweepy OAuth 1.0a）───────────────
def post_quote_tweet(commentary: str, quote_tweet_id: str) -> str:
    """tweepy OAuth 1.0a で引用ツイートを投稿する"""
    client = tweepy.Client(
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_SECRET,
    )
    response = client.create_tweet(
        text=commentary,
        quote_tweet_id=int(quote_tweet_id),
    )
    return str(response.data["id"])


# ── Gemini コメント生成 ──────────────────────────────
async def generate_commentary(tweet_text: str, author_name: str, author_username: str) -> str:
    """Gemini API でブックマークへの解説コメントを生成する（220文字以内）"""
    prompt = f"""以下のX(Twitter)の投稿をブックマークしました。この投稿への短いコメントを生成してください。

【投稿者】{author_name} (@{author_username})
【投稿内容】
{tweet_text}

【要件】
- 日本語でコメントしてください
- 投稿の要点や気づき・解説を自分の言葉で簡潔に書いてください
- 220文字以内に収めてください
- プロフェッショナルが専門的な知見から分かりやすく解説する、落ち着いた知的なトーンにしてください
- くだけすぎた表現（「〜だね！」「〜かな？」など）は避け、丁寧ながらも自信を感じさせる口調にしてください
- 絵文字は多用せず、最小限（または無し）にしてください
- 「この投稿は〜」のような言い回しは避けてください"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY}
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 400},
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=body, headers=headers)

    data = resp.json()
    if resp.status_code != 200:
        raise RuntimeError(f"Gemini API エラー: {data}")

    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    if len(text) > 220:
        text = text[:219] + "…"
    return text


# ── 処理済みDB ──────────────────────────────────────
def get_processed_ids() -> set:
    if not PROCESSED_FILE.exists():
        return set()
    try:
        return set(json.loads(PROCESSED_FILE.read_text(encoding="utf-8")).keys())
    except Exception:
        return set()


def save_processed(tweet_id: str, tweet_text: str, author_username: str,
                   tweet_url: str, commentary: str, post_tweet_id: str) -> None:
    data = {}
    if PROCESSED_FILE.exists():
        data = json.loads(PROCESSED_FILE.read_text(encoding="utf-8"))
    data[tweet_id] = {
        "tweet_text":      tweet_text,
        "author_username": author_username,
        "tweet_url":       tweet_url,
        "commentary":      commentary,
        "post_tweet_id":   post_tweet_id,
        "processed_at":    datetime.now(timezone.utc).isoformat(),
    }
    PROCESSED_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Slack 通知 ──────────────────────────────────────
def send_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        import urllib.request
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps({"text": message}).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[bookmark] Slack通知エラー: {e}")


# ── メイン処理 ──────────────────────────────────────
async def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[bookmark] {now} 処理開始")

    # 1. トークン取得（期限切れなら自動リフレッシュ）
    try:
        tokens = await get_valid_tokens()
    except RuntimeError as e:
        print(f"[bookmark] 認証エラー: {e}")
        send_slack(f"🔴 【Xブックマーク投稿エラー】認証エラー: {e}")
        return

    access_token = tokens["access_token"]
    user_id      = tokens.get("user_id", "")

    if not user_id:
        print("[bookmark] エラー: user_id が x_tokens.json にありません")
        return

    # 2. ブックマーク取得（X API v2）
    try:
        bookmarks = await fetch_bookmarks_api(access_token, user_id, limit=20)
    except RuntimeError as e:
        print(f"[bookmark] ブックマーク取得エラー: {e}")
        send_slack(f"🔴 【Xブックマーク投稿エラー】ブックマーク取得失敗: {e}")
        return

    # 3. 未処理を特定
    processed_ids = get_processed_ids()
    new_bookmarks = [b for b in bookmarks if b["id"] not in processed_ids]

    if not new_bookmarks:
        print("[bookmark] 新しいブックマークなし。終了。")
        return

    to_process = new_bookmarks[:MAX_POSTS_PER_RUN]
    posted = []
    errors = []

    for tweet in to_process:
        tweet_id        = tweet["id"]
        tweet_text      = tweet["text"]
        author_username = tweet.get("author_username", "unknown")
        author_name     = tweet.get("author_name", "")
        tweet_url       = f"https://x.com/{author_username}/status/{tweet_id}"

        try:
            # 4. コメント生成
            commentary = await generate_commentary(tweet_text, author_name, author_username)
            print(f"[bookmark] コメント生成完了: {commentary[:60]}…")

            # 5. 引用ツイート投稿（tweepy OAuth 1.0a）
            post_id = post_quote_tweet(commentary, tweet_id)
            print(f"[bookmark] 投稿完了: post_id={post_id}")

            # 6. 処理済みに記録
            save_processed(tweet_id, tweet_text, author_username, tweet_url, commentary, post_id)

            # 7. Slack通知
            send_slack(
                f"🔖 【Xブックマーク自動投稿】\n"
                f"元ツイート: {tweet_url}\n"
                f"コメント: {commentary}"
            )
            posted.append(post_id)
            await asyncio.sleep(2)

        except Exception as e:
            msg = f"tweet_id={tweet_id} の処理失敗: {e}"
            print(f"[bookmark] エラー: {msg}")
            send_slack(f"⚠️ 【Xブックマーク投稿エラー】{msg}")
            errors.append(msg)
            continue

    print(f"[bookmark] 完了: {len(posted)}件投稿 / {len(errors)}件エラー")


if __name__ == "__main__":
    asyncio.run(main())
