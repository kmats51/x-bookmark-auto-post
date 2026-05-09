"""
X(Twitter) ブックマーク自動投稿モジュール

動作フロー:
1. x_tokens.json から OAuth 2.0 トークンを読み込み（期限切れなら自動リフレッシュ）
2. X API v2 でブックマーク一覧を取得
3. DB で未処理のブックマークを特定（最大 MAX_POSTS_PER_RUN 件）
4. Gemini API で引用コメントを生成
5. 引用ツイートとして自動投稿
6. 処理結果を DB に保存
"""

import json
import os
import asyncio
import hashlib
import base64
import secrets
import psycopg2
import httpx
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── 設定 ──────────────────────────────────────────────
X_CLIENT_ID     = os.environ.get("X_CLIENT_ID", "")
X_CLIENT_SECRET = os.environ.get("X_CLIENT_SECRET", "")
X_REDIRECT_URI  = os.environ.get("X_REDIRECT_URI", "http://localhost:5000/callback")
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

TOKENS_FILE      = Path(__file__).parent / "x_tokens.json"
MAX_POSTS_PER_RUN = 3

X_API_BASE  = "https://api.twitter.com/2"
X_TOKEN_URL = "https://api.twitter.com/2/oauth2/token"
X_AUTH_URL  = "https://twitter.com/i/oauth2/authorize"

SCOPES = ["tweet.read", "tweet.write", "bookmark.read", "users.read", "offline.access"]


# ── トークン管理 ──────────────────────────────────────
def load_tokens() -> dict:
    if not TOKENS_FILE.exists():
        return {}
    return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))


def save_tokens(tokens: dict) -> None:
    TOKENS_FILE.write_text(
        json.dumps(tokens, ensure_ascii=False, indent=2), encoding="utf-8"
    )


async def refresh_access_token() -> str:
    """リフレッシュトークンを使ってアクセストークンを更新する。"""
    tokens = load_tokens()
    if not tokens.get("refresh_token"):
        raise RuntimeError(
            "リフレッシュトークンがありません。x_auth_setup.py を実行して認証してください。"
        )

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
    tokens["expires_at"] = (
        datetime.now(timezone.utc).timestamp() + data.get("expires_in", 7200)
    )
    save_tokens(tokens)
    return tokens["access_token"]


async def get_access_token() -> str:
    """有効なアクセストークンを返す（5 分前に自動リフレッシュ）。"""
    tokens = load_tokens()
    if not tokens:
        raise RuntimeError(
            "認証情報がありません。x_auth_setup.py を実行してください。"
        )

    expires_at = tokens.get("expires_at", 0)
    if datetime.now(timezone.utc).timestamp() > expires_at - 300:
        return await refresh_access_token()

    return tokens["access_token"]


def build_auth_url(code_verifier: str, state: str) -> tuple[str, str]:
    """PKCE 認証 URL とコードチャレンジを生成して返す。"""
    code_challenge = (
        base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        )
        .decode()
        .rstrip("=")
    )
    params = (
        f"response_type=code"
        f"&client_id={X_CLIENT_ID}"
        f"&redirect_uri={X_REDIRECT_URI}"
        f"&scope={'+'.join(SCOPES)}"
        f"&state={state}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
    )
    return f"{X_AUTH_URL}?{params}", code_challenge


async def exchange_code_for_token(code: str, code_verifier: str) -> dict:
    """認可コードをアクセストークンに交換する。"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            X_TOKEN_URL,
            data={
                "grant_type":    "authorization_code",
                "client_id":     X_CLIENT_ID,
                "redirect_uri":  X_REDIRECT_URI,
                "code":          code,
                "code_verifier": code_verifier,
            },
            auth=(X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"トークン取得失敗: {data}")
    return data


# ── DB 操作 ──────────────────────────────────────────
def _db_connect():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="sv_portal_db",
        user="sv_admin",
        password="sv_password",
    )


def get_processed_tweet_ids() -> set:
    """処理済みツイート ID のセットを DB から取得する。"""
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute("SELECT tweet_id FROM x_bookmark_posts")
        ids = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return ids
    except Exception as e:
        print(f"[x_bookmark] DB 読み込みエラー: {e}")
        return set()


def save_bookmark_post(
    tweet_id: str,
    tweet_text: str,
    author_username: str,
    tweet_url: str,
    commentary: str,
    post_tweet_id: str,
) -> None:
    """投稿記録を DB に保存する。"""
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute(
            """INSERT INTO x_bookmark_posts
               (tweet_id, tweet_text, author_username, tweet_url, commentary, post_tweet_id, status)
               VALUES (%s, %s, %s, %s, %s, %s, 'posted')
               ON CONFLICT (tweet_id) DO NOTHING""",
            (tweet_id, tweet_text, author_username, tweet_url, commentary, post_tweet_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[x_bookmark] DB 保存エラー: {e}")


def get_recent_posts(limit: int = 20) -> list:
    """最近の投稿履歴を DB から取得する。"""
    try:
        conn = _db_connect()
        cur  = conn.cursor()
        cur.execute(
            """SELECT tweet_id, author_username, commentary, post_tweet_id, status, processed_at
               FROM x_bookmark_posts
               ORDER BY processed_at DESC
               LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "tweet_id":        r[0],
                "author_username": r[1],
                "commentary":      r[2],
                "post_tweet_id":   r[3],
                "status":          r[4],
                "processed_at":    r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[x_bookmark] DB 履歴取得エラー: {e}")
        return []


# ── X API 呼び出し ────────────────────────────────────
async def fetch_bookmarks(access_token: str, user_id: str) -> list:
    """X API v2 からブックマーク一覧を取得する。"""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{X_API_BASE}/users/{user_id}/bookmarks",
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "tweet.fields": "id,text,created_at,author_id",
                "expansions":   "author_id",
                "user.fields":  "username,name",
                "max_results":  100,
            },
        )

    data = resp.json()
    if resp.status_code != 200:
        raise RuntimeError(f"ブックマーク取得失敗 ({resp.status_code}): {data}")

    tweets = data.get("data", [])
    users  = {u["id"]: u for u in data.get("includes", {}).get("users", [])}

    for tweet in tweets:
        author = users.get(tweet.get("author_id", ""), {})
        tweet["author_username"] = author.get("username", "unknown")
        tweet["author_name"]     = author.get("name", "")

    return tweets


async def fetch_my_user_id(access_token: str) -> tuple[str, str]:
    """認証ユーザーの user_id と username を返す。"""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{X_API_BASE}/users/me",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"user.fields": "id,username"},
        )

    data = resp.json()
    if resp.status_code != 200:
        raise RuntimeError(f"ユーザー情報取得失敗: {data}")

    return data["data"]["id"], data["data"]["username"]


async def post_quote_tweet(access_token: str, commentary: str, quote_tweet_id: str) -> str:
    """引用ツイートを投稿してツイート ID を返す。"""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{X_API_BASE}/tweets",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type":  "application/json",
            },
            json={"text": commentary, "quote_tweet_id": quote_tweet_id},
        )

    data = resp.json()
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"ツイート投稿失敗 ({resp.status_code}): {data}")

    return data["data"]["id"]


# ── Gemini コメント生成 ──────────────────────────────
async def generate_commentary(
    tweet_text: str, author_name: str, author_username: str
) -> str:
    """Gemini API でブックマークへの解説コメントを生成する（220 文字以内）。"""
    prompt = f"""以下のX(Twitter)の投稿をブックマークしました。この投稿への短いコメントを生成してください。

【投稿者】{author_name} (@{author_username})
【投稿内容】
{tweet_text}

【要件】
- 日本語でコメントしてください
- 投稿の要点や気づき・解説を自分の言葉で簡潔に書いてください
- 220文字以内に収めてください
- 自然な口調（「〜ですね」「〜だと思います」など）で書いてください
- 絵文字を適度に使ってOKです
- 「この投稿は〜」のような言い回しは避けてください"""

    url = (
        f"https://generativelanguage.googleapis.com/v1beta"
        f"/models/{GEMINI_MODEL}:generateContent"
    )
    headers = {
        "Content-Type":    "application/json",
        "x-goog-api-key":  GEMINI_API_KEY,
    }
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


# ── メイン処理 ──────────────────────────────────────
async def run_bookmark_poster() -> dict:
    """
    ブックマーク自動投稿のメイン処理。
    APScheduler または API エンドポイントから呼び出す。
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[x_bookmark] {now} 処理開始")

    # 1. アクセストークン取得
    try:
        access_token = await get_access_token()
    except RuntimeError as e:
        print(f"[x_bookmark] 認証エラー: {e}")
        return {"status": "error", "message": str(e), "posted": 0}

    tokens  = load_tokens()
    user_id = tokens.get("user_id")

    # user_id が未保存なら API から取得して保存
    if not user_id:
        try:
            user_id, username = await fetch_my_user_id(access_token)
            tokens["user_id"]  = user_id
            tokens["username"] = username
            save_tokens(tokens)
        except RuntimeError as e:
            print(f"[x_bookmark] ユーザー情報取得失敗: {e}")
            return {"status": "error", "message": str(e), "posted": 0}

    # 2. ブックマーク取得
    try:
        bookmarks = await fetch_bookmarks(access_token, user_id)
    except RuntimeError as e:
        print(f"[x_bookmark] ブックマーク取得エラー: {e}")
        return {"status": "error", "message": str(e), "posted": 0}

    # 3. 未処理ブックマークを特定
    processed_ids = get_processed_tweet_ids()
    new_bookmarks = [b for b in bookmarks if b["id"] not in processed_ids]

    if not new_bookmarks:
        print("[x_bookmark] 新しいブックマークなし")
        return {"status": "success", "posted": 0, "message": "新しいブックマークはありませんでした"}

    to_process = new_bookmarks[:MAX_POSTS_PER_RUN]
    posted     = []
    errors     = []

    for tweet in to_process:
        tweet_id        = tweet["id"]
        tweet_text      = tweet["text"]
        author_username = tweet.get("author_username", "unknown")
        author_name     = tweet.get("author_name", "")
        tweet_url       = f"https://twitter.com/{author_username}/status/{tweet_id}"

        try:
            # 4. コメント生成
            commentary = await generate_commentary(tweet_text, author_name, author_username)
            print(f"[x_bookmark] コメント生成: {commentary[:60]}…")

            # 5. 引用ツイート投稿
            post_id = await post_quote_tweet(access_token, commentary, tweet_id)

            # 6. DB 保存
            save_bookmark_post(
                tweet_id, tweet_text, author_username, tweet_url, commentary, post_id
            )

            posted.append({
                "original_tweet_id":  tweet_id,
                "original_tweet_url": tweet_url,
                "post_tweet_id":      post_id,
                "commentary":         commentary,
            })
            print(f"[x_bookmark] 投稿完了: post_id={post_id}")

            # API レート制限対策
            await asyncio.sleep(2)

        except Exception as e:
            msg = f"tweet_id={tweet_id} の処理失敗: {e}"
            print(f"[x_bookmark] エラー: {msg}")
            errors.append(msg)
            continue

    print(f"[x_bookmark] 完了: {len(posted)} 件投稿, {len(errors)} 件エラー")
    return {
        "status":  "success",
        "posted":  len(posted),
        "errors":  errors,
        "details": posted,
    }


if __name__ == "__main__":
    # cron から直接実行する場合:
    #   python /path/to/x_bookmark_poster.py
    #
    # crontab 設定例（毎朝 8:00 JST）:
    #   0 8 * * * cd /path/to/project && /path/to/.venv/bin/python x_bookmark_poster.py >> /var/log/x_bookmark.log 2>&1
    import asyncio

    result = asyncio.run(run_bookmark_poster())
    print(result)
