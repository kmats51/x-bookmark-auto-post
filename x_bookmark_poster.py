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
import httpx
from datetime import datetime, timezone
from pathlib import Path
import time
import asyncio
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import tweepy

load_dotenv()

# ── 設定 ──────────────────────────────────────────────
X_CLIENT_ID     = os.environ.get("X_CLIENT_ID", "")
X_CLIENT_SECRET = os.environ.get("X_CLIENT_SECRET", "")
X_REDIRECT_URI  = os.environ.get("X_REDIRECT_URI", "http://localhost:5000/callback")

X_API_KEY       = os.environ.get("X_API_KEY", "")
X_API_SECRET    = os.environ.get("X_API_SECRET", "")
X_ACCESS_TOKEN  = os.environ.get("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.environ.get("X_ACCESS_SECRET", "")

GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

TOKENS_FILE      = Path(__file__).parent / "x_tokens.json"
PROCESSED_FILE   = Path(__file__).parent / "processed_tweets.json"
USER_DATA_DIR    = Path(__file__).parent / "user_data"
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
    

# ── Slack 通知 ──────────────────────────────────────
def send_slack_notification(message: str) -> None:
    """Slack Incoming Webhook で通知を送信する。"""
    if not SLACK_WEBHOOK_URL:
        return
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as e:
        print(f"[x_bookmark] Slack 通知エラー: {e}")


# ── JSON 操作 (DBの代わり) ──────────────────────────────────
def get_processed_tweet_ids() -> set:
    """処理済みツイート ID のセットを JSON から取得する。"""
    if not PROCESSED_FILE.exists():
        return set()
    try:
        data = json.loads(PROCESSED_FILE.read_text(encoding="utf-8"))
        # キー（tweet_id）の集合を返す
        return set(data.keys())
    except Exception as e:
        print(f"[x_bookmark] JSON 読み込みエラー: {e}")
        return set()


def save_bookmark_post(
    tweet_id: str,
    tweet_text: str,
    author_username: str,
    tweet_url: str,
    commentary: str,
    post_tweet_id: str,
) -> None:
    """投稿記録を JSON に保存する。"""
    try:
        # 既存のデータを読み込み
        data = {}
        if PROCESSED_FILE.exists():
            data = json.loads(PROCESSED_FILE.read_text(encoding="utf-8"))
        
        # 新しいデータを追加
        data[tweet_id] = {
            "tweet_text": tweet_text,
            "author_username": author_username,
            "tweet_url": tweet_url,
            "commentary": commentary,
            "post_tweet_id": post_tweet_id,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        
        # ファイルに書き込み
        PROCESSED_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"[x_bookmark] JSON 保存エラー: {e}")


# ── X スクレイピング (Bookmark 取得用) ──────────────────
async def fetch_bookmarks_scraping(limit: int = 20) -> list:
    """Playwright を使ってブックマーク一覧を取得する。"""
    bookmarks = []
    
    async with async_playwright() as p:
        # persistent_context を使用してログイン情報を保持
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=True,  # 安定したら True に
            viewport={"width": 1280, "height": 800}
        )
        
        page = await context.new_page()
        print("[x_bookmark] ブックマークページにアクセス中...")
        await page.goto("https://x.com/i/bookmarks", wait_until="domcontentloaded", timeout=60000)
        
        # ログインチェック (ログイン画面が表示されたら中断)
        if "login" in page.url:
            await context.close()
            raise RuntimeError("X へのログインが必要です。user_data 内にセッションがありません。")

        # 少し待機して要素を読み込み
        await asyncio.sleep(5)
        
        # ツイート要素の抽出
        articles = await page.locator('article[data-testid="tweet"]').all()
        print(f"[x_bookmark] {len(articles)} 件のツイート要素を検出")
        
        for article in articles[:limit]:
            try:
                # テキスト
                text_els = article.locator('[data-testid="tweetText"]').first
                text = await text_els.inner_text() if await text_els.count() > 0 else ""
                
                # テキストが取得できなかった場合のフォールバック（リンクカードのタイトルなど）
                if not text:
                    # カードタイトルや、その他の dir="auto" の要素を探す
                    card_title_els = article.locator('[data-testid="card.layoutLarge.detail"] div').first
                    if await card_title_els.count() > 0:
                        text = await card_title_els.inner_text()
                    else:
                        # 最後の手段として、記事内の主要なテキストっぽいものを探す
                        other_text_els = article.locator('div[dir="auto"]').first
                        if await other_text_els.count() > 0:
                            text = await other_text_els.inner_text()
                
                # URL
                tweet_url = ""
                links = await article.locator('a[href*="/status/"]').all()
                for a in links:
                    href = await a.get_attribute("href") or ""
                    if "/status/" in href and "/photo/" not in href and "/video/" not in href:
                        # href は /username/status/12345 の形式
                        tweet_id = href.split("/")[-1]
                        author_username = href.split("/")[1]
                        tweet_url = f"https://x.com{href}"
                        break
                
                if text and tweet_url:
                    bookmarks.append({
                        "id": tweet_id,
                        "text": text,
                        "author_username": author_username,
                        "author_name": "", # スクレイピングでは一旦空
                    })
            except Exception as e:
                continue
        
        await context.close()
        
    return bookmarks


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


async def post_tweet_scraping(commentary: str, tweet_url: str) -> str:
    """Playwright を使ってブラウザ上で『引用ツイート』を行う。"""
    import urllib.parse
    
    # 投稿用テキストをエンコード
    encoded_text = urllib.parse.quote(commentary)
    encoded_url = urllib.parse.quote(tweet_url)
    
    post_id = f"scraped_{int(time.time())}"
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=True,
            viewport={"width": 1280, "height": 800}
        )
        page = await context.new_page()
        
        try:
            # 共有 Intent URL を使用 (これが一番確実)
            intent_url = f"https://x.com/intent/post?text={encoded_text}&url={encoded_url}"
            print(f"[x_bookmark] 引用投稿(Intent)にアクセス中...")
            await page.goto(intent_url, wait_until="domcontentloaded", timeout=60000)
            
            # 投稿ボタンの待機
            # Intent画面では [data-testid="tweetButton"] が最初からある場合が多い
            post_button = page.locator('button[data-testid="tweetButton"]').first
            await post_button.wait_for(timeout=30000)
            
            # 少し待機して X 側が URL を引用カードとして認識するのを待つ
            await asyncio.sleep(5)
            
            # 投稿
            await post_button.click()
            
            print("[x_bookmark] 引用ツイート完了を待機中...")
            await asyncio.sleep(5)
            
        except Exception as e:
            print(f"[x_bookmark] 引用投稿エラー: {e}")
            # エラー時のみデバッグ用スクショ
            await page.screenshot(path="debug_intent_error.png")
            await context.close()
            raise e
            
        await context.close()
        
    return post_id


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
- プロフェッショナルが専門的な知見から分かりやすく解説する、落ち着いた知的なトーンにしてください
- くだけすぎた表現（「〜だね！」「〜かな？」など）は避け、丁寧ながらも自信を感じさせる口調にしてください
- 絵文字は多用せず、最小限（または無し）にしてください
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

    # 2. ブックマーク取得 (スクレイピング方式)
    try:
        bookmarks = await fetch_bookmarks_scraping(limit=20)
    except Exception as e:
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
        tweet_url       = f"https://x.com/{author_username}/status/{tweet_id}"

        try:
            # 4. コメント生成
            commentary = await generate_commentary(tweet_text, author_name, author_username)
            print(f"[x_bookmark] コメント生成: {commentary[:60]}…")

            # 5. ブラウザ自動操作で投稿 (API制限を完全に回避)
            post_id = await post_tweet_scraping(commentary, tweet_url)

            # 6. DB 保存
            save_bookmark_post(
                tweet_id, tweet_text, author_username, tweet_url, commentary, post_id
            )

            # 7. Slack 通知
            msg = f"🔖 【Xブックマーク自動投稿】\n元のツイート: {tweet_url}\n解説内容: {commentary}\n投稿ステータス: 成功"
            send_slack_notification(msg)

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
            
            # エラー時も Slack に通知
            err_msg = f"⚠️ 【Xブックマーク自動投稿エラー】\n対象URL: {tweet_url}\n内容: {msg}"
            send_slack_notification(err_msg)
            
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
