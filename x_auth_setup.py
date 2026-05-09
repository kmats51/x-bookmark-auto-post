"""
X(Twitter) OAuth 2.0 PKCE 初回認証セットアップスクリプト

実行方法:
    python x_auth_setup.py

事前準備:
    .env に以下を設定してください:
        X_CLIENT_ID     = <X Developer Portal の Client ID>
        X_CLIENT_SECRET = <Client Secret（Confidential Client の場合のみ）>
        X_REDIRECT_URI  = http://localhost:5000/callback  ← Developer Portal にも同じ URL を登録

実行すると:
    1. 認証 URL を表示 → ブラウザで開いて X アカウントで認可
    2. リダイレクト先 URL を自動キャプチャ
    3. トークンを x_tokens.json に保存
"""

import asyncio
import hashlib
import base64
import secrets
import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
import os

load_dotenv()

X_CLIENT_ID     = os.environ.get("X_CLIENT_ID", "")
X_CLIENT_SECRET = os.environ.get("X_CLIENT_SECRET", "")
X_REDIRECT_URI  = os.environ.get("X_REDIRECT_URI", "http://localhost:5000/callback")

TOKENS_FILE = Path(__file__).parent / "x_tokens.json"

# コールバックで受け取るグローバル変数
_callback_params: dict = {}


class CallbackHandler(BaseHTTPRequestHandler):
    """ローカルサーバーで OAuth コールバックを受け取るハンドラ。"""

    def do_GET(self):
        global _callback_params
        parsed  = urlparse(self.path)
        params  = parse_qs(parsed.query)
        _callback_params = {k: v[0] for k, v in params.items()}

        html = b"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>X Auth</title></head>
<body style="font-family:sans-serif;padding:40px;">
<h2>&#10003; 認証成功！</h2>
<p>このタブを閉じてターミナルに戻ってください。</p>
</body></html>"""
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.end_headers()
        self.wfile.write(html)

    def log_message(self, format, *args):
        pass  # ログを抑制


def start_local_server(port: int) -> HTTPServer:
    server = HTTPServer(("localhost", port), CallbackHandler)
    server.timeout = 120
    return server


async def setup_auth():
    if not X_CLIENT_ID:
        print("エラー: .env に X_CLIENT_ID を設定してください。")
        sys.exit(1)

    print("=" * 60)
    print("  X(Twitter) OAuth 2.0 PKCE 認証セットアップ")
    print("=" * 60)

    # PKCE パラメータ生成
    code_verifier = secrets.token_urlsafe(32)
    code_challenge = (
        base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        )
        .decode()
        .rstrip("=")
    )
    state = secrets.token_urlsafe(16)

    scopes = [
        "tweet.read",
        "tweet.write",
        "bookmark.read",
        "users.read",
        "offline.access",
    ]

    auth_url = (
        f"https://twitter.com/i/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={X_CLIENT_ID}"
        f"&redirect_uri={X_REDIRECT_URI}"
        f"&scope={'+'.join(scopes)}"
        f"&state={state}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
    )

    print(f"\n以下の URL をブラウザで開いてください:\n\n{auth_url}\n")
    print("X アカウントでログイン → アプリを「許可」してください。")

    # コールバック URL のポートを取得
    parsed_redirect = urlparse(X_REDIRECT_URI)
    port = parsed_redirect.port or 5000

    print(f"\nリダイレクト URL ({X_REDIRECT_URI}) の受信を待機中...\n")

    # ローカルサーバーでコールバックを待つ（同期処理をスレッドで実行）
    server = start_local_server(port)
    loop   = asyncio.get_event_loop()

    def wait_for_callback():
        while not _callback_params:
            server.handle_request()

    await loop.run_in_executor(None, wait_for_callback)
    server.server_close()

    if "error" in _callback_params:
        print(f"認証エラー: {_callback_params}")
        sys.exit(1)

    if _callback_params.get("state") != state:
        print("エラー: state が一致しません（CSRF の可能性）")
        sys.exit(1)

    code = _callback_params.get("code")
    if not code:
        print("エラー: 認可コードが取得できませんでした。")
        sys.exit(1)

    print("認可コード取得完了。アクセストークンを取得中...")

    # コードをトークンに交換
    import httpx

    post_data = {
        "grant_type":    "authorization_code",
        "client_id":     X_CLIENT_ID,
        "redirect_uri":  X_REDIRECT_URI,
        "code":          code,
        "code_verifier": code_verifier,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.twitter.com/2/oauth2/token",
            data=post_data,
            auth=(X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    token_data = resp.json()
    if "access_token" not in token_data:
        print(f"トークン取得失敗: {token_data}")
        sys.exit(1)

    # ユーザー情報を取得
    access_token = token_data["access_token"]
    async with httpx.AsyncClient() as client:
        me_resp = await client.get(
            "https://api.twitter.com/2/users/me",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"user.fields": "id,username,name"},
        )

    me_data = me_resp.json()
    user_id  = me_data.get("data", {}).get("id", "")
    username = me_data.get("data", {}).get("username", "")
    name     = me_data.get("data", {}).get("name", "")

    # トークンを保存
    import datetime

    tokens = {
        "access_token":  access_token,
        "refresh_token": token_data.get("refresh_token", ""),
        "expires_at":    (
            datetime.datetime.now(datetime.timezone.utc).timestamp()
            + token_data.get("expires_in", 7200)
        ),
        "user_id":   user_id,
        "username":  username,
        "name":      name,
        "scope":     token_data.get("scope", ""),
    }
    TOKENS_FILE.write_text(
        json.dumps(tokens, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\n✅ 認証完了！")
    print(f"   アカウント : @{username} ({name})")
    print(f"   ユーザー ID: {user_id}")
    print(f"   保存先     : {TOKENS_FILE}")
    print("\nx_bookmark_poster.py による自動投稿が有効になりました。")


if __name__ == "__main__":
    asyncio.run(setup_auth())
