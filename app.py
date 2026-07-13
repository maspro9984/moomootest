"""moomoo 日本株リアルタイム相場 テストツール（Web版）。

使い方:
    pip install -r requirements.txt
    python app.py                 # デフォルト銘柄 9984 を表示
    python app.py --codes 9984,7203,6758
    python app.py --host 127.0.0.1 --port 11111   # OpenD の接続先

その後ブラウザで http://127.0.0.1:5000 を開く。

OpenD が起動していれば実データ、そうでなければモックデータを表示する。
"""

from __future__ import annotations

import argparse
import json
import queue
import threading
from typing import List

from flask import Flask, Response, jsonify, render_template

from moomoo_client import MoomooQuoteClient

app = Flask(__name__)

# 接続された SSE クライアント（ブラウザ）へのメッセージキュー一覧
_subscribers: List["queue.Queue[str]"] = []
_subscribers_lock = threading.Lock()

client: MoomooQuoteClient | None = None


def _broadcast(quote) -> None:
    """相場更新を全 SSE クライアントに送信する。"""
    payload = json.dumps(quote.to_dict(), ensure_ascii=False)
    with _subscribers_lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


@app.route("/")
def index():
    return render_template(
        "index.html",
        codes=",".join(client.codes) if client else "",
        mode=client.mode if client else "unknown",
    )


@app.route("/api/quotes")
def api_quotes():
    """現在のスナップショットを一括取得（初期表示・ポーリング用フォールバック）。"""
    return jsonify(
        {
            "mode": client.mode if client else "unknown",
            "quotes": client.get_quotes() if client else [],
        }
    )


@app.route("/api/stream")
def api_stream():
    """Server-Sent Events でリアルタイム更新を配信する。"""

    def gen():
        q: "queue.Queue[str]" = queue.Queue(maxsize=100)
        with _subscribers_lock:
            _subscribers.append(q)
        try:
            # 接続直後に現在値を送る
            for quote in (client.get_quotes() if client else []):
                yield f"data: {json.dumps(quote, ensure_ascii=False)}\n\n"
            while True:
                try:
                    payload = q.get(timeout=15)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    # keep-alive コメント（接続維持）
                    yield ": keepalive\n\n"
        finally:
            with _subscribers_lock:
                if q in _subscribers:
                    _subscribers.remove(q)

    return Response(gen(), mimetype="text/event-stream")


def main():
    global client
    parser = argparse.ArgumentParser(description="moomoo 日本株リアルタイム相場 Web ツール")
    parser.add_argument(
        "--codes",
        default="9984",
        help="銘柄コード（カンマ区切り）。例: 9984,7203,6758",
    )
    parser.add_argument("--host", default="127.0.0.1", help="OpenD のホスト")
    parser.add_argument("--port", type=int, default=11111, help="OpenD のポート")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web サーバーのホスト")
    parser.add_argument("--web-port", type=int, default=5000, help="Web サーバーのポート")
    args = parser.parse_args()

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    client = MoomooQuoteClient(
        codes=codes,
        host=args.host,
        port=args.port,
        on_update=_broadcast,
    )
    client.start()

    print(f"=> ブラウザで http://{args.web_host}:{args.web_port} を開いてください")
    app.run(host=args.web_host, port=args.web_port, threaded=True, debug=False)


if __name__ == "__main__":
    main()
