"""日本株版: 前日売買代金上位N × 上場来高値(ATH)接近ランキングをWebで表示する。

データ源は RSSPilot の dataapi(WebSocket)。moomoo/OpenD は使わない。
US版(ath_app.py)とは完全に別プロセス・別ページ。

使い方:
    python jp_app.py                          # RSSPilot=192.168.1.113, Web=5002
    python jp_app.py --rss-host 192.168.1.113 --rss-port 23203
    python jp_app.py --top 100 --show 20
    python jp_app.py --web-port 5002

その後ブラウザで http://127.0.0.1:5002 を開く。
"""

from __future__ import annotations

import argparse

from flask import Flask, jsonify, render_template, request

from jp_monitor import JpAthMonitor

app = Flask(__name__)

monitor: JpAthMonitor | None = None


@app.route("/")
def index():
    return render_template(
        "jp.html",
        top=monitor.top if monitor else 0,
        show=(monitor.display_top or 0) if monitor else 0,
        view="ath",
    )


@app.route("/turnover")
def turnover_page():
    """前日売買代金 上位の静的ページ（同レイアウト、更新なし）。"""
    return render_template(
        "jp.html",
        top=monitor.top if monitor else 0,
        show=(monitor.display_top or 0) if monitor else 0,
        view="turnover",
    )


@app.route("/api/ranking")
def api_ranking():
    """ランキングを JSON で返す。?sort=turnover で前日売買代金順。"""
    if monitor is None:
        return jsonify({"mode": "unknown", "rows": []})
    sort = "turnover" if request.args.get("sort") == "turnover" else "pct"
    return jsonify({
        "mode": monitor.mode,
        "session": monitor.session,
        "universe_n": monitor.universe_n,
        "rows": monitor.get_ranking(sort=sort),
    })


def main():
    global monitor
    parser = argparse.ArgumentParser(description="日本株 ATH接近ランキング Web モニター")
    parser.add_argument("--rss-host", default="192.168.1.113", help="RSSPilot のホスト")
    parser.add_argument("--rss-port", type=int, default=23203, help="RSSPilot のポート")
    parser.add_argument("--top", type=int, default=100, help="前日売買代金上位の監視銘柄数（デフォルト: 100）")
    parser.add_argument("--show", type=int, default=20, help="表示するATH比上位の件数（デフォルト: 20。0で全件）")
    parser.add_argument("--interval-ms", type=int, default=0,
                        help="購読の間引き間隔(ms)。0=更新の都度push（デフォルト）")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web サーバーのホスト")
    parser.add_argument("--web-port", type=int, default=5002, help="Web サーバーのポート")
    args = parser.parse_args()

    monitor = JpAthMonitor(
        host=args.rss_host,
        port=args.rss_port,
        top=args.top,
        display_top=args.show or None,
        interval_ms=args.interval_ms,
    )
    monitor.start()

    print(f"=> ブラウザで http://{args.web_host}:{args.web_port} を開いてください")
    app.run(host=args.web_host, port=args.web_port, threaded=True, debug=False)


if __name__ == "__main__":
    main()
