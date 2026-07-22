"""売買代金上位N銘柄の「上場来高値(ATH)接近ランキング」をWebでリアルタイム表示する。

使い方:
    pip install -r requirements.txt
    python ath_app.py                       # 米国 売買代金上位50をリアルタイム監視
    python ath_app.py --market US --top 100
    python ath_app.py --host 127.0.0.1 --port 11111       # OpenD 接続先
    python ath_app.py --web-port 5001                     # Web ポート

その後ブラウザで http://127.0.0.1:5001 を開く。

OpenD が起動していれば実データ、そうでなければモックデータ（擬似）を表示する。
"""

from __future__ import annotations

import argparse

import os

from flask import Flask, jsonify, render_template, request

from ath_monitor import AthMonitor
from notify import build_notifier

app = Flask(__name__)

monitor: AthMonitor | None = None


@app.route("/")
def index():
    return render_template(
        "ath.html",
        market=monitor.market_name if monitor else "US",
        top=monitor.top if monitor else 0,
        show=(monitor.display_top or 0) if monitor else 0,
        view="ath",
    )


@app.route("/turnover")
def turnover_page():
    """前日売買代金 上位の静的ページ（同レイアウト、更新なし）。"""
    return render_template(
        "ath.html",
        market=monitor.market_name if monitor else "US",
        top=monitor.top if monitor else 0,
        show=(monitor.display_top or 0) if monitor else 0,
        view="turnover",
    )


@app.route("/turnover-today")
def turnover_today_page():
    """当日売買代金 上位のリアルタイムページ（同レイアウト、自動更新あり）。"""
    return render_template(
        "ath.html",
        market=monitor.market_name if monitor else "US",
        top=monitor.top if monitor else 0,
        show=(monitor.display_top or 0) if monitor else 0,
        view="turnover_today",
    )


@app.route("/api/ranking")
def api_ranking():
    """ランキングを JSON で返す。?sort=turnover で前日売買代金順、
    ?sort=turnover_today で当日売買代金順。"""
    if monitor is None:
        return jsonify({"mode": "unknown", "rows": []})
    sort = request.args.get("sort")
    if sort not in ("turnover", "turnover_today"):
        sort = "pct"
    return jsonify(
        {
            "mode": monitor.mode,
            "session": monitor.session,
            "session_raw": monitor.session_raw,
            "market": monitor.market_name,
            "universe_n": monitor.universe_n,
            "rows": monitor.get_ranking(sort=sort),
        }
    )


def main():
    global monitor
    parser = argparse.ArgumentParser(description="ATH接近ランキング Web モニター")
    parser.add_argument("--market", default="US", help="市場 (US / HK / JP など。デフォルト: US)")
    parser.add_argument("--top", type=int, default=100, help="売買代金上位の監視銘柄数（デフォルト: 100）")
    parser.add_argument("--show", type=int, default=20, help="表示するATH比上位の件数（デフォルト: 20。0で全件）")
    parser.add_argument("--refresh-universe", action="store_true", help="保存済みユニバースを使わず売買代金上位を取り直す")
    parser.add_argument("--discord-webhook", default=os.environ.get("ATH_DISCORD_WEBHOOK"),
                        help="Discord Webhook URL（未指定なら環境変数 ATH_DISCORD_WEBHOOK を使用）")
    parser.add_argument("--notify-cooldown", type=int, default=3600,
                        help="同一銘柄の再通知を抑制する秒数（デフォルト3600=1時間）")
    parser.add_argument("--host", default="127.0.0.1", help="OpenD のホスト")
    parser.add_argument("--port", type=int, default=11111, help="OpenD のポート")
    parser.add_argument("--no-extended", action="store_true", help="プレ/アフターを購読しない")
    parser.add_argument("--no-yosen", action="store_true", help="陽線率の算出を無効にする")
    parser.add_argument("--yosen-interval", type=int, default=30, help="陽線率の更新間隔(秒)。デフォルト30")
    parser.add_argument("--yosen-tf", choices=["1m", "5m"], default="5m", help="陽線率の足種。デフォルト5m")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web サーバーのホスト")
    parser.add_argument("--web-port", type=int, default=5001, help="Web サーバーのポート")
    args = parser.parse_args()

    from ath_monitor import ft as _ft
    yosen_ktype = None
    if _ft is not None:
        yosen_ktype = _ft.KLType.K_1M if args.yosen_tf == "1m" else _ft.KLType.K_5M

    # Webhook の優先順位: --discord-webhook > 環境変数 > discord_webhook.txt
    webhook = args.discord_webhook
    if not webhook:
        for fn in ("discord_webhook.txt", ".discord_webhook"):
            if os.path.exists(fn):
                try:
                    webhook = open(fn, encoding="utf-8").read().strip()
                except Exception:
                    webhook = None
                if webhook:
                    print(f"[ath] {fn} からWebhookを読み込みました")
                    break
    notifier = build_notifier(webhook)
    print("[ath] Discord通知:", "有効" if notifier else "無効（discord_webhook.txt にURLを1行書くか --discord-webhook / 環境変数）")

    monitor = AthMonitor(
        market=args.market,
        top=args.top,
        host=args.host,
        port=args.port,
        extended_time=not args.no_extended,
        yosen=not args.no_yosen,
        yosen_interval=args.yosen_interval,
        yosen_ktype=yosen_ktype,
        display_top=args.show or None,
        refresh_universe=args.refresh_universe,
        notifier=notifier,
        notify_cooldown=args.notify_cooldown,
    )
    monitor.start()

    print(f"=> ブラウザで http://{args.web_host}:{args.web_port} を開いてください")
    app.run(host=args.web_host, port=args.web_port, threaded=True, debug=False)


if __name__ == "__main__":
    main()
