"""通知チャネル（Discord Webhook）。

Webhook URL は秘密情報なので、環境変数 ATH_DISCORD_WEBHOOK か CLI で渡す。
（コードやgitには URL を入れない）

依存追加なし（標準ライブラリの urllib のみ）。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Optional


def _yahoo_url(code: str) -> str:
    """コード(例 US.AAPL / JP.9984)から日本のYahoo FinanceチャートURLを作る。"""
    parts = str(code).split(".")
    mkt, sym = parts[0], parts[-1]
    if mkt == "JP":
        q = sym + ".T"
    elif mkt == "HK":
        q = sym.zfill(4) + ".HK"
    else:
        q = sym
    return f"https://finance.yahoo.co.jp/quote/{q}/chart"


class DiscordNotifier:
    """Discord Webhook へ通知を送る。"""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    # ---- メッセージ組み立て ---- #
    @staticmethod
    def build_ath_embed(ev: dict) -> dict:
        """ATH更新イベントから Discord embed ペイロードを作る。"""
        code = ev.get("code", "")
        name = ev.get("name") or code

        def money(v):
            if not isinstance(v, (int, float)):
                return "-"
            if v >= 1e12:
                return f"{v / 1e12:.2f} T"
            if v >= 1e9:
                return f"{v / 1e9:.2f} B"
            return f"{v:,.0f}"

        def price(v):
            return f"${v:,.2f}" if isinstance(v, (int, float)) else "-"

        fields = [
            {"name": "現在値", "value": price(ev.get("cur")), "inline": True},
            {"name": "上場来高値", "value": price(ev.get("ath")), "inline": True},
            {"name": "ATH比", "value": (f"{ev['pct']:.2f}%" if isinstance(ev.get("pct"), (int, float)) else "-"), "inline": True},
        ]
        chg = ev.get("change_rate")
        if isinstance(chg, (int, float)):
            fields.append({"name": "前日比", "value": f"{'+' if chg > 0 else ''}{chg:.2f}%", "inline": True})
        if ev.get("industry"):
            fields.append({"name": "業種", "value": str(ev["industry"]), "inline": True})
        if ev.get("turnover_rank"):
            fields.append({"name": "売買代金順位", "value": str(ev["turnover_rank"]), "inline": True})
        if ev.get("market_cap_rank"):
            fields.append({"name": "時価総額", "value": f"{money(ev.get('market_cap'))} (#{ev['market_cap_rank']})", "inline": True})

        return {
            "title": f"🚀 上場来高値 更新: {name} ({code})",
            "url": _yahoo_url(code),
            "color": 0xF59E0B,  # amber（アプリのATH更新ハイライトに合わせる）
            "fields": fields,
        }

    # ---- 送信 ---- #
    def send_ath_update(self, ev: dict) -> None:
        self._post({"embeds": [self.build_ath_embed(ev)]})

    def send_text(self, text: str) -> None:
        self._post({"content": text})

    def _post(self, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()


def build_notifier(discord_webhook: Optional[str]):
    """設定に応じて通知オブジェクトを返す（未設定なら None）。"""
    if discord_webhook:
        return DiscordNotifier(discord_webhook.strip())
    return None
