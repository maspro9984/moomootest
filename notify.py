"""通知チャネル（Discord Webhook）。

Webhook URL は秘密情報なので、環境変数 ATH_DISCORD_WEBHOOK か CLI で渡す。
（コードやgitには URL を入れない）

依存追加なし（標準ライブラリの urllib のみ）。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional


def _tv_symbol(code: str) -> str:
    """コード(例 US.AAPL / JP.9984 / HK.0700)を TradingView のシンボル表記にする。

    US は取引所(NASDAQ/NYSE)がデータに無いのでシンボル直指定（TradingViewが自動解決）。
    JP は東証(TSE:)、HK は HKEX: を付ける。
    """
    parts = str(code).split(".")
    mkt, sym = parts[0], parts[-1]
    if mkt == "JP":
        return f"TSE:{sym}"
    if mkt == "HK":
        return f"HKEX:{sym.lstrip('0') or sym}"
    return sym  # US など


def _chart_url(code: str) -> str:
    """コードから TradingView のチャートURLを作る。"""
    sym = urllib.parse.quote(_tv_symbol(code))
    return f"https://jp.tradingview.com/chart/?symbol={sym}"


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

        def spct(v):
            return f"{'+' if v > 0 else ''}{v:.2f}%" if isinstance(v, (int, float)) else "-"

        fields = [
            {"name": "現在値", "value": price(ev.get("cur")), "inline": True},
            {"name": "上場来高値", "value": price(ev.get("ath")), "inline": True},
        ]
        chg = ev.get("change_rate")
        if isinstance(chg, (int, float)):
            fields.append({"name": "前日比", "value": spct(chg), "inline": True})
        # 陽線率（プレ/レギュラー/トータル）
        fields.append({"name": "陽線率(プレ)", "value": spct(ev.get("yosen_pre")), "inline": True})
        fields.append({"name": "陽線率(レギュラー)", "value": spct(ev.get("yosen_reg")), "inline": True})
        fields.append({"name": "陽線率(トータル)", "value": spct(ev.get("yosen_total")), "inline": True})
        if ev.get("turnover_rank"):
            fields.append({"name": "売買代金順位", "value": f"#{ev['turnover_rank']}", "inline": True})
        if ev.get("industry"):
            fields.append({"name": "業種", "value": str(ev["industry"]), "inline": True})
        fields.append({"name": "チャート", "value": f"[TradingViewで開く]({_chart_url(code)})", "inline": False})

        return {
            "title": f"🚀 上場来高値 更新: {name} ({code})",
            "url": _chart_url(code),
            "color": 0xF59E0B,  # amber（アプリのATH更新ハイライトに合わせる）
            "fields": fields,
        }

    @staticmethod
    def build_ath_content(ev: dict) -> str:
        """embedが表示されない環境でも読める本文テキストを作る。"""
        code = ev.get("code", "")
        name = ev.get("name") or code
        cur = ev.get("cur")
        chg = ev.get("change_rate")
        yreg = ev.get("yosen_reg")
        ytot = ev.get("yosen_total")

        def spct(v):
            return f"{'+' if v > 0 else ''}{v:.2f}%" if isinstance(v, (int, float)) else None

        seg = []
        if isinstance(cur, (int, float)):
            seg.append(f"現在値 ${cur:,.2f}")
        if spct(chg):
            seg.append(f"前日比 {spct(chg)}")
        if spct(yreg):
            seg.append(f"陽線率(レギュラー) {spct(yreg)}")
        if spct(ytot):
            seg.append(f"陽線率(トータル) {spct(ytot)}")
        if ev.get("turnover_rank"):
            seg.append(f"売買代金順位 #{ev['turnover_rank']}")
        head = f"🚀 **{name} ({code})** 上場来高値を更新"
        body = head + ("　" + " / ".join(seg) if seg else "")
        return body + f"\n[📈 チャート]({_chart_url(code)})"

    # ---- 送信 ---- #
    def send_ath_update(self, ev: dict) -> None:
        # content(本文) と embed(詳細) を両方送る。embedが展開されなくても本文で読める。
        self._post({
            "content": self.build_ath_content(ev),
            "embeds": [self.build_ath_embed(ev)],
        })

    def send_text(self, text: str) -> None:
        self._post({"content": text})

    def _post(self, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                # Discord(Cloudflare)は既定の Python-urllib UA を403で弾くため明示する
                "User-Agent": "ath-monitor (https://github.com/maspro9984/moomootest, 1.0)",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()


def build_notifier(discord_webhook: Optional[str]):
    """設定に応じて通知オブジェクトを返す（未設定なら None）。"""
    if discord_webhook:
        return DiscordNotifier(discord_webhook.strip())
    return None
