"""moomoo (Futu) OpenD ゲートウェイに接続し、日本株のリアルタイム相場を購読するクライアント。

moomoo OpenAPI の構成:
  [このツール] --(TCP 127.0.0.1:11111)--> [OpenD ゲートウェイ] --> [moomoo サーバー]

OpenD が起動していない / moomoo-api が未インストールの場合は、
自動的にモックモードにフォールバックして擬似リアルタイムデータを生成します。
（UI の動作確認用。実データではありません）
"""

from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Callable, Dict, List, Optional

# SDK は任意依存。無ければモックモードで動作する。
# moomoo証券は moomoo-api (import名: moomoo) が正式。futu-api も互換なので両対応。
SDK_NAME: Optional[str] = None
try:
    from moomoo import (  # type: ignore
        OpenQuoteContext,
        StockQuoteHandlerBase,
        SubType,
        RET_OK,
    )

    SDK_NAME = "moomoo-api"
except Exception:  # pragma: no cover - 環境依存
    try:
        from futu import (  # type: ignore
            OpenQuoteContext,
            StockQuoteHandlerBase,
            SubType,
            RET_OK,
        )

        SDK_NAME = "futu-api"
    except Exception:
        pass

SDK_AVAILABLE = SDK_NAME is not None


@dataclass
class Quote:
    """1銘柄分のリアルタイム相場スナップショット。"""

    code: str
    name: str = ""
    last_price: float = 0.0
    open_price: float = 0.0
    prev_close_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    volume: int = 0
    turnover: float = 0.0
    change_val: float = 0.0
    change_rate: float = 0.0
    update_time: str = ""
    data_status: str = "MOCK"  # "REALTIME" or "MOCK"

    def to_dict(self) -> dict:
        return asdict(self)


QuoteCallback = Callable[[Quote], None]


class MoomooQuoteClient:
    """OpenD からリアルタイム相場を購読し、更新をコールバックで通知する。"""

    def __init__(
        self,
        codes: List[str],
        host: str = "127.0.0.1",
        port: int = 11111,
        on_update: Optional[QuoteCallback] = None,
    ):
        # 日本株コードは "JP.9984" 形式に正規化する
        self.codes = [self._normalize(c) for c in codes]
        self.host = host
        self.port = port
        self.on_update = on_update

        self._quotes: Dict[str, Quote] = {c: Quote(code=c) for c in self.codes}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._mode = "unknown"  # "realtime" | "mock"
        self._quote_ctx = None
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize(code: str) -> str:
        """'9984' -> 'JP.9984'。既に市場プレフィックスがあればそのまま。"""
        code = code.strip().upper()
        if "." in code:
            return code
        return f"JP.{code}"

    @property
    def mode(self) -> str:
        return self._mode

    def start(self) -> None:
        """バックグラウンドで購読を開始する。"""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._quote_ctx is not None:
            try:
                self._quote_ctx.close()
            except Exception:
                pass

    def get_quotes(self) -> List[dict]:
        with self._lock:
            return [self._quotes[c].to_dict() for c in self.codes]

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #
    def _run(self) -> None:
        if not SDK_AVAILABLE:
            print("[moomoo] SDK (moomoo-api) が未インストールです。`pip install moomoo-api` で実データに対応できます。")
        elif self._try_realtime():
            return
        # フォールバック
        self._mode = "mock"
        self._run_mock()

    @staticmethod
    def _opend_reachable(host: str, port: int, timeout: float = 2.0) -> bool:
        """OpenD の待受ポートに TCP 接続できるかを高速チェックする。"""
        import socket

        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    def _publish(self, quote: Quote) -> None:
        with self._lock:
            self._quotes[quote.code] = quote
        if self.on_update:
            try:
                self.on_update(quote)
            except Exception:
                pass

    # ---- realtime (futu-api) ---------------------------------------- #
    def _try_realtime(self) -> bool:
        """OpenD への接続と購読を試みる。成功したら True。"""
        # SDK の接続リトライを待たず、まずポート到達性を即時判定する
        if not self._opend_reachable(self.host, self.port):
            print(
                f"[moomoo] OpenD ({self.host}:{self.port}) に接続できません。"
                "OpenD が起動しているか確認してください。モックモードで起動します。"
            )
            return False

        print(f"[moomoo] SDK: {SDK_NAME} / OpenD {self.host}:{self.port} へ接続します...")
        try:
            quote_ctx = OpenQuoteContext(host=self.host, port=self.port)
        except Exception as exc:  # OpenD 未起動など
            print(f"[moomoo] OpenD へ接続できませんでした ({exc}). モックモードで起動します。")
            return False

        self._quote_ctx = quote_ctx
        client = self

        class _Handler(StockQuoteHandlerBase):
            def on_recv_rsp(self, rsp_pb):
                ret_code, data = super().on_recv_rsp(rsp_pb)
                if ret_code != RET_OK:
                    return ret_code, data
                for _, row in data.iterrows():
                    client._on_realtime_row(row)
                return ret_code, data

        quote_ctx.set_handler(_Handler())

        ret, data = quote_ctx.subscribe(self.codes, [SubType.QUOTE])
        if ret != RET_OK:
            print(f"[moomoo] 購読に失敗しました: {data}. モックモードで起動します。")
            try:
                quote_ctx.close()
            except Exception:
                pass
            self._quote_ctx = None
            return False

        self._mode = "realtime"
        print(f"[moomoo] リアルタイム購読を開始しました: {', '.join(self.codes)}")

        # 初期スナップショットを取得
        ret, snap = quote_ctx.get_stock_quote(self.codes)
        if ret == RET_OK:
            for _, row in snap.iterrows():
                self._on_realtime_row(row)

        # 接続維持
        while not self._stop.is_set():
            time.sleep(1)
        return True

    def _on_realtime_row(self, row) -> None:
        def g(key, default=0.0):
            try:
                val = row[key]
                return default if val is None else val
            except Exception:
                return default

        code = str(g("code", ""))
        if not code:
            return
        last = float(g("last_price"))
        prev_close = float(g("prev_close_price"))
        change_val = last - prev_close if prev_close else 0.0
        change_rate = (change_val / prev_close * 100.0) if prev_close else 0.0

        quote = Quote(
            code=code,
            name=str(g("stock_name", "") or ""),
            last_price=last,
            open_price=float(g("open_price")),
            prev_close_price=prev_close,
            high_price=float(g("high_price")),
            low_price=float(g("low_price")),
            volume=int(g("volume", 0) or 0),
            turnover=float(g("turnover")),
            change_val=round(change_val, 4),
            change_rate=round(change_rate, 4),
            update_time=str(g("data_date", "") or "") + " " + str(g("data_time", "") or ""),
            data_status="REALTIME",
        )
        self._publish(quote)

    # ---- mock -------------------------------------------------------- #
    def _run_mock(self) -> None:
        """擬似的なランダムウォークでリアルタイムデータを生成する。"""
        print("[moomoo] モックモードで起動しました（擬似データ）。実データには OpenD が必要です。")

        # 銘柄ごとの初期値（それらしい基準値）
        base_prices = {c: random.uniform(1500, 9000) for c in self.codes}
        names = {c: f"銘柄 {c}" for c in self.codes}
        # よく使う銘柄には分かりやすい名前を付ける
        friendly = {
            "JP.9984": "ソフトバンクグループ",
            "JP.7203": "トヨタ自動車",
            "JP.6758": "ソニーグループ",
            "JP.9432": "日本電信電話",
            "JP.8306": "三菱UFJフィナンシャル・グループ",
        }
        for c in self.codes:
            if c in friendly:
                names[c] = friendly[c]

        state = {}
        for c in self.codes:
            base = round(base_prices[c], 1)
            state[c] = {
                "prev_close": base,
                "open": round(base * random.uniform(0.99, 1.01), 1),
                "last": base,
                "high": base,
                "low": base,
                "volume": 0,
            }

        while not self._stop.is_set():
            for c in self.codes:
                s = state[c]
                # ランダムウォーク（±0.3%程度）
                drift = random.gauss(0, 0.003)
                s["last"] = max(1.0, round(s["last"] * (1 + drift), 1))
                s["high"] = max(s["high"], s["last"])
                s["low"] = min(s["low"] or s["last"], s["last"])
                s["volume"] += random.randint(100, 5000)

                prev = s["prev_close"]
                change_val = s["last"] - prev
                change_rate = (change_val / prev * 100.0) if prev else 0.0

                quote = Quote(
                    code=c,
                    name=names[c],
                    last_price=round(s["last"], 1),
                    open_price=round(s["open"], 1),
                    prev_close_price=round(prev, 1),
                    high_price=round(s["high"], 1),
                    low_price=round(s["low"], 1),
                    volume=s["volume"],
                    turnover=round(s["last"] * s["volume"], 1),
                    change_val=round(change_val, 2),
                    change_rate=round(change_rate, 2),
                    update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    data_status="MOCK",
                )
                self._publish(quote)
            time.sleep(1.0)
