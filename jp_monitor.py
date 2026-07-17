"""日本株版: 前日売買代金上位N銘柄の「上場来高値(ATH)接近ランキング」監視。

データ源は RSSPilot の dataapi(WebSocket) のみ（moomoo/OpenD は不要）。
  ユニバース : GetRanking(kind="volume")  … 前日売買代金ランキング(kabutan由来)
  業種/市場   : GetStockMaster            … JPX data_j.xls 由来
  ATH        : GetSnapshot の 上場来高値 / 上場来高値2
  現在値ほか  : GetSnapshot + Subscribe(リアルタイム push)

ATHの扱い:
  RSS(マケスピ)の「上場来高値」は分割前の未調整値のことがあり、web由来の
  「上場来高値2」(kabutan, 分割調整済)と食い違う。RSSPilot本体と同じく
  「安い方」を採用する。

セッション(JST):
  前場 09:00-11:30 / 昼休み 11:30-12:30 / 後場 12:30-15:30 / それ以外はクローズ
  ザラ場中に当日高値が基準ATHを超えたら「ATH更新」、大引け(15:30)でリセット。
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from jp_client import RssPilotClient

# 売買代金は千円単位で来るので円に直す倍率
TURNOVER_UNIT = 1000

# スナップショットで取る要素
SNAPSHOT_ELEMENTS = [
    "銘柄名", "現在値", "前日終値", "高値", "売買代金",
    "上場来高値", "上場来高値2", "陽線率", "後場陽線率",
]
# リアルタイム購読する要素（変動するものだけ）
SUBSCRIBE_ELEMENTS = ["現在値", "高値", "売買代金", "前日終値", "陽線率", "後場陽線率"]


def jp_session(now: Optional[datetime] = None) -> str:
    """JSTの時刻から市場セッションを返す。MORNING/LUNCH/AFTERNOON/CLOSED。

    ※ 祝日は判定しない（休場日はデータが流れないので実害は小さい）。
    """
    now = now or datetime.now()
    if now.weekday() >= 5:      # 土日
        return "CLOSED"
    t = now.hour * 60 + now.minute
    if 9 * 60 <= t < 11 * 60 + 30:
        return "MORNING"
    if 11 * 60 + 30 <= t < 12 * 60 + 30:
        return "LUNCH"
    if 12 * 60 + 30 <= t < 15 * 60 + 30:
        return "AFTERNOON"
    return "CLOSED"


def _f(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def pick_ath(rss_ath, web_ath) -> float:
    """上場来高値は「安い方」を採用（RSS側が分割未調整のことがあるため）。"""
    a, b = _f(rss_ath), _f(web_ath)
    vals = [v for v in (a, b) if v > 0]
    return min(vals) if vals else 0.0


class JpAthMonitor:
    """前日売買代金上位N × ATH接近率をリアルタイム監視する。"""

    def __init__(self, host: str, port: int = 23203, top: int = 100,
                 display_top: Optional[int] = 20, interval_ms: int = 0):
        self.host = host
        self.port = port
        self.top = top
        self.display_top = display_top
        self.interval_ms = interval_ms

        self._state: Dict[str, dict] = {}
        self._codes: List[str] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._client: Optional[RssPilotClient] = None
        self._thread: Optional[threading.Thread] = None
        self._mode = "unknown"          # "realtime" | "error"
        self._session = "CLOSED"
        self._session_ready = False

    # ------------------------------------------------------------------ #
    @property
    def mode(self) -> str:
        return self._mode

    @property
    def session(self) -> str:
        return self._session

    @property
    def universe_n(self) -> int:
        return len(self._codes)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._client:
            self._client.stop()

    # ------------------------------------------------------------------ #
    def _run(self) -> None:
        if not self._setup():
            self._mode = "error"
            return
        self._mode = "realtime"
        # セッション監視（大引けでATH更新フラグをリセット）
        while not self._stop.is_set():
            self._refresh_session()
            self._stop.wait(3)

    def _setup(self) -> bool:
        cli = RssPilotClient(self.host, self.port, tag="athjp")
        if not cli.start():
            print("[jp] RSSPilot に接続できませんでした。")
            return False
        self._client = cli
        cli.on_data = self._on_data
        # 購読はセッション単位なので、再接続時に貼り直す
        cli.on_reconnect = self._resubscribe

        # 1) ユニバース: 前日売買代金ランキング
        rank = cli.get_ranking("volume", self.top)
        if not rank:
            print("[jp] 前日売買代金ランキングを取得できませんでした。")
            return False
        codes = [str(r["code"]) for r in rank]
        rank_map = {str(r["code"]): r["rank"] for r in rank}
        name_map = {str(r["code"]): r.get("name") for r in rank}

        # 2) 業種・市場区分（JPX由来。銘柄名もこちらの正式名を優先）
        master = cli.get_stock_master(codes)

        # 3) 初期スナップショット
        snap = cli.get_snapshot(codes, SNAPSHOT_ELEMENTS)

        state: Dict[str, dict] = {}
        for code in codes:
            v = snap.get(code, {})
            m = master.get(code, {})
            state[code] = {
                "name": m.get("name") or v.get("銘柄名") or name_map.get(code) or code,
                "industry": m.get("gyoshu33") or "",
                "market": m.get("market") or "",
                "turnover_rank": rank_map.get(code),
                "ath": pick_ath(v.get("上場来高値"), v.get("上場来高値2")),
                "ath_rss": _f(v.get("上場来高値")),
                "ath_web": _f(v.get("上場来高値2")),
                "ath_updated": False,
                "last": _f(v.get("現在値")),
                "prev_close": _f(v.get("前日終値")),
                "high": _f(v.get("高値")),
                "turnover": _f(v.get("売買代金")) * TURNOVER_UNIT,
                "yosen": v.get("陽線率"),
                "yosen_pm": v.get("後場陽線率"),
            }
        with self._lock:
            self._state = state
            self._codes = codes

        # 4) リアルタイム購読
        if not cli.subscribe(codes, SUBSCRIBE_ELEMENTS, self.interval_ms):
            print("[jp] 購読に失敗しました。")
            return False
        self._refresh_session(initial=True)
        print(f"[jp] リアルタイム監視を開始: 前日売買代金上位{len(codes)}銘柄")
        return True

    def _resubscribe(self) -> None:
        """再接続後の復帰。購読を貼り直し、取りこぼした分をスナップショットで補正。"""
        cli, codes = self._client, list(self._codes)
        if cli is None or not codes:
            return
        if not cli.subscribe(codes, SUBSCRIBE_ELEMENTS, self.interval_ms):
            print("[jp] 再購読に失敗しました")
            return
        snap = cli.get_snapshot(codes, SNAPSHOT_ELEMENTS)
        with self._lock:
            for code, v in snap.items():
                s = self._state.get(code)
                if s is None:
                    continue
                s["last"] = _f(v.get("現在値")) or s.get("last")
                s["prev_close"] = _f(v.get("前日終値")) or s.get("prev_close")
                s["high"] = max(s.get("high") or 0.0, _f(v.get("高値")))
                s["turnover"] = _f(v.get("売買代金")) * TURNOVER_UNIT or s.get("turnover")
                if v.get("陽線率") is not None:
                    s["yosen"] = v.get("陽線率")
                if v.get("後場陽線率") is not None:
                    s["yosen_pm"] = v.get("後場陽線率")
        print(f"[jp] 再購読しました（{len(codes)}銘柄）")

    # ------------------------------------------------------------------ #
    def _on_data(self, code: str, element: str, value) -> None:
        """RSSPilot からの push を state に反映する。"""
        with self._lock:
            s = self._state.get(code)
            if s is None:
                return
            if element == "現在値":
                s["last"] = _f(value)
            elif element == "前日終値":
                s["prev_close"] = _f(value)
            elif element == "売買代金":
                s["turnover"] = _f(value) * TURNOVER_UNIT
            elif element == "陽線率":
                s["yosen"] = value
            elif element == "後場陽線率":
                s["yosen_pm"] = value
            elif element == "高値":
                high = _f(value)
                if high > (s.get("high") or 0.0):
                    s["high"] = high
                    # ザラ場中に基準ATHを超えたら「ATH更新」（大引けでリセット）
                    if self._session in ("MORNING", "AFTERNOON") and s.get("ath") and high > s["ath"]:
                        s["ath_updated"] = True

    def _refresh_session(self, initial: bool = False) -> None:
        new = jp_session()
        old = self._session
        self._session = new
        if initial or not self._session_ready:
            self._session_ready = True
            return
        if old == "AFTERNOON" and new == "CLOSED":
            # 大引け: ATH更新フラグをリセット
            self._reset_daily()

    def _reset_daily(self) -> None:
        with self._lock:
            for s in self._state.values():
                s["ath_updated"] = False
        print("[jp] 大引け: ATH更新フラグをリセットしました")

    # ------------------------------------------------------------------ #
    def get_ranking(self, sort: str = "pct") -> List[dict]:
        """ATH接近率降順(既定) または 前日売買代金順のランキングを返す。"""
        rows: List[dict] = []
        with self._lock:
            for code, s in self._state.items():
                ath = s.get("ath") or 0.0
                high = s.get("high") or 0.0
                cur = s.get("last") or 0.0
                # 当日高値がATHを超えていればそれが実質の新ATH
                eff_ath = max(ath, high)
                if not eff_ath or not cur:
                    continue
                prev_close = s.get("prev_close") or 0.0
                change = (cur - prev_close) if prev_close else None
                rows.append({
                    "code": code,
                    "name": s.get("name"),
                    "industry": s.get("industry"),
                    "market": s.get("market"),
                    "cur": cur,
                    "ath": round(eff_ath, 2),
                    "orig_ath": round(ath, 2),
                    "ath_rss": s.get("ath_rss"),
                    "ath_web": s.get("ath_web"),
                    "high": round(high, 2),
                    "pct": round(cur / eff_ath * 100.0, 4),
                    "change": round(change, 2) if change is not None else None,
                    "change_rate": round(change / prev_close * 100.0, 4) if change is not None and prev_close else None,
                    # 陽線率は比率で来るので % に直す
                    "yosen": round(_f(s.get("yosen")) * 100.0, 3) if s.get("yosen") is not None else None,
                    "yosen_pm": round(_f(s.get("yosen_pm")) * 100.0, 3) if s.get("yosen_pm") is not None else None,
                    "turnover": s.get("turnover"),
                    "turnover_rank": s.get("turnover_rank"),
                    "ath_updated": bool(s.get("ath_updated")),
                })
        if sort == "turnover":
            rows = [r for r in rows if r.get("turnover_rank")]
            rows.sort(key=lambda x: x["turnover_rank"])
        else:
            rows.sort(key=lambda x: x["pct"], reverse=True)
        if self.display_top:
            rows = rows[: self.display_top]
        for i, r in enumerate(rows, start=1):
            r["rank"] = i
        return rows
