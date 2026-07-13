"""売買代金上位N銘柄の「上場来高値(ATH)接近率」をリアルタイム監視するクライアント。

仕組み:
  1. 起動時にスクリーナーで売買代金 (TURNOVER) 上位N銘柄を取得（ユニバース確定）
  2. その銘柄群を get_market_snapshot にかけ、上場来高値 highest_history_price(=ATH)を取得
  3. QUOTE をリアルタイム購読し、last_price / high_price(当日高値) を受信
  4. effective_ATH = max(ATH, 当日高値) として、接近率 = 現在値 / effective_ATH を随時計算
     - 当日高値が既存ATHを超えたら「新高値更新中」として検知
     - セッション(プレ/レギュラー/アフター)に応じて現在値フィールドを切り替える

リアルタイム配信(QUOTE)には ATH は含まれないため、ATH は起動時に snapshot で取得し、
以降は当日高値との max で client 側で更新する（再ポーリング不要）。

OpenD が起動していない / SDK が使えない場合はモックモードにフォールバックする。
"""

from __future__ import annotations

import random
import threading
import time
from typing import Dict, List, Optional

import top_turnover as tt  # fetch_top / resolve_market / ft を再利用

ft = tt.ft

# get_market_snapshot の1回あたり銘柄数（安全側の分割サイズ）
SNAPSHOT_CHUNK = 50


def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _session_bucket(market_state: str) -> str:
    """OpenD の市場ステート文字列を PRE / REGULAR / AFTER / OVERNIGHT / CLOSED に分類する。"""
    s = (market_state or "").upper()
    if "PRE_MARKET" in s or s == "PRE":
        return "PRE"
    if "OVERNIGHT" in s:
        return "OVERNIGHT"
    if "AFTER_HOURS_END" in s or "END" in s or "CLOSED" in s or "REST" in s:
        # アフター終了・休場はクローズ扱い（現在値は last_price を使う）
        return "CLOSED"
    if "AFTER" in s:
        return "AFTER"
    if "TRADING" in s or "MORNING" in s or "AFTERNOON" in s or "REGULAR" in s or "OPEN" in s:
        return "REGULAR"
    return "CLOSED"


class AthMonitor:
    """売買代金上位銘柄の ATH 接近率をリアルタイム監視する。"""

    def __init__(
        self,
        market: str = "US",
        top: int = 50,
        host: str = "127.0.0.1",
        port: int = 11111,
        extended_time: bool = True,
    ):
        self.market_name = market.upper()
        self.top = top
        self.host = host
        self.port = port
        self.extended_time = extended_time

        self._state: Dict[str, dict] = {}   # code -> {name, ath, last, high, pre, after, overnight, turnover, update_time}
        self._codes: List[str] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._mode = "unknown"              # "realtime" | "mock"
        self._session = "CLOSED"
        self._session_raw = ""
        self._quote_ctx = None
        self._thread: Optional[threading.Thread] = None
        self._started_at = ""

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    @property
    def mode(self) -> str:
        return self._mode

    @property
    def session(self) -> str:
        return self._session

    @property
    def session_raw(self) -> str:
        return self._session_raw

    @property
    def universe_n(self) -> int:
        return len(self._codes)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._quote_ctx is not None:
            try:
                self._quote_ctx.close()
            except Exception:
                pass

    def get_ranking(self) -> List[dict]:
        """現在の状態から ATH 接近率降順のランキングを組み立てて返す。"""
        sess = self._session
        rows: List[dict] = []
        with self._lock:
            for code, s in self._state.items():
                ath = s.get("ath") or 0.0
                high = s.get("high") or 0.0
                cur = self._session_price(s, sess)
                # 当日高値が既存ATHを超えていれば、それが実質の新ATH
                eff_ath = max(ath, high)
                if not eff_ath or not cur:
                    continue
                pct = cur / eff_ath * 100.0
                is_new_ath = bool(ath and high and high >= ath)
                rows.append(
                    {
                        "code": code,
                        "name": s.get("name", ""),
                        "cur": round(cur, 4),
                        "ath": round(eff_ath, 4),
                        "orig_ath": round(ath, 4),
                        "high": round(high, 4),
                        "pct": round(pct, 4),
                        "turnover": s.get("turnover"),
                        "turnover_rank": s.get("turnover_rank"),
                        "is_new_ath": is_new_ath,
                        "update_time": s.get("update_time", ""),
                    }
                )
        rows.sort(key=lambda x: x["pct"], reverse=True)
        for i, r in enumerate(rows, start=1):
            r["rank"] = i
        return rows

    @staticmethod
    def _session_price(s: dict, sess: str) -> float:
        """現在のセッションに対応する現在値を選ぶ（無ければ last_price にフォールバック）。"""
        if sess == "PRE" and s.get("pre"):
            return s["pre"]
        if sess == "AFTER" and s.get("after"):
            return s["after"]
        if sess == "OVERNIGHT" and s.get("overnight"):
            return s["overnight"]
        return s.get("last") or 0.0

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #
    def _run(self) -> None:
        try:
            if self._try_realtime():
                return
        except SystemExit:
            # fetch_top 等が内部で sys.exit する場合に備える
            pass
        except Exception as exc:  # pragma: no cover
            print(f"[ath] リアルタイム初期化に失敗: {exc}")
        self._mode = "mock"
        self._run_mock()

    @staticmethod
    def _opend_reachable(host: str, port: int, timeout: float = 2.0) -> bool:
        import socket

        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    # ---- realtime ---------------------------------------------------- #
    def _try_realtime(self) -> bool:
        if ft is None:
            print("[ath] SDK(moomoo-api)が未インストールです。モックモードで起動します。")
            return False
        if not self._opend_reachable(self.host, self.port):
            print(f"[ath] OpenD ({self.host}:{self.port}) に接続できません。モックモードで起動します。")
            return False

        quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
        self._quote_ctx = quote_ctx

        # 1) 売買代金上位N（ユニバース）
        market = tt.resolve_market(self.market_name)
        rows = tt.fetch_top(quote_ctx, market, self.top)
        if not rows:
            print("[ath] 売買代金上位の取得結果が空。モックモードで起動します。")
            quote_ctx.close()
            self._quote_ctx = None
            return False
        codes = [r["code"] for r in rows]
        turnover_map = {r["code"]: r.get("turnover") for r in rows}
        # fetch_top は売買代金の降順で rank を採番済み（=売買代金順位）
        turnover_rank_map = {r["code"]: r.get("rank") for r in rows}

        # 2) ATH（上場来高値）と初期値を snapshot で取得
        with self._lock:
            for chunk in _chunked(codes, SNAPSHOT_CHUNK):
                ret, data = quote_ctx.get_market_snapshot(chunk)
                if ret != ft.RET_OK:
                    print(f"[ath] snapshot 取得失敗: {data}. モックモードで起動します。")
                    quote_ctx.close()
                    self._quote_ctx = None
                    return False
                for _, r in data.iterrows():
                    code = r["code"]
                    self._state[code] = {
                        "name": r.get("name", "") or "",
                        "ath": _f(r.get("highest_history_price")),
                        "last": _f(r.get("last_price")),
                        "high": _f(r.get("high_price")),
                        "pre": _f(r.get("pre_price")),
                        "after": _f(r.get("after_price")),
                        "overnight": _f(r.get("overnight_price")),
                        "turnover": turnover_map.get(code),
                        "turnover_rank": turnover_rank_map.get(code),
                        "update_time": str(r.get("update_time", "") or ""),
                    }
            self._codes = codes

        # 3) QUOTE リアルタイム購読
        monitor = self

        class _Handler(ft.StockQuoteHandlerBase):
            def on_recv_rsp(self, rsp_pb):
                ret_code, data = super().on_recv_rsp(rsp_pb)
                if ret_code != ft.RET_OK:
                    return ret_code, data
                for _, row in data.iterrows():
                    monitor._on_row(row)
                return ret_code, data

        quote_ctx.set_handler(_Handler())
        ret, data = quote_ctx.subscribe(
            codes, [ft.SubType.QUOTE], extended_time=self.extended_time
        )
        if ret != ft.RET_OK:
            print(f"[ath] 購読失敗: {data}. モックモードで起動します。")
            quote_ctx.close()
            self._quote_ctx = None
            return False

        self._mode = "realtime"
        self._refresh_session()
        print(f"[ath] リアルタイム監視を開始: {self.market_name} 上位{len(codes)}銘柄")

        # 接続維持しつつ、セッション状態を定期更新
        while not self._stop.is_set():
            time.sleep(3)
            self._refresh_session()
        return True

    def _refresh_session(self) -> None:
        if self._quote_ctx is None:
            return
        try:
            ret, gs = self._quote_ctx.get_global_state()
            if ret == ft.RET_OK:
                key = f"market_{self.market_name.lower()}"
                raw = gs.get(key) or gs.get("market_us") or ""
                self._session_raw = str(raw)
                self._session = _session_bucket(str(raw))
        except Exception:
            pass

    def _on_row(self, row) -> None:
        try:
            code = str(row.get("code", "") if hasattr(row, "get") else row["code"])
        except Exception:
            return
        if not code:
            return
        with self._lock:
            s = self._state.get(code)
            if s is None:
                return
            last = _f(_rowget(row, "last_price"))
            high = _f(_rowget(row, "high_price"))
            if last:
                s["last"] = last
            # 当日高値は単調に更新（realtime の high_price を採用しつつ後退させない）
            if high:
                s["high"] = max(s.get("high") or 0.0, high)
            for fld, key in (("pre", "pre_price"), ("after", "after_price"), ("overnight", "overnight_price")):
                v = _f(_rowget(row, key))
                if v:
                    s[fld] = v
            dt = f"{_rowget(row, 'data_date', '')} {_rowget(row, 'data_time', '')}".strip()
            if dt:
                s["update_time"] = dt

    # ---- mock -------------------------------------------------------- #
    def _run_mock(self) -> None:
        print("[ath] モックモードで起動しました（擬似データ）。実データには OpenD が必要です。")
        self._session = "REGULAR"
        self._session_raw = "MOCK_TRADING"
        sample = [
            ("US.AAPL", "アップル"), ("US.NVDA", "エヌビディア"), ("US.MSFT", "マイクロソフト"),
            ("US.AMZN", "アマゾン"), ("US.META", "メタ"), ("US.TSLA", "テスラ"),
            ("US.AMD", "AMD"), ("US.GOOGL", "アルファベット"), ("US.AVGO", "ブロードコム"),
            ("US.MU", "マイクロン"), ("US.NFLX", "ネットフリックス"), ("US.PLTR", "パランティア"),
        ][: max(1, min(self.top, 12))]
        with self._lock:
            for code, name in sample:
                ath = round(random.uniform(100, 1000), 2)
                last = round(ath * random.uniform(0.4, 0.99), 2)
                self._state[code] = {
                    "name": name, "ath": ath, "last": last, "high": last,
                    "pre": 0.0, "after": 0.0, "overnight": 0.0,
                    "turnover": round(random.uniform(2e9, 3e10), 0),
                    "update_time": "",
                }
            self._codes = [c for c, _ in sample]
            # 売買代金の降順で順位を採番
            for i, (code, s) in enumerate(
                sorted(self._state.items(), key=lambda kv: kv[1].get("turnover") or 0, reverse=True),
                start=1,
            ):
                s["turnover_rank"] = i

        while not self._stop.is_set():
            with self._lock:
                for code in self._codes:
                    s = self._state[code]
                    drift = random.gauss(0, 0.004)
                    s["last"] = max(1.0, round(s["last"] * (1 + drift), 2))
                    s["high"] = max(s.get("high") or 0.0, s["last"])
                    s["update_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(1.0)


def _f(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _rowget(row, key, default=None):
    try:
        v = row[key]
        return default if v is None else v
    except Exception:
        return default
