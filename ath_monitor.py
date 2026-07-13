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
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import top_turnover as tt  # fetch_top / resolve_market / ft を再利用

ft = tt.ft

# get_market_snapshot の1回あたり銘柄数（安全側の分割サイズ）
SNAPSHOT_CHUNK = 50


def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _classify_session(time_key: str) -> str:
    """分足の時刻(ET/市場ローカル)を PRE / REGULAR / AFTER に分類する。"""
    try:
        h = int(time_key[11:13])
        m = int(time_key[14:16])
        t = h * 60 + m
    except Exception:
        return "OTHER"
    if 4 * 60 <= t < 9 * 60 + 30:
        return "PRE"
    if 9 * 60 + 30 <= t < 16 * 60:
        return "REGULAR"
    if 16 * 60 <= t < 20 * 60:
        return "AFTER"
    return "OTHER"


def _ret_pct(cur: float, base: float) -> Optional[float]:
    """陽線率 = (現在値 - 始値) / 始値 * 100。基準0/欠損なら None。プラス=陽線。"""
    return round((cur - base) / base * 100.0, 3) if (base and cur) else None


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
        yosen: bool = True,
        yosen_interval: int = 30,
        yosen_ktype=None,
        display_top: Optional[int] = None,
    ):
        self.market_name = market.upper()
        self.top = top
        # 表示件数（ATH比の上位 display_top 件に絞る。None=全件）
        self.display_top = display_top
        self.host = host
        self.port = port
        self.extended_time = extended_time
        # 陽線率: 当日分足を定期取得して算出（既定 5分足・30秒毎）
        self.yosen = yosen
        self.yosen_interval = yosen_interval
        self.yosen_ktype = yosen_ktype or (ft.KLType.K_5M if ft else None)

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
                # 前日比（現在値 − 前日終値）
                prev_close = s.get("prev_close") or 0.0
                change_val = (cur - prev_close) if prev_close else None
                change_rate = (change_val / prev_close * 100.0) if change_val is not None else None
                # 陽線率 = (現在値 - 始値)/始値。セッション別。
                yosen_pre = _ret_pct(s.get("pre") or 0.0, s.get("pre_open") or 0.0)
                yosen_reg = _ret_pct(s.get("last") or 0.0, s.get("reg_open") or 0.0)
                yosen_total = _ret_pct(cur, s.get("day_open") or 0.0)
                rows.append(
                    {
                        "code": code,
                        "name": s.get("name", ""),
                        "industry": s.get("industry", ""),
                        "cur": round(cur, 4),
                        "ath": round(eff_ath, 4),
                        "orig_ath": round(ath, 4),
                        "high": round(high, 4),
                        "pct": round(pct, 4),
                        "change": round(change_val, 4) if change_val is not None else None,
                        "change_rate": round(change_rate, 4) if change_rate is not None else None,
                        "yosen_pre": yosen_pre,
                        "yosen_reg": yosen_reg,
                        "yosen_total": yosen_total,
                        "turnover": s.get("turnover"),
                        "turnover_rank": s.get("turnover_rank"),
                        "is_new_ath": is_new_ath,
                        "ath_updated": bool(s.get("ath_updated")),
                        "update_time": s.get("update_time", ""),
                    }
                )
        rows.sort(key=lambda x: x["pct"], reverse=True)
        # ATH比の上位 display_top 件に絞る（監視は全銘柄のまま、表示のみ限定）
        if self.display_top:
            rows = rows[: self.display_top]
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
                        "industry": "",
                        "ath": _f(r.get("highest_history_price")),
                        "ath_updated": False,   # 実行中に当日高値が起動時ATHを超えたら True（以後保持）
                        "prev_close": _f(r.get("prev_close_price")),
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

        # 2b) 業種（所属INDUSTRYプレート）を取得して付与
        industries = self._fetch_industries(quote_ctx, codes)
        with self._lock:
            for code, ind in industries.items():
                if code in self._state:
                    self._state[code]["industry"] = ind

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

        # 陽線率の定期取得（当日分足）をバックグラウンドで開始
        if self.yosen and self.yosen_ktype is not None:
            threading.Thread(target=self._yosen_loop, daemon=True).start()

        # 接続維持しつつ、セッション状態を定期更新
        while not self._stop.is_set():
            time.sleep(3)
            self._refresh_session()
        return True

    # ---- 陽線率（当日分足） ------------------------------------------ #
    def _yosen_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._refresh_yosen()
            except Exception as exc:  # pragma: no cover
                print(f"[ath] 陽線率の更新エラー: {exc}")
            self._stop.wait(self.yosen_interval)

    def _refresh_yosen(self) -> None:
        """当日分足から各セッションの始値（プレ始値・当日始値・レギュラー始値）を取得する。

        陽線率 = (現在値 - 始値)/始値 は get_ranking で現在値と突き合わせて算出する。
        レギュラー始値は snapshot の open_price を優先し、無ければ分足から補う。
        """
        if self._quote_ctx is None:
            return
        # JST基準で前後1日を範囲指定すれば、ETの当該取引日を確実に含む
        now = datetime.now()
        start = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        end = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        for code in list(self._codes):
            if self._stop.is_set():
                break
            try:
                ret, kl, _ = self._quote_ctx.request_history_kline(
                    code, start=start, end=end,
                    ktype=self.yosen_ktype, autype=ft.AuType.NONE,
                    max_count=1000, extended_time=True,
                )
                if ret != ft.RET_OK or kl is None or len(kl) == 0:
                    self._stop.wait(0.3)
                    continue
                # 最新の取引日のみを対象にする（分足は時刻昇順）
                latest = max(str(t)[:10] for t in kl["time_key"])
                pre_open = None
                reg_open = None
                day_open = None
                for _, r in kl.iterrows():
                    tk = str(r["time_key"])
                    if tk[:10] != latest:
                        continue
                    o = _f(r["open"])
                    if day_open is None:
                        day_open = o
                    sess = _classify_session(tk)
                    if sess == "PRE" and pre_open is None:
                        pre_open = o
                    elif sess == "REGULAR" and reg_open is None:
                        reg_open = o
                with self._lock:
                    s = self._state.get(code)
                    if s is not None:
                        # すべて「当日の」始値。未到来のセッションは 0（=陽線率は空）
                        s["pre_open"] = pre_open or 0.0
                        s["reg_open"] = reg_open or 0.0
                        s["day_open"] = day_open or 0.0
            except Exception:
                pass
            self._stop.wait(0.3)

    def _fetch_industries(self, quote_ctx, codes) -> Dict[str, str]:
        """各銘柄の所属 INDUSTRY プレート名（＝業種）を取得する。取得失敗は空扱い。"""
        out: Dict[str, str] = {}
        try:
            for chunk in _chunked(codes, SNAPSHOT_CHUNK):
                ret, d = quote_ctx.get_owner_plate(chunk)
                if ret != ft.RET_OK:
                    continue
                for _, r in d.iterrows():
                    if str(r.get("plate_type", "")).upper() != "INDUSTRY":
                        continue
                    code = r.get("code")
                    if code and code not in out:
                        out[code] = str(r.get("plate_name", "") or "")
        except Exception as exc:  # pragma: no cover
            print(f"[ath] 業種取得をスキップ: {exc}")
        return out

    def _refresh_session(self) -> None:
        if self._quote_ctx is None:
            return
        try:
            ret, gs = self._quote_ctx.get_global_state()
            if ret == ft.RET_OK:
                key = f"market_{self.market_name.lower()}"
                raw = gs.get(key) or gs.get("market_us") or ""
                new_bucket = _session_bucket(str(raw))
                old_bucket = self._session
                self._session_raw = str(raw)
                self._session = new_bucket
                if old_bucket != new_bucket:
                    if new_bucket == "REGULAR":
                        # レギュラー開始＝新しい取引日: 当日高値とフラグをリセット
                        self._reset_daily(reset_high=True)
                    elif old_bucket == "REGULAR":
                        # 大引け(取引終了): ATH更新フラグをリセット（当日高値表示は維持）
                        self._reset_daily(reset_high=False)
        except Exception:
            pass

    def _reset_daily(self, reset_high: bool) -> None:
        """ATH更新フラグ（と任意で当日高値）をリセットする。"""
        with self._lock:
            for s in self._state.values():
                s["ath_updated"] = False
                if reset_high:
                    s["high"] = 0.0
        label = "レギュラー開始" if reset_high else "取引終了(大引け)"
        print(f"[ath] {label}: ATH更新フラグをリセットしました")

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
            prev_close = _f(_rowget(row, "prev_close_price"))
            if prev_close:
                s["prev_close"] = prev_close
            if last:
                s["last"] = last
            # 当日高値は単調に更新（realtime の high_price を採用しつつ後退させない）
            if high:
                s["high"] = max(s.get("high") or 0.0, high)
            # レギュラーセッション中に当日高値が起動時ATHを超えたら「ATH更新」を確定。
            # 米国レギュラー終了(大引け)で _refresh_session がリセットする。
            if self._session == "REGULAR" and s.get("ath") and high > s["ath"]:
                s["ath_updated"] = True
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
        mock_industry = ["半導体", "ソフトウェア", "ネット・小売", "自動車", "民生用電子製品"]
        with self._lock:
            for code, name in sample:
                ath = round(random.uniform(100, 1000), 2)
                last = round(ath * random.uniform(0.4, 0.99), 2)
                mock_open = round(last * random.uniform(0.97, 1.03), 2)
                self._state[code] = {
                    "name": name, "industry": random.choice(mock_industry),
                    "ath": ath, "ath_updated": False,
                    "prev_close": round(last * random.uniform(0.97, 1.03), 2),
                    "reg_open": mock_open, "day_open": mock_open, "pre_open": 0.0,
                    "last": last, "high": last,
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
                    if s.get("ath") and s["high"] > s["ath"]:
                        s["ath_updated"] = True
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
