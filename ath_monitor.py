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

import json
import math
import os
import queue
import random
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import top_turnover as tt  # fetch_top / resolve_market / ft を再利用

ft = tt.ft

# get_market_snapshot の1回あたり銘柄数（安全側の分割サイズ）
SNAPSHOT_CHUNK = 50

# ユニバース（売買代金上位）の保存ファイル名テンプレ
UNIVERSE_FILE_TMPL = "universe_{market}.json"
# ATH更新フラグ・基準ATHの保存ファイル名テンプレ（同一取引日なら再起動で復元）
ATH_STATE_FILE_TMPL = "ath_state_{market}.json"


def _et_date() -> str:
    """米国東部時間の日付（取引日キー）。zoneinfo優先、無ければ夏時間を近似。"""
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        from datetime import timezone

        now = datetime.now(timezone.utc)
        offset = 4 if 3 <= now.month <= 11 else 5  # 3-11月は概ねEDT(-4)
        return (now - timedelta(hours=offset)).strftime("%Y-%m-%d")


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
    """OpenD の市場ステート文字列を PRE / REGULAR / AFTER / OVERNIGHT / CLOSED に分類する。

    注意: "AFTERNOON"(レギュラー午後) に "AFTER" が含まれるため、レギュラーを先に判定する。
    """
    s = (market_state or "").upper()
    # レギュラーセッション（米国は MORNING / AFTERNOON。他に TRADING/REGULAR）
    if s in ("MORNING", "AFTERNOON", "TRADING", "REGULAR"):
        return "REGULAR"
    if "PRE_MARKET" in s or s == "PRE":
        return "PRE"
    if "AFTER_HOURS" in s:
        # AFTER_HOURS_BEGIN=大引け後の時間外, AFTER_HOURS_END=時間外終了(=クローズ扱い)
        return "CLOSED" if "END" in s else "AFTER"
    if "OVERNIGHT" in s or "NIGHT" in s:
        return "OVERNIGHT"
    # CLOSED / REST / WAITING_OPEN / AUCTION / TRADE_AT_LAST / NONE など
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
        refresh_universe: bool = False,
        notifier=None,
        notify_cooldown: int = 3600,
    ):
        self.market_name = market.upper()
        self.top = top
        # ユニバース（売買代金上位N）を毎回取り直すか。False なら保存分を再利用。
        self.refresh_universe = refresh_universe
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
        self._state_dirty = False   # ATH状態を保存すべき変更があったか
        self._session_ready = False # 初回セッション判定を済ませたか（起動直後の誤リセット防止）
        # 通知（ATH更新を Discord 等へ。同一銘柄は notify_cooldown 秒は再通知しない）
        self.notifier = notifier
        self.notify_cooldown = notify_cooldown
        self._notify_queue: "queue.Queue[dict]" = queue.Queue(maxsize=500)

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

    def get_ranking(self, sort: str = "pct") -> List[dict]:
        """現在の状態からランキングを組み立てて返す。

        sort="pct"（既定）: ATH接近率の降順。
        sort="turnover": 前日売買代金の順位（昇順）。
        sort="turnover_today": 当日売買代金（プレ+レギュラー+アフター合計）の降順。
        いずれも display_top 件に絞る。
        """
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
                # 当日売買代金 = セッション別（プレ+レギュラー+アフター）の合計
                turnover_today = (
                    (s.get("turnover_pre") or 0.0)
                    + (s.get("turnover_reg") or 0.0)
                    + (s.get("turnover_after") or 0.0)
                )
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
                        "turnover_pre": s.get("turnover_pre"),
                        "turnover_reg": s.get("turnover_reg"),
                        "turnover_after": s.get("turnover_after"),
                        "turnover_today": turnover_today,
                        "market_cap": s.get("market_cap") or None,
                        "market_cap_rank": s.get("market_cap_rank"),
                        "is_new_ath": is_new_ath,
                        "ath_updated": bool(s.get("ath_updated")),
                        "prev_new": s.get("prev_new"),
                        "update_time": s.get("update_time", ""),
                    }
                )
        if sort == "turnover":
            # 前日売買代金の順位（昇順）。順位が無いものは末尾。
            rows = [r for r in rows if r.get("turnover_rank")]
            rows.sort(key=lambda x: x["turnover_rank"])
        elif sort == "turnover_today":
            # 当日売買代金の降順。同額（=0など）は前日売買代金順位で安定ソート。
            rows.sort(key=lambda x: (-(x.get("turnover_today") or 0.0),
                                     x.get("turnover_rank") or 10**9))
        else:
            rows.sort(key=lambda x: x["pct"], reverse=True)
        # 上位 display_top 件に絞る（監視は全銘柄のまま、表示のみ限定）
        if self.display_top:
            rows = rows[: self.display_top]
        for i, r in enumerate(rows, start=1):
            r["rank"] = i
        # 念のため JSON 規格外の値(NaN/Inf)を落としてから返す（画面停止の防止）
        return [_json_safe(r) for r in rows]

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

        # 1) 売買代金上位N（ユニバース）: 保存分があれば再利用（前日分を固定運用）
        market = tt.resolve_market(self.market_name)
        rows = self._load_or_fetch_universe(quote_ctx, market)
        if not rows:
            print("[ath] 売買代金上位の取得結果が空。モックモードで起動します。")
            quote_ctx.close()
            self._quote_ctx = None
            return False
        codes = [r["code"] for r in rows]
        turnover_map = {r["code"]: r.get("turnover") for r in rows}
        # fetch_top は売買代金の降順で rank を採番済み（=売買代金順位）
        turnover_rank_map = {r["code"]: r.get("rank") for r in rows}

        # 2) ATH等の初期値を snapshot で取得して state を構築 → 業種付与 → 差し替え
        new_state = self._build_state(quote_ctx, codes, turnover_map, turnover_rank_map)
        if new_state is None:
            print("[ath] snapshot 取得失敗。モックモードで起動します。")
            quote_ctx.close()
            self._quote_ctx = None
            return False
        industries = self._fetch_industries(quote_ctx, codes)
        for code, ind in industries.items():
            if code in new_state:
                new_state[code]["industry"] = ind
        with self._lock:
            self._state = new_state
            self._codes = codes

        # 2c) 同一取引日ならATH更新フラグ・基準ATHを復元（再起動でハイライトが消えないように）
        self._restore_ath_state()

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

        # 通知スレッド開始＋起動通知（Webhook疎通確認を兼ねる）
        if self.notifier is not None:
            threading.Thread(target=self._notify_loop, daemon=True).start()
            try:
                self.notifier.send_text(
                    f"✅ ATHモニター開始: {self.market_name} 上位{len(codes)}銘柄を監視中"
                )
            except Exception as exc:
                print(f"[ath] 起動通知の送信に失敗（Webhook設定を確認）: {exc}")

        # 接続維持しつつ、セッション状態を定期更新。変更があればATH状態を保存。
        while not self._stop.is_set():
            time.sleep(3)
            self._refresh_session()
            if self._state_dirty:
                self._state_dirty = False
                self._save_ath_state()
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
                # 当日(米国東部日付)の分足でなければ陽線率は出さない。
                # （引け後〜翌寄り前は履歴に前営業日しか無く、前日値を今日として
                #   誤表示してしまうのを防ぐ）
                if latest != _et_date():
                    with self._lock:
                        s = self._state.get(code)
                        if s is not None:
                            s["pre_open"] = 0.0
                            s["reg_open"] = 0.0
                            s["day_open"] = 0.0
                    self._stop.wait(0.3)
                    continue
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

    def _build_state(self, quote_ctx, codes, turnover_map, turnover_rank_map) -> Optional[dict]:
        """snapshot から各銘柄の初期 state 辞書を構築して返す（失敗時 None）。"""
        state: dict = {}
        for chunk in _chunked(codes, SNAPSHOT_CHUNK):
            ret, data = quote_ctx.get_market_snapshot(chunk)
            if ret != ft.RET_OK:
                print(f"[ath] snapshot 取得失敗: {data}")
                return None
            for _, r in data.iterrows():
                code = r["code"]
                ath0 = _f(r.get("highest_history_price"))
                high0 = _f(r.get("high_price"))
                state[code] = {
                    "name": r.get("name", "") or "",
                    "industry": "",
                    "ath": ath0,
                    # 前日ATH更新: 直近レギュラー高値(起動/引け時のsnapshot高値)がATHに到達。
                    # 起動が寄り前なら前営業日、引け後の入替時はその日の結果＝翌日の「前日」。
                    "prev_new": bool(ath0 and high0 and high0 >= ath0 * 0.9999),
                    "ath_updated": False,   # 当日高値が基準ATHを超えたら True（大引けまで保持）
                    "prev_close": _f(r.get("prev_close_price")),
                    "market_cap": _f(r.get("total_market_val")),
                    "last": _f(r.get("last_price")),
                    "high": _f(r.get("high_price")),
                    "pre": _f(r.get("pre_price")),
                    "after": _f(r.get("after_price")),
                    "overnight": _f(r.get("overnight_price")),
                    "turnover": turnover_map.get(code),
                    "turnover_rank": turnover_rank_map.get(code),
                    # セッション別の当日売買代金（realtimeで更新、日次リセット）
                    "turnover_pre": 0.0,
                    "turnover_reg": 0.0,
                    "turnover_after": 0.0,
                    "update_time": str(r.get("update_time", "") or ""),
                }
        # 時価総額順位を採番（ユニバース内を時価総額の降順で）
        for i, (code, s) in enumerate(
            sorted(state.items(), key=lambda kv: kv[1].get("market_cap") or 0, reverse=True),
            start=1,
        ):
            s["market_cap_rank"] = i
        return state

    def _swap_universe(self) -> None:
        """引け後に新ユニバースへ入れ替える（稼働中に再取得・再購読）。

        _refresh_session の大引け遷移から呼ばれる。ユニバースファイルは直前に
        無効化済みなので取り直しになり、days=1 は当日確定売買代金になる。
        """
        quote_ctx = self._quote_ctx
        if quote_ctx is None:
            return
        print("[ath] 引け後: 新ユニバースへ入れ替えます...")
        market = tt.resolve_market(self.market_name)
        old_codes = list(self._codes)
        try:
            rows = self._load_or_fetch_universe(quote_ctx, market)
        except SystemExit:
            rows = None
        except Exception as exc:
            print(f"[ath] 新ユニバース取得エラー: {exc}")
            rows = None
        if not rows:
            print("[ath] 新ユニバース取得失敗。現状のまま継続します。")
            return
        codes = [r["code"] for r in rows]
        turnover_map = {r["code"]: r.get("turnover") for r in rows}
        turnover_rank_map = {r["code"]: r.get("rank") for r in rows}

        new_state = self._build_state(quote_ctx, codes, turnover_map, turnover_rank_map)
        if new_state is None:
            print("[ath] 新ユニバースの snapshot 失敗。現状のまま継続します。")
            return
        industries = self._fetch_industries(quote_ctx, codes)
        for code, ind in industries.items():
            if code in new_state:
                new_state[code]["industry"] = ind

        # state を差し替え（ロック下で原子的に）
        with self._lock:
            self._state = new_state
            self._codes = codes
        # 新しい取引日の基準としてATH状態を保存（フラグ空・新baseline）
        self._save_ath_state()

        # 購読を入れ替え（差分のみ）
        new_set, old_set = set(codes), set(old_codes)
        to_unsub = [c for c in old_codes if c not in new_set]
        to_sub = [c for c in codes if c not in old_set]
        try:
            if to_unsub:
                quote_ctx.unsubscribe(to_unsub, [ft.SubType.QUOTE])
            if to_sub:
                quote_ctx.subscribe(to_sub, [ft.SubType.QUOTE], extended_time=self.extended_time)
        except Exception as exc:
            print(f"[ath] 購読の入れ替えエラー: {exc}")
        print(f"[ath] 新ユニバースへ入れ替え完了: {len(codes)}銘柄（解除{len(to_unsub)}/追加{len(to_sub)}）")

    def _universe_path(self) -> str:
        return UNIVERSE_FILE_TMPL.format(market=self.market_name)

    def _invalidate_universe(self) -> None:
        """保存済みユニバースを無効化（削除）し、次回起動時に取り直させる。

        US大引けで呼ぶ。稼働中インスタンスの監視銘柄は変えず（顔ぶれ維持）、
        次回起動時に「その取引日の確定売買代金」で取り直す。
        """
        path = self._universe_path()
        try:
            if os.path.exists(path):
                os.remove(path)
                print("[ath] 取引終了: ユニバースを無効化（次回起動で取り直し）")
        except Exception as exc:
            print(f"[ath] ユニバース無効化に失敗: {exc}")

    def _load_or_fetch_universe(self, quote_ctx, market) -> List[dict]:
        """ユニバース（売買代金上位N）を取得する。

        保存ファイルがあり、当日(米国東部日付)に取得済みで --refresh-universe 指定が
        無ければ、それを再利用する（＝再起動しても顔ぶれが変わらない）。
        無ければ取得して保存する。US大引けで無効化 or 日付が変われば取り直す。

        ※プレマーケット/引け後に実行すれば、screener の TURNOVER(days=1) は
          直近の完了取引日（＝前日）の確定売買代金になる。
        """
        path = self._universe_path()
        today = _et_date()   # 米国東部日付でキー（US大引け後〜翌日でリセット）

        if not self.refresh_universe and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                rows = data.get("rows") or []
                if (
                    data.get("market") == self.market_name
                    and data.get("date") == today
                    and len(rows) >= self.top
                ):
                    rows = rows[: self.top]
                    print(f"[ath] 保存済みユニバースを再利用: {path} (取得日 {data.get('date')}, {len(rows)}銘柄)")
                    return rows
                else:
                    print(f"[ath] 保存ユニバースは対象外（date={data.get('date')} 件数={len(rows)}）。取り直します。")
            except Exception as exc:
                print(f"[ath] ユニバース読込失敗（取り直します）: {exc}")

        rows = tt.fetch_top(quote_ctx, market, self.top)
        if rows:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(
                        {"market": self.market_name, "date": today, "rows": rows},
                        f, ensure_ascii=False,
                    )
                print(f"[ath] ユニバースを取得・保存: {path} ({len(rows)}銘柄, 取得日 {today})")
            except Exception as exc:
                print(f"[ath] ユニバース保存失敗: {exc}")
        return rows

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
                if not self._session_ready:
                    # 起動直後の初回判定ではリセットしない（復元済みフラグを守る）
                    self._session_ready = True
                elif old_bucket != new_bucket:
                    if new_bucket == "PRE":
                        # 新しい取引日のプレ開始: セッション別売買代金をリセット
                        self._reset_session_turnover()
                    if new_bucket == "REGULAR":
                        # レギュラー開始＝新しい取引日: 当日高値とフラグをリセット
                        self._reset_daily(reset_high=True)
                    elif old_bucket == "REGULAR":
                        # 大引け(取引終了): ATH更新フラグをリセット（当日高値表示は維持）
                        self._reset_daily(reset_high=False)
                        # ユニバースを無効化→その日確定の売買代金上位へ即入れ替え（再購読）
                        self._invalidate_universe()
                        self._swap_universe()
        except Exception:
            pass

    def _reset_daily(self, reset_high: bool) -> None:
        """ATH更新フラグ（と任意で当日高値）をリセットする。"""
        with self._lock:
            for s in self._state.values():
                s["ath_updated"] = False
                s["notified_at"] = 0.0   # 通知クールダウンもリセット
                if reset_high:
                    s["high"] = 0.0
        self._state_dirty = True   # リセット後の状態を保存（再起動で復活させない）
        label = "レギュラー開始" if reset_high else "取引終了(大引け)"
        print(f"[ath] {label}: ATH更新フラグをリセットしました")

    def _reset_session_turnover(self) -> None:
        """セッション別の当日売買代金を0にリセット（新しい取引日のプレ開始時）。"""
        with self._lock:
            for s in self._state.values():
                s["turnover_pre"] = 0.0
                s["turnover_reg"] = 0.0
                s["turnover_after"] = 0.0
        print("[ath] プレ開始: セッション別売買代金をリセットしました")

    # ---- ATH更新状態の永続化（同一取引日なら再起動で復元） ---------- #
    def _ath_state_path(self) -> str:
        return ATH_STATE_FILE_TMPL.format(market=self.market_name)

    def _restore_ath_state(self) -> None:
        path = self._ath_state_path()
        data = None
        if not self.refresh_universe and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    d = json.load(f)
                if d.get("market") == self.market_name and d.get("date") == _et_date():
                    data = d
            except Exception as exc:
                print(f"[ath] ATH状態の読込失敗: {exc}")
        if data:
            base = data.get("baseline") or {}
            upd = set(data.get("updated") or [])
            n = 0
            with self._lock:
                for code, s in self._state.items():
                    if base.get(code):
                        s["ath"] = base[code]   # 当日の基準ATHを復元（新高値検知の基準）
                    if code in upd:
                        s["ath_updated"] = True
                        n += 1
            print(f"[ath] ATH状態を復元（取引日 {data.get('date')}）: ATH更新済み {n}銘柄")
        else:
            # 新しい取引日 or 初回: 現在のATHを基準として保存
            self._save_ath_state()

    def _save_ath_state(self) -> None:
        path = self._ath_state_path()
        try:
            with self._lock:
                baseline = {c: s.get("ath") for c, s in self._state.items() if s.get("ath")}
                updated = [c for c, s in self._state.items() if s.get("ath_updated")]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {"market": self.market_name, "date": _et_date(),
                     "baseline": baseline, "updated": updated},
                    f, ensure_ascii=False,
                )
        except Exception as exc:
            print(f"[ath] ATH状態の保存失敗: {exc}")

    # ---- 通知（ATH更新を Discord 等へ） ------------------------------ #
    def _maybe_notify(self, code: str, s: dict) -> None:
        """新高値時に通知をキュー投入。同一銘柄は notify_cooldown 秒は抑制する。

        ※ _on_row のロック内から呼ばれる想定（キュー投入のみで送信はしない）。
        """
        if self.notifier is None:
            return
        now = time.time()
        if now - (s.get("notified_at") or 0.0) < self.notify_cooldown:
            return
        s["notified_at"] = now
        cur = s.get("last") or 0.0
        eff_ath = max(s.get("ath") or 0.0, s.get("high") or 0.0)
        prev_close = s.get("prev_close") or 0.0
        ev = {
            "code": code,
            "name": s.get("name"),
            "industry": s.get("industry"),
            "cur": cur,
            "ath": eff_ath,
            "change_rate": ((cur - prev_close) / prev_close * 100.0) if prev_close else None,
            "yosen_pre": _ret_pct(s.get("pre") or 0.0, s.get("pre_open") or 0.0),
            "yosen_reg": _ret_pct(cur, s.get("reg_open") or 0.0),
            "yosen_total": _ret_pct(cur, s.get("day_open") or 0.0),
            "turnover_rank": s.get("turnover_rank"),
            "market_cap": s.get("market_cap"),
            "market_cap_rank": s.get("market_cap_rank"),
        }
        try:
            self._notify_queue.put_nowait(ev)
        except queue.Full:
            pass

    def _notify_loop(self) -> None:
        """キューの通知を順次送信（Discordのレート制限に配慮して間隔を空ける）。"""
        while not self._stop.is_set():
            try:
                ev = self._notify_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                self.notifier.send_ath_update(ev)
            except Exception as exc:
                print(f"[ath] 通知送信エラー: {exc}")
            self._stop.wait(1.2)   # レート制限対策のペース

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
            if high and high > (s.get("high") or 0.0):
                s["high"] = high
                # レギュラー中に基準ATHを超えた「新高値」の瞬間
                if self._session == "REGULAR" and s.get("ath") and high > s["ath"]:
                    if not s.get("ath_updated"):
                        s["ath_updated"] = True
                        self._state_dirty = True   # 保存対象（実際の保存はメインループで）
                    # 通知（連続更新はクールダウンで抑制、経過後の更新は再度一度だけ）
                    self._maybe_notify(code, s)
            for fld, key in (("pre", "pre_price"), ("after", "after_price"), ("overnight", "overnight_price")):
                v = _f(_rowget(row, key))
                if v:
                    s[fld] = v
            # セッション別の当日売買代金。既に終えたセッション分は取り込みつつ、
            # まだ来ていないセッション（前日値が残る after 等）は取り込まない。
            sess = self._session
            if sess in ("PRE", "REGULAR", "AFTER"):
                tp = _f(_rowget(row, "pre_turnover"))
                if tp:
                    s["turnover_pre"] = tp
            if sess in ("REGULAR", "AFTER"):
                tr = _f(_rowget(row, "turnover"))
                if tr:
                    s["turnover_reg"] = tr
            if sess == "AFTER":
                ta = _f(_rowget(row, "after_turnover"))
                if ta:
                    s["turnover_after"] = ta
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
                    "market_cap": round(random.uniform(5e10, 4e12), 0),
                    "turnover_pre": round(random.uniform(1e7, 5e8), 0),
                    "turnover_reg": round(random.uniform(1e8, 5e9), 0),
                    "turnover_after": round(random.uniform(1e6, 2e8), 0),
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
            # 時価総額の降順で順位を採番
            for i, (code, s) in enumerate(
                sorted(self._state.items(), key=lambda kv: kv[1].get("market_cap") or 0, reverse=True),
                start=1,
            ):
                s["market_cap_rank"] = i

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
    """数値化。None / 変換不可 / NaN・Inf(欠損値) はすべて 0.0 にする。

    NaN をそのまま通すと jsonify が JSON 規格外の `NaN` を出力し、
    ブラウザ側の res.json() が例外になって画面が「読み込み中…」で止まる。
    """
    try:
        if v is None:
            return 0.0
        f = float(v)
        return f if math.isfinite(f) else 0.0
    except Exception:
        return 0.0


def _json_safe(d: dict) -> dict:
    """dict 内の NaN / Inf を None に置き換える（JSON 規格外の値を出さないため）。"""
    return {
        k: (None if isinstance(v, float) and not math.isfinite(v) else v)
        for k, v in d.items()
    }


def _rowget(row, key, default=None):
    try:
        v = row[key]
        return default if v is None else v
    except Exception:
        return default
