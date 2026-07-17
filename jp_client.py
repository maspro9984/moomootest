"""RSSPilot の dataapi(WebSocket) クライアント。

RSSPilotSv.js の DataClient として接続し、日本株のデータを取得する。
  Login(clientType=4) -> GetRanking / GetStockMaster / GetSnapshot / Subscribe

プロトコルは JSON の request/yourRequest 形式。Subscribe すると NotifyData
(または intervalMs>0 で NotifyDataBatch) が push されてくる。

このモジュールは受信をバックグラウンドスレッドで回し、値の更新をコールバックで
通知する（moomoo版の MoomooQuoteClient と同じ思想）。
"""

from __future__ import annotations

import json
import threading
import time
from typing import Callable, Dict, List, Optional

from websockets.sync.client import connect

# RSSPilotSv.js の ClientType.DataClient
CLIENT_TYPE_DATA = 4
SUBPROTOCOL = "echo-protocol"


class RssPilotClient:
    """RSSPilot dataapi への接続を保持し、request/response と push を扱う。

    リクエストは「送って、対応する応答が来るまで待つ」同期スタイルで提供する。
    受信スレッドが1本で回り、応答は待ち合わせ用の箱へ、push はコールバックへ流す。
    """

    def __init__(self, host: str, port: int = 23203, tag: str = "athjp"):
        self.host = host
        self.port = port
        self.tag = tag
        self._ws = None
        self._lock = threading.Lock()          # 送信の排他
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # 応答待ち: request名 -> {"ev": Event, "msg": dict}
        self._waiters: Dict[str, dict] = {}
        self._waiters_lock = threading.Lock()
        # push コールバック: (code, element_name, value) -> None
        self.on_data: Optional[Callable[[str, str, object], None]] = None
        self.connected = threading.Event()
        self.last_error = ""

    # ------------------------------------------------------------------ #
    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}/"

    def start(self) -> bool:
        """接続してログインし、受信スレッドを開始する。成功したら True。"""
        try:
            self._ws = connect(self.url, subprotocols=[SUBPROTOCOL], open_timeout=10)
        except Exception as exc:
            self.last_error = f"接続失敗: {exc}"
            print(f"[jp] {self.last_error}")
            return False

        self._thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()

        res = self.request("Login", {"clientType": CLIENT_TYPE_DATA, "tag": self.tag},
                           expect="LoginResult", timeout=10)
        if not res or not res.get("result"):
            self.last_error = f"ログイン失敗: {res}"
            print(f"[jp] {self.last_error}")
            return False
        self.connected.set()
        print(f"[jp] RSSPilot に接続しました ({self.url} sid={res.get('sid')})")
        return True

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    def _recv_loop(self) -> None:
        while not self._stop.is_set():
            try:
                raw = self._ws.recv(timeout=5)
            except TimeoutError:
                continue
            except Exception as exc:
                if not self._stop.is_set():
                    self.last_error = f"受信エラー: {exc}"
                    print(f"[jp] {self.last_error}")
                    self.connected.clear()
                break
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            self._dispatch(msg)

    def _dispatch(self, msg: dict) -> None:
        req = msg.get("request")
        # push 系
        if req == "NotifyData":
            if self.on_data:
                self.on_data(str(msg.get("code")), msg.get("element"), msg.get("value"))
            return
        if req == "NotifyDataBatch":
            if self.on_data:
                for item in msg.get("list") or []:
                    code = str(item.get("code"))
                    for name, value in (item.get("values") or {}).items():
                        self.on_data(code, name, value)
            return
        # 応答系: request名で待ち合わせを解除
        with self._waiters_lock:
            w = self._waiters.get(req)
            if w is not None:
                w["msg"] = msg
                w["ev"].set()

    # ------------------------------------------------------------------ #
    def send(self, payload: dict) -> bool:
        try:
            with self._lock:
                self._ws.send(json.dumps(payload, ensure_ascii=False))
            return True
        except Exception as exc:
            self.last_error = f"送信エラー: {exc}"
            print(f"[jp] {self.last_error}")
            return False

    def request(self, request: str, params: dict, expect: str, timeout: float = 15.0) -> Optional[dict]:
        """request を送り、expect という request名の応答が来るまで待って返す。"""
        ev = threading.Event()
        with self._waiters_lock:
            self._waiters[expect] = {"ev": ev, "msg": None}
        payload = {"request": request}
        payload.update(params)
        if not self.send(payload):
            with self._waiters_lock:
                self._waiters.pop(expect, None)
            return None
        ok = ev.wait(timeout)
        with self._waiters_lock:
            w = self._waiters.pop(expect, None)
        if not ok:
            self.last_error = f"{request} がタイムアウトしました"
            print(f"[jp] {self.last_error}")
            return None
        return w["msg"] if w else None

    # ---- 各API -------------------------------------------------------- #
    def get_ranking(self, kind: str = "volume", limit: int = 100) -> List[dict]:
        """前日ランキング。[{rank, code, name, value}] を返す。"""
        res = self.request("GetRanking", {"kind": kind, "limit": limit}, expect="RankingResult")
        if not res or not res.get("result"):
            return []
        return res.get("list") or []

    def get_stock_master(self, codes) -> Dict[str, dict]:
        """市場区分・業種。code -> {name, market, gyoshu33, ...} を返す。"""
        res = self.request("GetStockMaster", {"codes": codes}, expect="StockMasterResult")
        if not res or not res.get("result"):
            return {}
        return {str(r["code"]): r for r in (res.get("list") or [])}

    def get_snapshot(self, codes, elements) -> Dict[str, dict]:
        """スナップショット。code -> {element名: 値} を返す。"""
        res = self.request("GetSnapshot", {"codes": codes, "elements": elements},
                           expect="SnapshotResult")
        if not res or not res.get("result"):
            return {}
        return {str(r["code"]): (r.get("values") or {}) for r in (res.get("list") or [])}

    def subscribe(self, codes, elements, interval_ms: int = 0) -> bool:
        """購読開始。以後 on_data が呼ばれる。"""
        res = self.request("Subscribe",
                           {"codes": codes, "elements": elements, "intervalMs": interval_ms},
                           expect="SubscribeResult")
        return bool(res and res.get("result"))
