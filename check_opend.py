"""moomoo OpenD 接続・実データ取得の診断ツール。

Web ツール (app.py) で実データが出ない場合、まずこれを実行して
どの段階で失敗しているかを切り分けます。

使い方:
    python check_opend.py                    # 9984 でテスト
    python check_opend.py --code 7203
    python check_opend.py --host 127.0.0.1 --port 11111
    python check_opend.py --watch 10        # 10秒間プッシュ配信を観測

チェック内容:
    [1] OpenD のポートに TCP 接続できるか
    [2] SDK (moomoo-api / futu-api) がインストールされているか
    [3] OpenD の状態 (バージョン / 相場サーバーへのログイン状況)
    [4] 日本市場の状態 (取引時間内か)
    [5] 銘柄のリアルタイムクォート購読・取得
    [6] (--watch) リアルタイムプッシュ配信の受信
"""

from __future__ import annotations

import argparse
import socket
import sys
import time


def ok(msg: str) -> None:
    print(f"  ✅ {msg}")


def ng(msg: str) -> None:
    print(f"  ❌ {msg}")


def info(msg: str) -> None:
    print(f"     {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description="moomoo OpenD 接続診断")
    parser.add_argument("--code", default="9984", help="テストする銘柄コード (デフォルト: 9984)")
    parser.add_argument("--host", default="127.0.0.1", help="OpenD のホスト")
    parser.add_argument("--port", type=int, default=11111, help="OpenD のポート")
    parser.add_argument("--watch", type=int, default=0, metavar="SEC", help="指定秒数プッシュ配信を観測する")
    args = parser.parse_args()

    code = args.code.strip().upper()
    if "." not in code:
        code = f"JP.{code}"

    print(f"=== moomoo OpenD 診断 ({args.host}:{args.port}, 銘柄: {code}) ===\n")

    # [1] TCP 到達性 ------------------------------------------------------
    print("[1] OpenD ポート到達性")
    try:
        with socket.create_connection((args.host, args.port), timeout=3):
            pass
        ok(f"{args.host}:{args.port} に TCP 接続できました")
    except OSError as exc:
        ng(f"{args.host}:{args.port} に接続できません ({exc})")
        info("→ OpenD が起動しているか確認してください。")
        info("   ダウンロード: https://www.moomoo.com/download/OpenAPI")
        info("   OpenD 起動後、moomoo ID でログインが完了している必要があります。")
        return 1

    # [2] SDK -------------------------------------------------------------
    print("[2] SDK インストール確認")
    sdk = None
    try:
        import moomoo as ft  # type: ignore

        sdk = "moomoo-api"
    except ImportError:
        try:
            import futu as ft  # type: ignore

            sdk = "futu-api"
        except ImportError:
            ng("moomoo-api / futu-api のどちらもインストールされていません")
            info("→ pip install moomoo-api")
            return 1
    ok(f"SDK: {sdk}")

    # [3] OpenD 状態 -------------------------------------------------------
    print("[3] OpenD 状態")
    quote_ctx = ft.OpenQuoteContext(host=args.host, port=args.port)
    try:
        ret, state = quote_ctx.get_global_state()
        if ret != ft.RET_OK:
            ng(f"get_global_state 失敗: {state}")
            return 1
        ok(f"OpenD バージョン: {state.get('server_ver', '?')}")
        logined = state.get("qot_logined", state.get("Qot_Logined", "?"))
        if str(logined).lower() in ("true", "1"):
            ok("相場サーバーへログイン済み")
        else:
            ng(f"相場サーバー未ログイン (qot_logined={logined})")
            info("→ OpenD 側で moomoo ID のログインを完了してください。")

        # [4] 日本市場の状態
        print("[4] 日本市場の状態")
        market_jp = state.get("market_jp", state.get("Market_JP", None))
        if market_jp is not None:
            info(f"日本市場: {market_jp}")
            if str(market_jp).upper() in ("CLOSED", "NONE", "CLOSE"):
                info("→ 取引時間外です。価格は直近値のまま更新されない場合があります。")
        else:
            info("日本市場の状態を取得できませんでした（SDKバージョンによりキー名が異なります）")

        # [5] 購読と取得 ---------------------------------------------------
        print(f"[5] {code} のリアルタイムクォート購読")
        ret, data = quote_ctx.subscribe([code], [ft.SubType.QUOTE])
        if ret != ft.RET_OK:
            ng(f"購読失敗: {data}")
            info("→ 考えられる原因:")
            info("   - 日本株のリアルタイム相場権限が口座に無い")
            info("     (moomoo アプリ > 市場データ で権限を確認)")
            info("   - 購読上限 (デフォルト500件) の超過")
            info("   - 銘柄コードの誤り")
            return 1
        ok("購読成功")

        ret, quote = quote_ctx.get_stock_quote([code])
        if ret != ft.RET_OK:
            ng(f"クォート取得失敗: {quote}")
            return 1
        row = quote.iloc[0]
        ok("クォート取得成功:")
        info(f"銘柄名   : {row.get('stock_name', '?')}")
        info(f"現在値   : {row.get('last_price', '?')}")
        info(f"前日終値 : {row.get('prev_close_price', '?')}")
        info(f"始値     : {row.get('open_price', '?')}")
        info(f"高値/安値: {row.get('high_price', '?')} / {row.get('low_price', '?')}")
        info(f"出来高   : {row.get('volume', '?')}")
        info(f"時刻     : {row.get('data_date', '?')} {row.get('data_time', '?')}")

        # [6] プッシュ観測 ---------------------------------------------------
        if args.watch > 0:
            print(f"[6] {args.watch} 秒間プッシュ配信を観測")
            received = {"n": 0}

            class _Handler(ft.StockQuoteHandlerBase):
                def on_recv_rsp(self, rsp_pb):
                    ret_code, d = super().on_recv_rsp(rsp_pb)
                    if ret_code == ft.RET_OK:
                        for _, r in d.iterrows():
                            received["n"] += 1
                            print(
                                f"     push #{received['n']}: {r['code']} "
                                f"last={r['last_price']} vol={r['volume']} "
                                f"{r.get('data_time', '')}"
                            )
                    return ret_code, d

            quote_ctx.set_handler(_Handler())
            time.sleep(args.watch)
            if received["n"] > 0:
                ok(f"{received['n']} 件のプッシュを受信しました（リアルタイム配信 OK）")
            else:
                info("プッシュ受信なし。取引時間外か、値動きが無い可能性があります。")

        print("\n=== 診断完了: 実データ取得が可能な状態です ===")
        print("→ `python app.py --codes 9984` で Web 画面を起動してください。")
        return 0
    finally:
        quote_ctx.close()


if __name__ == "__main__":
    sys.exit(main())
