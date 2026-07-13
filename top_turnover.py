"""前日（直近取引日）の売買代金上位N銘柄を取得して出力する。

moomoo OpenD のスクリーナー API (get_stock_filter) を使い、
指定市場（デフォルト: 米国）を売買代金 (TURNOVER) の降順で並べて
上位N件を取得します。

売買代金 (TURNOVER) は SDK 内部で「累積系フィールド」に分類されるため、
SimpleFilter ではなく AccumulateFilter（days 指定必須）で指定します。

売買代金 = TURNOVER は「その取引日に約定した金額の合計（株価×出来高の累計）」。
米国市場の取引終了後〜翌日の場に入るまでに実行すると、直近取引日（＝前日）の
確定値が得られます。

使い方:
    python top_turnover.py                     # 米国 上位100
    python top_turnover.py --market US --top 100
    python top_turnover.py --top 50 --csv us_top50.csv
    python top_turnover.py --host 127.0.0.1 --port 11111

出力:
    順位 / コード / 銘柄名 / 現在値 / 売買代金 を表形式で標準出力に表示。
    --csv を付けると CSV ファイルにも保存します。
"""

from __future__ import annotations

import argparse
import csv
import sys

try:
    import moomoo as ft
except ImportError:
    try:
        import futu as ft  # type: ignore
    except ImportError:
        print("SDK が見つかりません。`python -m pip install moomoo-api` を実行してください。")
        sys.exit(1)


# スクリーナーの1回の取得上限（SDK 仕様）
MAX_PER_PAGE = 200


def human_money(v: float) -> str:
    """売買代金を読みやすい単位（B=十億 / M=百万）に整形する。"""
    if v is None:
        return "-"
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:,.2f} B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:,.2f} M"
    return f"{v:,.0f}"


def resolve_market(name: str):
    """'US' / 'HK' / 'JP' などの文字列を Market enum に変換する。"""
    name = name.strip().upper()
    market = getattr(ft.Market, name, None)
    if market is None:
        valid = [m for m in dir(ft.Market) if m.isupper() and not m.startswith("_")]
        print(f"未対応の市場 '{name}'。利用可能: {', '.join(valid)}")
        sys.exit(1)
    return market


def fetch_top(quote_ctx, market, top: int):
    """売買代金の降順で上位 top 件を取得する。

    Returns: list[dict]  (rank, code, name, turnover, last_price)
    """
    # 売買代金でソートするフィルタ。
    # 重要: TURNOVER(売買代金) は SDK 内部で「累積系フィールド」に分類されており、
    # SimpleFilter では扱えず AccumulateFilter を使う必要がある。
    # （SimpleFilter に入れるとサーバが「このフィルターフィールドには対応していません」を返す。）
    # days=1 で直近取引日（＝前日）の売買代金を対象にする。
    # is_no_filter=False（フィルタ有効）にする場合、範囲値(min/max)の指定が必須。
    # 下限を 0 にすることで実質「全銘柄」を対象にしつつ降順ソートを行う。
    turnover_filter = ft.AccumulateFilter()
    turnover_filter.stock_field = ft.StockField.TURNOVER
    turnover_filter.days = 1                   # 直近1取引日（前日）の売買代金
    turnover_filter.is_no_filter = False
    turnover_filter.filter_min = 0             # 下限0＝実質無制限（範囲値必須のため設定）
    turnover_filter.sort = ft.SortDir.DESCEND  # 降順（大きい順）

    # 現在値も出力に含めるためのフィルタ（絞り込みはせず、値の取得のみ）
    price_filter = ft.SimpleFilter()
    price_filter.stock_field = ft.StockField.CUR_PRICE
    price_filter.is_no_filter = True           # フィルタリングはしない（フィールドを出力に含めるだけ）

    results: list = []
    begin = 0
    while len(results) < top:
        num = min(MAX_PER_PAGE, top - len(results))
        ret, data = quote_ctx.get_stock_filter(
            market=market,
            filter_list=[turnover_filter, price_filter],
            begin=begin,
            num=num,
        )
        if ret != ft.RET_OK:
            print(f"スクリーナー取得に失敗しました: {data}")
            print("→ 米国相場の権限、または OpenD のログイン状態を確認してください。")
            sys.exit(1)

        last_page, all_count, ret_list = data
        if not ret_list:
            break

        for item in ret_list:
            # フィルタ値はフィルタオブジェクトをキーにして取り出す
            def val(flt):
                try:
                    return item[flt]
                except Exception:
                    return None

            results.append(
                {
                    "code": getattr(item, "stock_code", ""),
                    "name": getattr(item, "stock_name", ""),
                    "turnover": val(turnover_filter),
                    "last_price": val(price_filter),
                }
            )

        begin += len(ret_list)
        if last_page or len(ret_list) < num:
            break

    # 順位を付与
    for i, row in enumerate(results[:top], start=1):
        row["rank"] = i
    return results[:top]


def print_table(rows: list, market_name: str) -> None:
    print(f"\n=== {market_name} 売買代金 上位 {len(rows)} 銘柄（直近取引日） ===\n")
    header = f"{'順位':>3}  {'コード':<12} {'銘柄名':<28} {'現在値':>10}  {'売買代金':>14}"
    print(header)
    print("-" * len(header.encode("ascii", "ignore")) if False else "-" * 78)
    for r in rows:
        name = (r["name"] or "")[:26]
        price = r["last_price"]
        price_s = f"{price:,.2f}" if isinstance(price, (int, float)) else "-"
        print(
            f"{r['rank']:>3}  {r['code']:<12} {name:<28} "
            f"{price_s:>10}  {human_money(r['turnover']):>14}"
        )
    print()


def save_csv(rows: list, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "code", "name", "last_price", "turnover"])
        for r in rows:
            writer.writerow([r["rank"], r["code"], r["name"], r["last_price"], r["turnover"]])
    print(f"CSV を保存しました: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="売買代金上位N銘柄の取得")
    parser.add_argument("--market", default="US", help="市場 (US / HK / JP など。デフォルト: US)")
    parser.add_argument("--top", type=int, default=100, help="取得件数（デフォルト: 100）")
    parser.add_argument("--host", default="127.0.0.1", help="OpenD ホスト")
    parser.add_argument("--port", type=int, default=11111, help="OpenD ポート")
    parser.add_argument("--csv", metavar="PATH", help="CSV 出力先パス（任意）")
    args = parser.parse_args()

    market = resolve_market(args.market)

    quote_ctx = ft.OpenQuoteContext(host=args.host, port=args.port)
    try:
        rows = fetch_top(quote_ctx, market, args.top)
    finally:
        quote_ctx.close()

    if not rows:
        print("データを取得できませんでした。")
        return 1

    print_table(rows, args.market.upper())
    if args.csv:
        save_csv(rows, args.csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
