"""売買代金上位N銘柄を「上場来高値(ATH)への近さ」でランキングして出力する。

処理の流れ:
  1. スクリーナーで売買代金 (TURNOVER) 上位N銘柄を取得（top_turnover.py を再利用）
  2. その銘柄群を get_market_snapshot にかけ、上場来高値 highest_history_price と
     最新値 last_price を取得
  3. 近接率 = last_price / highest_history_price（%）の降順に並べる
     （100% に近いほど上場来高値に接近している）

「上場来高値」は 52週高値や n週平均高値ではなく、上場来の最高値（ATH）。
moomoo OpenD の snapshot が直接返すため、別データソースは不要。

使い方:
    python ath_ranking.py                      # 米国 売買代金上位100 → ATH接近ランキング
    python ath_ranking.py --market US --top 50
    python ath_ranking.py --top 100 --csv us_ath.csv
    python ath_ranking.py --min-pct 90         # ATH比90%以上だけ表示

想定運用:
    米国市場の引け後に実行すると、直近取引日の確定値でランキングできる。
"""

from __future__ import annotations

import argparse
import csv
import sys

import top_turnover as tt  # fetch_top / resolve_market / human_money / ft を再利用

ft = tt.ft

# get_market_snapshot の1回あたり銘柄数（安全側の分割サイズ）
SNAPSHOT_CHUNK = 50


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def fetch_ath(quote_ctx, codes):
    """コードのリストから上場来高値・最新値・銘柄名を取得する。

    Returns: dict[code] -> {"name", "last", "ath"}
    """
    out: dict = {}
    for chunk in chunked(codes, SNAPSHOT_CHUNK):
        ret, data = quote_ctx.get_market_snapshot(chunk)
        if ret != ft.RET_OK:
            print(f"スナップショット取得に失敗しました: {data}")
            print("→ OpenD のログイン状態、相場権限、リクエスト頻度制限を確認してください。")
            sys.exit(1)
        for _, r in data.iterrows():
            out[r["code"]] = {
                "name": r.get("name", ""),
                "last": r.get("last_price"),
                "ath": r.get("highest_history_price"),
            }
    return out


def build_ranking(rows, ath_map, min_pct):
    """売買代金上位 rows に ATH 情報を結合し、近接率降順のランキングを作る。"""
    ranked = []
    skipped = []
    for r in rows:
        code = r["code"]
        info = ath_map.get(code)
        ath = info["ath"] if info else None
        last = (info["last"] if info else None) or r.get("last_price")
        # ATH が無い/0（新規上場直後など）や最新値欠損はランキング対象外
        if not ath or not last:
            skipped.append(code)
            continue
        pct = last / ath * 100.0
        if min_pct is not None and pct < min_pct:
            continue
        ranked.append(
            {
                "code": code,
                "name": (info["name"] if info else "") or r.get("name", ""),
                "last": last,
                "ath": ath,
                "pct": pct,
                "turnover": r.get("turnover"),
            }
        )
    ranked.sort(key=lambda x: x["pct"], reverse=True)
    for i, row in enumerate(ranked, start=1):
        row["rank"] = i
    return ranked, skipped


def print_table(rows, market_name, universe_n):
    print(
        f"\n=== {market_name} 売買代金上位{universe_n}銘柄 / 上場来高値(ATH)接近ランキング ===\n"
    )
    header = (
        f"{'順位':>3}  {'コード':<12} {'銘柄名':<24} "
        f"{'最新値':>10} {'上場来高値':>12} {'ATH比':>7}  {'売買代金':>12}"
    )
    print(header)
    print("-" * 88)
    for r in rows:
        name = (r["name"] or "")[:22]
        print(
            f"{r['rank']:>3}  {r['code']:<12} {name:<24} "
            f"{r['last']:>10,.2f} {r['ath']:>12,.2f} {r['pct']:>6.2f}%  "
            f"{tt.human_money(r['turnover']):>12}"
        )
    print()


def save_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "code", "name", "last_price", "ath", "ath_pct", "turnover"])
        for r in rows:
            writer.writerow(
                [r["rank"], r["code"], r["name"], r["last"], r["ath"], f"{r['pct']:.4f}", r["turnover"]]
            )
    print(f"CSV を保存しました: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="売買代金上位N銘柄のATH接近ランキング")
    parser.add_argument("--market", default="US", help="市場 (US / HK / JP など。デフォルト: US)")
    parser.add_argument("--top", type=int, default=100, help="売買代金上位の取得件数（デフォルト: 100）")
    parser.add_argument("--min-pct", type=float, default=None, help="ATH比の下限(%%)。指定するとこれ以上のみ表示")
    parser.add_argument("--host", default="127.0.0.1", help="OpenD ホスト")
    parser.add_argument("--port", type=int, default=11111, help="OpenD ポート")
    parser.add_argument("--csv", metavar="PATH", help="CSV 出力先パス（任意）")
    args = parser.parse_args()

    market = tt.resolve_market(args.market)

    quote_ctx = ft.OpenQuoteContext(host=args.host, port=args.port)
    try:
        # 1) 売買代金上位N
        rows = tt.fetch_top(quote_ctx, market, args.top)
        if not rows:
            print("売買代金上位の取得結果が空でした。")
            return 1
        # 2) ATH（上場来高値）取得
        codes = [r["code"] for r in rows]
        ath_map = fetch_ath(quote_ctx, codes)
    finally:
        quote_ctx.close()

    # 3) 結合・ランキング
    ranked, skipped = build_ranking(rows, ath_map, args.min_pct)
    if not ranked:
        print("ランキング対象の銘柄がありませんでした（ATH欠損 or フィルタで全除外）。")
        return 1

    print_table(ranked, args.market.upper(), len(rows))
    if skipped:
        print(f"※ ATH情報を取得できずスキップ: {len(skipped)}件 ({', '.join(skipped[:10])}{' ...' if len(skipped) > 10 else ''})")
    if args.csv:
        save_csv(ranked, args.csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
