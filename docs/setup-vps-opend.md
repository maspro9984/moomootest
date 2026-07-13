# さくらVPS（Ubuntu）で OpenD CLI + スクリプトを常駐させる手順書

このリポジトリを **さくらVPS（Ubuntu/Debian, x86_64）** 上で動かすための runbook。
VPS 上で起動した Claude Code セッションが、この手順に沿って作業する想定。

## ゴールと構成

```
[さくらVPS / Ubuntu]
  ├─ OpenD (CLI版, systemd常駐) ── 127.0.0.1:11111 で listen（外部公開しない）
  ├─ このリポジトリ（git clone 済み）
  │    └─ top_turnover.py 等を venv で実行し 127.0.0.1:11111 に接続
  └─ Claude Code（SSHセッション内で起動）→ commit & push → GitHub(origin/main)
```

- OpenD とスクリプトは **同一ホストに同居**させ、接続は `127.0.0.1:11111` に限定する。
- OpenD の 11111 番ポートは **インターネットに晒さない**（後述のセキュリティ参照）。

## 事前に手元に用意しておくもの（このセッションには渡さない）

> ⚠️ **認証情報（moomooのアカウント/パスワード/認証コード）は Claude に貼らないこと。**
> OpenD の設定・ログイン時に、あなた自身が直接入力する。

- moomoo アカウント（ログインID＝メール or 電話番号）とパスワード
- そのアカウントに **米国株のリアルタイム/相場権限** があること（ローカルで US.MU 等が取れているので確認済みの想定）
- **初回ログインのデバイス認証コード**を受け取れる状態（SMS / moomooアプリ）
- sudo 権限のあるユーザー

---

## Step 1. システム準備

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip git tmux ufw curl
```

作業用の一般ユーザーで進める（root 直運用は避ける）。

## Step 2. リポジトリ取得と Python 環境

```bash
cd ~
git clone https://github.com/maspro9984/moomootest.git
cd moomootest
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install moomoo-api
```

`.venv/` は `.gitignore` 済みか確認（無ければ追加）。

## Step 3. OpenD CLI版の入手・展開

> 📌 **公式ダウンロードページで最新の「OpenD コマンドライン版 / Linux(Ubuntu)」の正確なファイル名・URLを確認してから取得する。**
> バージョンや配布形式が変わるため、URLをここに固定しない。moomoo OpenAPI のダウンロードページ（要ログインの場合あり）を参照。
> 迷ったら WebFetch でダウンロードページを開いて Ubuntu 用 x64 ビルドのリンクを特定する。

- 取得したアーカイブ（例: `OpenD_x.x.xxxx_Ubuntu16.04_x64.tar.gz`）を `~/opend/` に展開。
- 中身の目安: `OpenD` バイナリ、設定ファイル（`OpenD.xml` 等）、依存ライブラリ。
- 実行ビットを確認（`chmod +x OpenD`）。glibc 不足でエラーが出たら、その依存を `apt` で補う。

## Step 4. 設定と初回ログイン（対話・デバイス認証を突破）

初回はデバイス認証コードの入力が必要になるため、**tmux 上でフォアグラウンド実行**して対話に応じる。

```bash
tmux new -s opend
cd ~/opend
./OpenD    # 設定ファイル or 引数でアカウント/パスワードを指定して起動
```

- 起動オプション/設定キーの正確な名称はそのバージョンの README/config に従う
  （`api_port=11111`、ログインアカウント、パスワード（md5指定の版もある）等）。
- **初回ログインで認証コードを求められたら入力**し、「login success」相当のログが出るまで確認。
- ログイン状態・端末情報はファイルに保存される → **消すと再度デバイス認証**になるので、
  そのディレクトリを次の systemd 運用でもそのまま使う（消さない・volumeを分けない）。
- 確認できたら tmux を `Ctrl-b d` でデタッチ（プロセスは残る）。この時点で疎通テスト（Step 6）してもよい。

> 🔒 **認証情報を含む設定ファイル（OpenD.xml 等）は絶対に git にコミットしない。**
> リポジトリ内に置く場合は `.gitignore` に追加。今回は `~/opend/` 配下（リポジトリ外）に置くのが安全。

## Step 5. systemd で常駐化

初回ログインが通ったら、恒久運用として systemd サービス化する。

```ini
# /etc/systemd/system/opend.service   （パス/ユーザー名は実際に合わせる）
[Unit]
Description=moomoo OpenD gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<your-user>
WorkingDirectory=/home/<your-user>/opend
ExecStart=/home/<your-user>/opend/OpenD   # 起動引数/設定は Step4 と同じ
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now opend
sudo systemctl status opend --no-pager
journalctl -u opend -f    # ログ確認
```

> ⚠️ 再起動やサーバー再起動後に**再度デバイス認証を要求される**場合がある。
> その時は一度 tmux でフォアグラウンド起動して認証を通し、状態を確立してから systemd に戻す。

## Step 6. 疎通確認

```bash
cd ~/moomootest
source .venv/bin/activate
python top_turnover.py --top 5
```

US の売買代金上位（MU / NVDA / META …）が表示されれば成功。
表示が文字化けする場合はロケール（`LANG=ja_JP.UTF-8` 等）を設定する（Windows端末側の問題とは別）。

---

## セキュリティ

- **11111 番ポートを外部公開しない。** OpenD は `127.0.0.1` に bind し、スクリプトも同ホストから接続する。
- ufw 例（SSH のみ許可、それ以外は拒否）:
  ```bash
  sudo ufw allow OpenSSH
  sudo ufw enable
  sudo ufw status
  ```
  （11111 は開けない）
- どうしても手元PCから OpenD に繋ぎたいときは、ポートを開けず **SSHトンネル**経由:
  ```bash
  ssh -L 11111:localhost:11111 <your-user>@<vps-host>
  ```
- 認証情報を含むファイルは git 管理外／`.gitignore`。誤ってコミットしていないか `git status` で毎回確認。

## 開発ワークフロー

- 編集・実行は VPS 上（Claude Code を SSH セッションで起動）。
- 変更は commit → `git push origin main` で GitHub にバックアップ／履歴化。
- 二者択一の「ローカル編集→デプロイ」ではなく、**実行環境（＝OpenDのある場所）で開発し、git はバージョン管理として併用**する方針。
