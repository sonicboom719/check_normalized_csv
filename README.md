# check_normalized_csv

日本の自治体の投票所データを管理するための正規化CSVファイル検証・処理ツール

## 概要

このツールは、Google Driveに保存されている自治体の正規化CSVファイルに対して以下の処理を行います：

- CSVファイルの妥当性検証（エンコーディング、ヘッダー、データ形式）
- 緯度経度情報の自動修正（Google Maps API / 国土地理院API使用）
- 最終正規化CSVの作成（複数ファイルの結合、重複除去、ソート）
- ファイル名の自動修正（スペルミスなど）

## 必要な環境

- Python 3.7以上
- Google Cloud Platform アカウント（Google Drive API / Sheets API / Maps API使用）

## セットアップ

### 1. 依存ライブラリのインストール

```bash
pip install gspread google-api-python-client google-auth-oauthlib chardet requests
```

### 2. Google API認証情報の設定

`my_secrets.json`ファイルを作成し、以下の形式で認証情報を記述：

```json
{
  "OAUTH2_CLIENT_INFO": {
    "installed": {
      "client_id": "YOUR_CLIENT_ID",
      "client_secret": "YOUR_CLIENT_SECRET",
      "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token"
    }
  },
  "GOOGLE_API_KEY": "YOUR_GOOGLE_MAPS_API_KEY"
}
```

### 3. 設定ファイルの作成

`my_settings.json`ファイルを作成し、緯度経度更新をスキップする自治体を指定：

```json
{
  "SKIP_LATLONG_UPDATE_LIST": [
    ["福岡県", "福岡市"],
    ["東京都", "世田谷区"]
  ]
}
```

## 使用方法

### 基本的な使い方

```bash
# 全自治体をチェック（読み取り専用）
python check_normalized_csv.py

# 特定の都道府県をチェック
python check_normalized_csv.py 東京都

# 特定の市区町村をチェック
python check_normalized_csv.py 東京都 渋谷区
```

### オプション

- `-u, --update`: 修正内容でCSVファイルを上書き更新
- `-d, --delete`: ファイル名に「削除希望」を含むファイルを削除
- `-f, --final`: 最終正規化CSV作成モード
- `-lu, --last-updated`: 指定日時以降に更新されたファイルのみ処理

### 使用例

```bash
# 緯度経度エラーを自動修正してアップロード
python check_normalized_csv.py -u

# 削除希望ファイルを削除
python check_normalized_csv.py -d

# 最終正規化CSVを作成
python check_normalized_csv.py 福岡県 福岡市 -f

# 2025年7月1日以降に更新されたファイルのみチェック
python check_normalized_csv.py -lu 20250701
```

## CSVファイル形式

### 期待されるヘッダー

基本形式：
```
prefecture,city,number,address,name,lat,long
```

オプション（note列付き）：
```
prefecture,city,number,address,name,lat,long,note
```

### データ例

```csv
prefecture,city,number,address,name,lat,long,note
東京都,渋谷区,1-1,渋谷1-1-1,渋谷区役所,35.658581,139.698553,
東京都,渋谷区,1-2,神南1-23-1,渋谷区スポーツセンター,35.661820,139.700473,
```

## 機能詳細

### 1. CSV検証機能

- エンコーディング検出（UTF-8、Shift-JIS対応）
- BOM有無の検出と自動変換
- ヘッダー検証
- 緯度経度の妥当性チェック
- 重複行検出（number, name, address）

### 2. 緯度経度修正機能（-uオプション）

- 無効な緯度経度を自動検出
- Google Maps APIと国土地理院APIで座標を取得
- 両APIの結果が200m以上離れている場合は「緯度経度は怪しい」をnote列に記録

### 3. 最終正規化CSV作成機能（-fオプション）

- 基本ファイル（`{市区町村名}_normalized.csv`）と追加ファイル（`*append.csv`）を結合
- 緯度経度が空の行を除外
- 重複行を検出・除去（number, name, lat, longの組み合わせ）
- 政令指定都市の行政区でグルーピング後、number列で自然順ソート

### 4. ファイル名自動修正

以下のパターンを自動検出・修正：
- `nomalized` → `normalized`（lの欠落）
- `normarized` → `normalized`（l→r誤記）
- 先頭の余計な文字列を除去

## ログ出力

実行結果は以下に出力されます：
- コンソール（標準出力）
- `check_normalized_csv.log`ファイル

## エラーレベル

- **ERROR**: 重大な問題（データ不整合など）
- **WARNING**: 注意が必要な問題（緯度経度エラーなど）
- **INFO**: 処理状況の通知

特定の自治体（`SKIP_LATLONG_UPDATE_LIST`に登録）では、緯度経度エラーをINFOレベルで出力します。

## テスト

```bash
# 全テストを実行
python test_check_normalized_csv.py

# 特定のテストクラスを実行
python -m unittest test_check_normalized_csv.TestCheckCsvContent
```

## ライセンス

MIT License

## 作者

TeamMirai

## 貢献

バグ報告や機能提案は、GitHubのIssueでお願いします。