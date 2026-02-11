# copy_2026_folder.py - Googleドライブフォルダコピースクリプト

## 概要

このスクリプトは、Googleドライブ上のフォルダを2026年衆院選用に選択的にコピーするPythonツールです。コピー元フォルダの直下の階層から開始し、特定の形式のCSVファイルのみを更新日時を比較してコピーします。

## 主な機能

### 基本機能

- **階層的なフォルダコピー**: コピー元フォルダの直下（都道府県レベル）からコピーを開始
- **選択的ファイルコピー**: `*_normalized_final.csv` または `*_normalized_final_upd.csv` のみをコピー対象
- **ファイル名サフィックス**: `--suffix` オプションでコピー先ファイル名にサフィックスを付加可能
- **更新日時比較**: コピー元ファイルが新しい場合のみ上書きコピー
- **階層制限**: `{prefecture}/{city}` または `立候補者なし/{prefecture}/{city}` の階層のみを処理（それ以下は対象外）
- **進捗表示**: `[都道府県番号/都道府県総数][市区町村番号/市区町村総数]` 形式で処理の進捗状況を表示
- **統計情報**: バッチ終了時に各CSVファイルの総数、コピー済み数、スキップ数を表示
- **詳細ログ出力**: 処理内容を詳細にログファイルとコンソールに出力
- **ドライランモード**: 実際のコピーを行わず、処理内容のみを表示

### 特別な処理ルール

1. **コピー元フォルダ自体はコピーしない**
   - `BASE_FOLDER_ID` で指定されたフォルダの直下の層（都道府県フォルダ）からコピーを開始
   - コピー元フォルダ自体は、コピー先に作成されません

2. **「2025参院選後」または「2025参議院選挙後」フォルダの除外**
   - このフォルダ自体はコピーしない
   - フォルダ内の `*_normalized_final_upd.csv` ファイルのみを親フォルダ（市区町村フォルダ）にコピー
   - 優先順位: 「2025参院選後」 > 「2025参議院選挙後」（両方存在する場合は両方処理）

3. **CSVファイルのコピー対象**
   - `*_normalized_final.csv`: 同じフォルダにコピー
   - `*_normalized_final_upd.csv`: 同じフォルダにコピー（「2025参院選後」内の場合は親フォルダ）
   - その他のCSVファイル（例: `*_normalized.csv`, `*_normalized_append.csv`）: コピー対象外

4. **ファイル名サフィックス**
   - `--suffix` オプションを指定すると、コピー先ファイル名にサフィックスが付加されます
   - 例: `--suffix=_末尾` を指定すると、`大磯町_normalized_final.csv` → `大磯町_normalized_final_末尾.csv`
   - サフィックスは `.csv` の直前に挿入されます

5. **更新日時による上書き判定**
   - 既存ファイルがない場合: 新規コピー
   - コピー元の更新日時 > コピー先の更新日時: 上書きコピー
   - コピー元の更新日時 <= コピー先の更新日時: スキップ（更新なし）

## 前提条件

### 必要なファイル

- `my_secrets.json`: Google API認証情報
- `my_settings.json`: コピー元フォルダIDなどの設定
- `token.json`: OAuth2トークン（自動生成）

### my_settings.json の設定

```json
{
  "SKIP_LATLONG_UPDATE_LIST": [...],
  "BASE_FOLDER_ID": "コピー元フォルダID（必須）",
  "DEST_FOLDER_ID": "デフォルトのコピー先フォルダID（オプション）"
}
```

- **BASE_FOLDER_ID**: コピー元となるフォルダのGoogleドライブID（必須）
- **DEST_FOLDER_ID**: デフォルトのコピー先フォルダID（オプション、引数省略時に使用）

### my_secrets.json の設定

```json
{
  "OAUTH2_CLIENT_INFO": {
    "installed": {
      "client_id": "your-client-id",
      "project_id": "your-project-id",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_secret": "your-client-secret",
      "redirect_uris": ["http://localhost"]
    }
  }
}
```

### 必要なPythonライブラリ

```bash
pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
```

## 使用方法

### 基本的なコピー（コピー先フォルダIDを指定）

```bash
python copy_2026_folder.py <コピー先フォルダID>
```

- `BASE_FOLDER_ID` で指定されたフォルダの直下の層を `<コピー先フォルダID>` にコピー

### コピー先をmy_settings.jsonから読み込み

```bash
python copy_2026_folder.py
```

- `my_settings.json` の `DEST_FOLDER_ID` をコピー先として使用
- `DEST_FOLDER_ID` が設定されていない場合はエラー

### ファイル名サフィックスを指定してコピー

```bash
python copy_2026_folder.py <コピー先フォルダID> --suffix=_末尾
```

- コピー先ファイル名にサフィックスを付加
- 例: `大磯町_normalized_final.csv` → `大磯町_normalized_final_末尾.csv`

### ドライランモード（実際のコピーは行わない）

```bash
python copy_2026_folder.py <コピー先フォルダID> --dry-run
```

または

```bash
python copy_2026_folder.py --dry-run
```

- 実際のコピー処理は行わず、処理内容のみをログに出力
- コピー対象の確認や、処理の事前検証に使用

## 使用例

### 例1: 特定のフォルダにコピー

```bash
python copy_2026_folder.py 1AbCdEfGhIjKlMnOpQrStUvWxYz123456
```

### 例2: デフォルトのコピー先にコピー

```bash
# my_settings.jsonにDEST_FOLDER_IDが設定されている場合
python copy_2026_folder.py
```

### 例3: サフィックス付きでコピー

```bash
python copy_2026_folder.py 1AbCdEfGhIjKlMnOpQrStUvWxYz123456 --suffix=_2026衆院選
```

### 例4: ドライランで処理内容を確認

```bash
python copy_2026_folder.py 1AbCdEfGhIjKlMnOpQrStUvWxYz123456 --dry-run
```

### 例5: サフィックス付きでドライラン

```bash
python copy_2026_folder.py 1AbCdEfGhIjKlMnOpQrStUvWxYz123456 --suffix=_末尾 --dry-run
```

## 処理フロー

1. **認証**: Google API OAuth2認証
2. **設定読み込み**: `my_settings.json` からコピー元フォルダIDを取得
3. **妥当性チェック**: コピー元・コピー先フォルダIDの検証
4. **階層的コピー**:
   - **第1階層（都道府県または立候補者なし）**: コピー元フォルダ直下のフォルダを走査
     - 通常の都道府県フォルダ: 第2階層（市区町村）を処理
     - 「立候補者なし」フォルダ: 第2階層（都道府県）→第3階層（市区町村）を処理
   - **市区町村フォルダ内**:
     - `*_normalized_final.csv` または `*_normalized_final_upd.csv`: 更新日時を比較してコピー
     - 「2025参院選後」または「2025参議院選挙後」フォルダ: 内部の `*_normalized_final_upd.csv` のみを親フォルダにコピー
     - その他のフォルダ/ファイル: スキップ
5. **統計情報表示**: コピー対象ファイルの総数、コピー済み数、スキップ数を表示

## ログ出力

### ログファイル

- `copy_2026_folder.log`: 処理の詳細ログ（UTF-8エンコーディング）

### ログレベル

- **INFO**: 通常の処理進捗
- **WARNING**: APIエラーからのリトライ、更新日時取得失敗時の警告
- **ERROR**: エラー発生時

### ログ出力例

```
2026-01-18 10:00:00 INFO === フォルダコピー開始 ===
2026-01-18 10:00:01 INFO 認証情報を取得中...
2026-01-18 10:00:02 INFO コピー元フォルダID: 1yOsYgLdHmd0v6BvA8Guainsx_jC8enx8
2026-01-18 10:00:03 INFO コピー先フォルダID: 1jdKsN3zdgITQshcSQCW6SAHq-PLk8m9J (my_settings.jsonから読み込み)
2026-01-18 10:00:04 INFO コピー元フォルダ名: 自治体
2026-01-18 10:00:05 INFO === フォルダ構造のコピー開始 ===
2026-01-18 10:00:06 INFO 第1階層フォルダ数: 48
2026-01-18 10:00:07 INFO [1/48] 都道府県フォルダ: 愛媛県
2026-01-18 10:00:08 INFO   既存のフォルダを使用: 愛媛県 (ID: 1XyZ...)
2026-01-18 10:00:09 INFO   [1/48][1/20] 市区町村フォルダ: 愛南町
2026-01-18 10:00:10 INFO     既存のフォルダを使用: 愛南町 (ID: 1AbC...)
2026-01-18 10:00:11 INFO     新規コピー: 愛南町_normalized_final.csv
2026-01-18 10:00:12 INFO     スキップ（対象外のFile）: 愛南町_normalized.csv
2026-01-18 10:00:13 INFO   [1/48][2/20] 市区町村フォルダ: 鬼北町
2026-01-18 10:00:14 INFO     既存のフォルダを使用: 鬼北町 (ID: 1Def...)
2026-01-18 10:00:15 INFO     上書きコピー: 鬼北町_normalized_final.csv (更新あり (コピー元: 2026-01-18T10:30:45.123Z, コピー先: 2026-01-17T09:20:30.456Z))
...
2026-01-18 10:15:00 INFO [48/48] 立候補者なしフォルダ: 立候補者なし
2026-01-18 10:15:01 INFO   既存のフォルダを使用: 立候補者なし (ID: 1Jkl...)
2026-01-18 10:15:02 INFO   立候補者なし配下の都道府県数: 3
2026-01-18 10:15:03 INFO   [48/48][1/3] 都道府県フォルダ: 東京都
2026-01-18 10:15:04 INFO     既存のフォルダを使用: 東京都 (ID: 1Mno...)
2026-01-18 10:15:05 INFO     [48/48][1/3][1/1] 市区町村フォルダ: 小笠原村
2026-01-18 10:15:06 INFO       既存のフォルダを使用: 小笠原村 (ID: 1Pqr...)
2026-01-18 10:15:07 INFO       新規コピー: 小笠原村_normalized_final.csv
...
2026-01-18 10:30:00 INFO === フォルダ構造のコピー完了 ===
2026-01-18 10:30:01 INFO === 統計情報 ===
2026-01-18 10:30:02 INFO *_normalized_final.csv:
2026-01-18 10:30:03 INFO   総数: 1500
2026-01-18 10:30:04 INFO   コピー済み: 250
2026-01-18 10:30:05 INFO   スキップ: 1250
2026-01-18 10:30:06 INFO *_normalized_final_upd.csv:
2026-01-18 10:30:07 INFO   総数: 450
2026-01-18 10:30:08 INFO   コピー済み: 120
2026-01-18 10:30:09 INFO   スキップ: 330
2026-01-18 10:30:10 INFO === フォルダコピー完了 ===
2026-01-18 10:30:11 INFO コピー先フォルダID: 1jdKsN3zdgITQshcSQCW6SAHq-PLk8m9J
```

## エラーハンドリング

### APIエラー時のリトライ

- **リトライ回数**: 最大20回
- **リトライ戦略**: 指数バックオフ + ジッター（ランダム要素）
- **待機時間**: `base_delay * (2 ^ attempt) + random(0, 1)` 秒

### よくあるエラーと対処法

#### エラー1: `my_settings.jsonにBASE_FOLDER_IDが設定されていません`

**原因**: `my_settings.json` に `BASE_FOLDER_ID` フィールドがない

**対処法**: `my_settings.json` に以下を追加
```json
{
  "BASE_FOLDER_ID": "コピー元フォルダID"
}
```

#### エラー2: `コピー先フォルダIDが指定されていません`

**原因**: コマンドライン引数でコピー先フォルダIDを指定せず、`my_settings.json` にも `DEST_FOLDER_ID` がない

**対処法**: 以下のいずれか
- コマンドライン引数でコピー先フォルダIDを指定
- `my_settings.json` に `DEST_FOLDER_ID` を追加

#### エラー3: `指定されたID XXX はフォルダではありません`

**原因**: 指定されたIDがファイルまたは無効なIDである

**対処法**: 正しいフォルダIDを指定（GoogleドライブのURLから取得）

## フォルダ構造の例

### コピー元の構造

```
BASE_FOLDER_ID (例: 自治体) ← このフォルダ自体はコピーされない
├── 愛媛県/  ← ここから下をコピー
│   ├── 愛南町/
│   │   ├── 愛南町_normalized.csv  ← スキップ（対象外）
│   │   ├── 愛南町_normalized_append1.csv  ← スキップ（対象外）
│   │   ├── 愛南町_normalized_final.csv  ← コピー（更新日時を比較）
│   │   ├── 2025参院選後/  ← このフォルダは除外
│   │   │   └── 愛南町_normalized_final_upd.csv  ← コピー（親フォルダへ）
│   │   └── 2025参議院選挙後/  ← このフォルダも除外
│   │       └── 愛南町_normalized_final_upd.csv  ← コピー（親フォルダへ）
│   └── 松山市/
│       └── 松山市_normalized_final.csv  ← コピー（更新日時を比較）
├── 東京都/
│   └── 渋谷区/
│       └── 渋谷区_normalized_final.csv  ← コピー（更新日時を比較）
└── 立候補者なし/  ← ここから下もコピー
    ├── 東京都/
    │   └── 小笠原村/
    │       └── 小笠原村_normalized_final.csv  ← コピー（更新日時を比較）
    └── 鹿児島県/
        └── 十島村/
            └── 十島村_normalized_final.csv  ← コピー（更新日時を比較）
```

### コピー後の構造

```
DEST_FOLDER_ID
├── 愛媛県/  ← コピー元の直下から開始
│   ├── 愛南町/
│   │   ├── 愛南町_normalized_final.csv  ← 同じフォルダにコピーされた（更新日時が新しい場合のみ）
│   │   └── 愛南町_normalized_final_upd.csv  ← 親フォルダ（愛南町）にコピーされた
│   └── 松山市/
│       └── 松山市_normalized_final.csv
├── 東京都/
│   └── 渋谷区/
│       └── 渋谷区_normalized_final.csv
└── 立候補者なし/
    ├── 東京都/
    │   └── 小笠原村/
    │       └── 小笠原村_normalized_final.csv
    └── 鹿児島県/
        └── 十島村/
            └── 十島村_normalized_final.csv
```

## 注意事項

1. **認証**: 初回実行時はブラウザで認証が必要
2. **API制限**: Google Drive APIには1日あたりの利用制限があるため、大量のファイルをコピーする場合は注意
3. **更新日時による上書き**: コピー元ファイルが新しい場合のみ上書きされます。同じまたは古い場合はスキップされます
4. **コピー対象のCSV**: `*_normalized_final.csv` または `*_normalized_final_upd.csv` のみがコピー対象
5. **その他のファイル**: 上記以外のCSVファイル（`*_normalized.csv`、`*_normalized_append.csv` など）、画像、PDF、Googleドキュメントなどはすべてスキップされます
6. **階層制限**: `{prefecture}/{city}` または `立候補者なし/{prefecture}/{city}` の階層のみが処理対象。それより深い階層のフォルダ（「2025参院選後」を除く）は無視されます
7. **ショートカット**: フォルダやファイルのショートカットもスキップされます

## トラブルシューティング

### 認証エラーが発生する場合

1. `token.json` を削除して再認証を試す
2. `my_secrets.json` の認証情報が正しいか確認

### コピーが途中で停止する場合

- ログファイル (`copy_2026_folder.log`) を確認してエラー箇所を特定
- API制限に達している可能性がある場合は、時間をおいて再実行

### フォルダIDの取得方法

GoogleドライブでフォルダのURLを確認:
```
https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOpQrStUvWxYz123456
                                        ↑
                                    このIDを使用
```

## 関連ファイル

- [backup_folder.py](backup_folder.py): フォルダバックアップスクリプト（参考実装）
- [check_normalized_csv.py](check_normalized_csv.py): CSV検証スクリプト
- [my_settings.json](my_settings.json): 設定ファイル
- [my_secrets.json](my_secrets.json): 認証情報ファイル（要作成）

## ライセンス

このスクリプトは、TeamMiraiプロジェクトの一部です。

## 更新履歴

- **2026-01-20 (v4.0)**: 機能追加
  - `--suffix` オプションを追加（コピー先ファイル名にサフィックスを付加）
  - ファイル名・フォルダ名の末尾空白を自動トリミング
  - 「2025参議院選挙後」フォルダも「2025参院選後」と同様に処理対象に追加

- **2026-01-18 (v3.1)**: フォルダ構造仕様の修正
  - フォルダ構造を `{prefecture}/{city}` および `立候補者なし/{prefecture}/{city}` に対応

- **2026-01-18 (v3.0)**: 機能追加
  - 更新日時比較機能を追加（コピー元が新しい場合のみ上書き）
  - 統計情報表示機能を追加（総数、コピー済み数、スキップ数）
  - prefecture/city別の進捗表示を追加
  - ファイル数の事前カウントを廃止（処理時間短縮）
  - ログ出力を改善（新規コピー、上書きコピー、スキップを明確に区別）

- **2026-01-18 (v2.0)**: 仕様変更
  - コピー元フォルダ直下の層からコピーを開始（フォルダ自体はコピーしない）
  - `*_normalized_final.csv` は同じフォルダにコピー（親フォルダではなく）
  - `*_normalized_final.csv` または `*_normalized_final_upd.csv` のみをコピー対象とする
  - `{prefecture}/{city}` または `{prefecture}/立候補者なし/{city}` より下層のフォルダは除外

- **2026-01-18 (v1.0)**: 初版作成
  - 基本的なフォルダコピー機能
  - 「2025参院選後」フォルダの除外処理
  - ドライランモード
  - 詳細ログ出力
