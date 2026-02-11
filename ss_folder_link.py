#!/usr/bin/env python3
"""
GoogleスプレッドシートとGoogle Driveフォルダを連携するスクリプト

使用方法:
    python ss_folder_link.py <基準フォルダID> <スプシID> <シートID> <列>

機能:
    - スプレッドシートのA列（prefecture）とB列（city）を読み込み
    - 基準フォルダ内で該当するcityフォルダを探索
    - 見つかったフォルダIDを指定列に書き込み
    - フォルダ構造: {prefecture}/{city} または 立候補者なし/{prefecture}/{city} または {prefecture}/立候補者なし/{city}
"""

import argparse
import json
import logging
import os
import sys
import time
import random
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('ss_folder_link.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def retry_on_api_error(func, max_retries: int = 20, base_delay: float = 1.0):
    """APIエラー時のリトライ機能"""
    for attempt in range(max_retries + 1):
        try:
            return func()
        except HttpError as error:
            if attempt == max_retries:
                logger.error(f"最大リトライ回数({max_retries})に達しました: {error}")
                raise

            # 指数バックオフ + ジッター（ランダム要素）
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"APIエラー (試行 {attempt + 1}/{max_retries + 1}): {error}")
            logger.info(f"{delay:.1f}秒後にリトライします...")
            time.sleep(delay)
        except Exception as error:
            # HttpError以外のエラーは即座に再発生
            logger.error(f"予期しないエラー: {error}")
            raise

def load_secrets():
    """my_secrets.jsonから認証情報を読み込む"""
    try:
        with open('my_secrets.json', encoding='utf-8') as f:
            secrets = json.load(f)
        return secrets['OAUTH2_CLIENT_INFO']
    except FileNotFoundError:
        logger.error("my_secrets.jsonファイルが見つかりません")
        sys.exit(1)
    except KeyError:
        logger.error("my_secrets.jsonにOAUTH2_CLIENT_INFOが含まれていません")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"my_secrets.jsonのJSON形式が不正です: {e}")
        sys.exit(1)

def get_credentials():
    """Google API認証情報を取得"""
    OAUTH2_CLIENT_INFO = load_secrets()
    SCOPES = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    TOKEN_PATH = 'token.json'

    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_config(OAUTH2_CLIENT_INFO, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w', encoding='utf-8') as token:
            token.write(creds.to_json())

    return creds

def list_drive_files(service, folder_id: str) -> List[Dict]:
    """フォルダ内の全ファイル・フォルダを取得"""
    files = []
    page_token = None

    while True:
        def api_call():
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType)',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token
            ).execute()
            return response

        try:
            response = retry_on_api_error(api_call)
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)

            if not page_token:
                break

        except Exception as error:
            logger.error(f"ファイル一覧取得エラー: {error}")
            break

    return files

def column_letter_to_index(column: str) -> int:
    """列アルファベットを0ベースのインデックスに変換（例: A->0, B->1, Z->25, AA->26）"""
    result = 0
    for char in column.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1

def index_to_column_letter(index: int) -> str:
    """0ベースのインデックスを列アルファベットに変換（例: 0->A, 1->B, 25->Z, 26->AA）"""
    result = ""
    index += 1  # 1ベースに変換
    while index > 0:
        index -= 1
        result = chr(ord('A') + index % 26) + result
        index //= 26
    return result

def read_spreadsheet(sheets_service, spreadsheet_id: str, sheet_id: int) -> List[Dict]:
    """
    スプレッドシートからデータを読み込み

    Returns:
        List[Dict]: [{'row': 2, 'prefecture': '東京都', 'city': '渋谷区'}, ...]
    """
    def api_call():
        # シートIDからシート名を取得
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()

        sheet_name = None
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['sheetId'] == sheet_id:
                sheet_name = sheet['properties']['title']
                break

        if not sheet_name:
            raise ValueError(f"シートID {sheet_id} が見つかりません")

        # データを読み込み（A列とB列、2行目以降）
        range_name = f"'{sheet_name}'!A2:B"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        return result, sheet_name

    try:
        result, sheet_name = retry_on_api_error(api_call)
        values = result.get('values', [])

        data = []
        for idx, row in enumerate(values, start=2):  # 2行目から開始
            if len(row) >= 2:
                prefecture = row[0].strip() if row[0] else ""
                city = row[1].strip() if row[1] else ""
                if prefecture and city:  # 両方とも空でない場合のみ追加
                    data.append({
                        'row': idx,
                        'prefecture': prefecture,
                        'city': city
                    })

        logger.info(f"スプレッドシート読み込み完了: {len(data)}件のデータ")
        return data, sheet_name

    except Exception as error:
        logger.error(f"スプレッドシート読み込みエラー: {error}")
        raise

def build_folder_map(drive_service, base_folder_id: str) -> Dict[Tuple[str, str], str]:
    """
    基準フォルダ内のフォルダ構造を探索してマップを作成

    Returns:
        Dict[Tuple[prefecture, city], folder_id]
    """
    folder_map = {}

    # 第1階層を取得
    first_level_items = list_drive_files(drive_service, base_folder_id)
    first_level_folders = [item for item in first_level_items if item['mimeType'] == 'application/vnd.google-apps.folder']

    total_first_level = len(first_level_folders)
    logger.info(f"第1階層フォルダ数: {total_first_level}")

    for first_idx, first_item in enumerate(first_level_folders, 1):
        first_name = first_item['name'].strip()
        first_id = first_item['id']

        if first_name == "立候補者なし":
            # 立候補者なし/{prefecture}/{city}
            logger.info(f"[{first_idx}/{total_first_level}] 立候補者なしフォルダ: {first_name}")

            prefecture_items = list_drive_files(drive_service, first_id)
            prefecture_folders = [item for item in prefecture_items if item['mimeType'] == 'application/vnd.google-apps.folder']

            for prefecture_item in prefecture_folders:
                prefecture_name = prefecture_item['name'].strip()
                prefecture_id = prefecture_item['id']

                city_items = list_drive_files(drive_service, prefecture_id)
                city_folders = [item for item in city_items if item['mimeType'] == 'application/vnd.google-apps.folder']

                for city_item in city_folders:
                    city_name = city_item['name'].strip()
                    city_id = city_item['id']
                    key = (prefecture_name, city_name)
                    folder_map[key] = city_id
                    logger.info(f"  登録: {prefecture_name}/{city_name} -> {city_id}")

        else:
            # {prefecture}/{city} または {prefecture}/立候補者なし/{city}
            prefecture_name = first_name
            prefecture_id = first_id

            logger.info(f"[{first_idx}/{total_first_level}] 都道府県フォルダ: {prefecture_name}")

            second_level_items = list_drive_files(drive_service, prefecture_id)
            second_level_folders = [item for item in second_level_items if item['mimeType'] == 'application/vnd.google-apps.folder']

            for second_item in second_level_folders:
                second_name = second_item['name'].strip()
                second_id = second_item['id']

                if second_name == "立候補者なし":
                    # {prefecture}/立候補者なし/{city}
                    city_items = list_drive_files(drive_service, second_id)
                    city_folders = [item for item in city_items if item['mimeType'] == 'application/vnd.google-apps.folder']

                    for city_item in city_folders:
                        city_name = city_item['name'].strip()
                        city_id = city_item['id']
                        key = (prefecture_name, city_name)
                        folder_map[key] = city_id
                        logger.info(f"  登録: {prefecture_name}/立候補者なし/{city_name} -> {city_id}")
                else:
                    # {prefecture}/{city}
                    city_name = second_name
                    city_id = second_id
                    key = (prefecture_name, city_name)
                    folder_map[key] = city_id
                    logger.info(f"  登録: {prefecture_name}/{city_name} -> {city_id}")

    logger.info(f"フォルダマップ作成完了: {len(folder_map)}件")
    return folder_map

def write_to_spreadsheet(sheets_service, spreadsheet_id: str, sheet_name: str, column: str, updates: List[Tuple[int, str]]):
    """
    スプレッドシートに書き込み

    Args:
        updates: [(row_number, folder_id), ...] のリスト
    """
    if not updates:
        logger.info("書き込むデータがありません")
        return

    # A1記法でのセル範囲を準備
    data = []
    for row, value in updates:
        range_name = f"'{sheet_name}'!{column}{row}"
        data.append({
            'range': range_name,
            'values': [[value]]
        })

    def api_call():
        body = {
            'valueInputOption': 'RAW',
            'data': data
        }
        result = sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        return result

    try:
        result = retry_on_api_error(api_call)
        updated_cells = result.get('totalUpdatedCells', 0)
        logger.info(f"スプレッドシート書き込み完了: {updated_cells}セルを更新")
    except Exception as error:
        logger.error(f"スプレッドシート書き込みエラー: {error}")
        raise

def main():
    parser = argparse.ArgumentParser(description='スプレッドシートとGoogle Driveフォルダを連携')
    parser.add_argument('base_folder_id', help='基準フォルダID')
    parser.add_argument('spreadsheet_id', help='スプレッドシートID')
    parser.add_argument('sheet_id', type=int, help='シートID（数値）')
    parser.add_argument('column', help='書き込み先の列（例: C）')

    args = parser.parse_args()

    logger.info("=== スプレッドシート-フォルダ連携開始 ===")
    logger.info(f"基準フォルダID: {args.base_folder_id}")
    logger.info(f"スプレッドシートID: {args.spreadsheet_id}")
    logger.info(f"シートID: {args.sheet_id}")
    logger.info(f"書き込み列: {args.column}")

    try:
        # 認証情報を取得
        logger.info("認証情報を取得中...")
        creds = get_credentials()

        # Google Drive APIサービスを作成
        drive_service = build('drive', 'v3', credentials=creds)

        # Google Sheets APIサービスを作成
        sheets_service = build('sheets', 'v4', credentials=creds)

        # スプレッドシートからデータを読み込み
        logger.info("スプレッドシートを読み込み中...")
        sheet_data, sheet_name = read_spreadsheet(sheets_service, args.spreadsheet_id, args.sheet_id)

        # フォルダマップを作成
        logger.info("フォルダ構造を探索中...")
        folder_map = build_folder_map(drive_service, args.base_folder_id)

        # マッチング処理
        logger.info("マッチング処理開始...")
        updates = []
        found_count = 0
        not_found_count = 0
        total_count = len(sheet_data)

        for idx, data in enumerate(sheet_data, 1):
            row = data['row']
            prefecture = data['prefecture']
            city = data['city']
            key = (prefecture, city)

            if key in folder_map:
                folder_id = folder_map[key]
                updates.append((row, folder_id))
                found_count += 1
                logger.info(f"[{idx}/{total_count}] {prefecture}/{city} -> {folder_id}")
            else:
                updates.append((row, "not found"))
                not_found_count += 1
                logger.warning(f"[{idx}/{total_count}] {prefecture}/{city} -> not found")

        # スプレッドシートに書き込み
        logger.info("スプレッドシートに書き込み中...")
        write_to_spreadsheet(sheets_service, args.spreadsheet_id, sheet_name, args.column, updates)

        # 統計情報を表示
        logger.info("=== 統計情報 ===")
        logger.info(f"総データ数: {total_count}")
        logger.info(f"見つかった: {found_count}")
        logger.info(f"見つからない: {not_found_count}")

        logger.info("=== 処理完了 ===")

    except Exception as e:
        logger.error(f"予期しないエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
