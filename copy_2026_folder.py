#!/usr/bin/env python3
"""
Googleドライブのフォルダを2026年衆院選用にコピーするスクリプト

使用方法:
    python copy_2026_folder.py [コピー先フォルダID] [--dry-run]

機能:
    - my_settings.jsonで指定された基底フォルダ（BASE_FOLDER_ID）の直下の層からコピー
    - {prefecture}/{city} または 立候補者なし/{prefecture}/{city} の階層のみをコピー
    - *_normalized_final.csv または *_normalized_final_upd.csv のみをコピー対象とする
    - 「2025参院選後」または「2025参議院選挙後」フォルダは除外、その中の*_normalized_final_upd.csvは親フォルダにコピー
    - *_normalized_final.csvファイルは同じフォルダにコピー
"""

import argparse
import json
import logging
import os
import sys
import time
import random
from typing import Dict, List, Optional, Set, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('copy_2026_folder.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# グローバル進捗カウンタ
progress_counter = {'current': 0, 'total': 0}

# グローバル統計情報
statistics = {
    'normalized_final_csv': {'total': 0, 'copied': 0, 'skipped': 0},
    'normalized_final_upd_csv': {'total': 0, 'copied': 0, 'skipped': 0}
}

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

def load_settings():
    """my_settings.jsonから設定を読み込む"""
    try:
        with open('my_settings.json', encoding='utf-8') as f:
            settings = json.load(f)
        return settings
    except FileNotFoundError:
        logger.error("my_settings.jsonファイルが見つかりません")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"my_settings.jsonのJSON形式が不正です: {e}")
        sys.exit(1)

def get_credentials():
    """Google API認証情報を取得"""
    OAUTH2_CLIENT_INFO = load_secrets()
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
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
                fields='nextPageToken, files(id, name, mimeType, parents)',
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

def get_folder_name(service, folder_id: str) -> Optional[str]:
    """フォルダ名を取得"""
    def api_call():
        folder_metadata = service.files().get(
            fileId=folder_id,
            fields='name',
            supportsAllDrives=True
        ).execute()
        return folder_metadata

    try:
        folder_metadata = retry_on_api_error(api_call)
        return folder_metadata['name']
    except Exception as error:
        logger.error(f"フォルダ名取得エラー: {error}")
        return None

def validate_folder_id(service, folder_id: str) -> bool:
    """フォルダIDの妥当性をチェック"""
    def api_call():
        folder_metadata = service.files().get(
            fileId=folder_id,
            fields='mimeType',
            supportsAllDrives=True
        ).execute()
        return folder_metadata

    try:
        folder_metadata = retry_on_api_error(api_call)

        if folder_metadata['mimeType'] != 'application/vnd.google-apps.folder':
            logger.error(f"指定されたID {folder_id} はフォルダではありません")
            return False

        return True

    except Exception as error:
        logger.error(f"フォルダID検証エラー: {error}")
        return False

def create_folder(service, folder_name: str, parent_id: str) -> Optional[str]:
    """フォルダを作成"""
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }

    def api_call():
        new_folder = service.files().create(
            body=folder_metadata,
            supportsAllDrives=True,
            fields='id,name'
        ).execute()
        return new_folder

    try:
        new_folder = retry_on_api_error(api_call)
        logger.info(f"フォルダ作成: {folder_name} (ID: {new_folder['id']})")
        return new_folder['id']
    except Exception as error:
        logger.error(f"フォルダ作成エラー: {error}")
        return None

def copy_file(service, file_id: str, new_parent_id: str, new_name: Optional[str] = None, show_progress: bool = False) -> Optional[str]:
    """ファイルをコピー"""
    def api_call():
        # ファイルのメタデータを取得
        file_metadata = service.files().get(
            fileId=file_id,
            fields='name,mimeType',
            supportsAllDrives=True
        ).execute()

        # コピー用のメタデータを作成
        copy_metadata = {
            'parents': [new_parent_id]
        }

        # 新しい名前が指定されている場合は設定
        if new_name:
            copy_metadata['name'] = new_name

        # ファイルをコピー
        copied_file = service.files().copy(
            fileId=file_id,
            body=copy_metadata,
            supportsAllDrives=True,
            fields='id,name'
        ).execute()

        return file_metadata, copied_file

    try:
        file_metadata, copied_file = retry_on_api_error(api_call)

        # 進捗表示
        if show_progress and progress_counter['total'] > 0:
            progress_counter['current'] += 1
            logger.info(f"[{progress_counter['current']}/{progress_counter['total']}] ファイルコピー: {file_metadata['name']} -> {copied_file['name']} (ID: {copied_file['id']})")
        else:
            logger.info(f"ファイルコピー: {file_metadata['name']} -> {copied_file['name']} (ID: {copied_file['id']})")

        return copied_file['id']
    except Exception as error:
        logger.error(f"ファイルコピーエラー: {error}")
        return None

def find_existing_folder(service, parent_id: str, folder_name: str) -> Optional[str]:
    """親フォルダ内に同名のフォルダが既に存在するか確認"""
    items = list_drive_files(service, parent_id)
    for item in items:
        if item['name'].strip() == folder_name and item['mimeType'] == 'application/vnd.google-apps.folder':
            return item['id']
    return None

def find_existing_file(service, parent_id: str, file_name: str) -> Optional[Dict]:
    """親フォルダ内に同名のファイルが既に存在するか確認"""
    items = list_drive_files(service, parent_id)
    for item in items:
        if item['name'].strip() == file_name and item['mimeType'] != 'application/vnd.google-apps.folder':
            return item
    return None

def get_file_modified_time(service, file_id: str) -> Optional[str]:
    """ファイルの更新日時を取得"""
    def api_call():
        file_metadata = service.files().get(
            fileId=file_id,
            fields='modifiedTime',
            supportsAllDrives=True
        ).execute()
        return file_metadata

    try:
        file_metadata = retry_on_api_error(api_call)
        return file_metadata.get('modifiedTime')
    except Exception as error:
        logger.error(f"ファイル更新日時取得エラー: {error}")
        return None

def should_copy_file(service, source_file_id: str, existing_file: Optional[Dict]) -> Tuple[bool, str]:
    """
    ファイルをコピーすべきか判定

    Returns:
        (bool, str): (コピーすべきか, 理由)
    """
    if not existing_file:
        return True, "新規"

    # コピー元の更新日時を取得
    source_modified_time = get_file_modified_time(service, source_file_id)
    if not source_modified_time:
        logger.warning("コピー元ファイルの更新日時が取得できません。コピーをスキップします。")
        return False, "コピー元の更新日時取得失敗"

    # コピー先の更新日時を取得
    existing_file_id = existing_file['id']
    existing_modified_time = get_file_modified_time(service, existing_file_id)
    if not existing_modified_time:
        logger.warning("コピー先ファイルの更新日時が取得できません。上書きコピーします。")
        return True, "コピー先の更新日時取得失敗"

    # 日時を比較（ISO 8601形式の文字列比較で十分）
    if source_modified_time > existing_modified_time:
        return True, f"更新あり (コピー元: {source_modified_time}, コピー先: {existing_modified_time})"
    else:
        return False, f"更新なし (コピー元: {source_modified_time}, コピー先: {existing_modified_time})"

def delete_file(service, file_id: str) -> bool:
    """ファイルまたはフォルダを削除"""
    def api_call():
        service.files().delete(
            fileId=file_id,
            supportsAllDrives=True
        ).execute()

    try:
        retry_on_api_error(api_call)
        logger.info(f"ファイル削除: {file_id}")
        return True
    except Exception as error:
        logger.error(f"ファイル削除エラー: {error}")
        return False

def count_target_files(service, source_folder_id: str) -> int:
    """コピー対象のファイル数をカウント"""
    total_count = 0

    # 第1階層（都道府県）を取得
    prefecture_items = list_drive_files(service, source_folder_id)

    # フォルダのみをフィルタリング
    prefecture_folders = [item for item in prefecture_items if item['mimeType'] == 'application/vnd.google-apps.folder']
    total_prefectures = len(prefecture_folders)

    logger.info(f"都道府県数: {total_prefectures}")

    for pref_idx, prefecture_item in enumerate(prefecture_folders, 1):
        prefecture_name = prefecture_item['name']
        prefecture_id = prefecture_item['id']

        # 第2階層（市区町村 または 立候補者なし）を取得
        second_level_items = list_drive_files(service, prefecture_id)

        # フォルダのみをフィルタリング
        city_folders = [item for item in second_level_items if item['mimeType'] == 'application/vnd.google-apps.folder']
        total_cities = len(city_folders)

        for city_idx, second_item in enumerate(city_folders, 1):
            second_name = second_item['name']
            second_id = second_item['id']

            # 進捗ログ出力
            logger.info(f"[{pref_idx}/{total_prefectures}][{city_idx}/{total_cities}] カウント中: {prefecture_name}/{second_name}")

            if second_name == "立候補者なし":
                # 立候補者なしフォルダの場合、その下の市区町村フォルダを処理
                city_items = list_drive_files(service, second_id)
                for city_item in city_items:
                    if city_item['mimeType'] != 'application/vnd.google-apps.folder':
                        continue

                    city_id = city_item['id']
                    city_files = list_drive_files(service, city_id)

                    # CSVファイルをカウント
                    for file_item in city_files:
                        if file_item['mimeType'] == 'application/vnd.google-apps.folder':
                            # 「2025参院選後」または「2025参議院選挙後」フォルダの中身もチェック
                            if file_item['name'] == "2025参院選後" or file_item['name'] == "2025参議院選挙後":
                                inner_files = list_drive_files(service, file_item['id'])
                                for inner_file in inner_files:
                                    if inner_file['name'].endswith('_normalized_final_upd.csv'):
                                        total_count += 1
                        elif file_item['name'].endswith('_normalized_final.csv') or file_item['name'].endswith('_normalized_final_upd.csv'):
                            total_count += 1
            else:
                # 通常の市区町村フォルダ
                city_files = list_drive_files(service, second_id)

                # CSVファイルをカウント
                for file_item in city_files:
                    if file_item['mimeType'] == 'application/vnd.google-apps.folder':
                        # 「2025参院選後」または「2025参議院選挙後」フォルダの中身もチェック
                        if file_item['name'] == "2025参院選後" or file_item['name'] == "2025参議院選挙後":
                            inner_files = list_drive_files(service, file_item['id'])
                            for inner_file in inner_files:
                                if inner_file['name'].endswith('_normalized_final_upd.csv'):
                                    total_count += 1
                    elif file_item['name'].endswith('_normalized_final.csv') or file_item['name'].endswith('_normalized_final_upd.csv'):
                        total_count += 1

    return total_count

def add_suffix_to_filename(filename: str, suffix: str) -> str:
    """
    ファイル名にサフィックスを付加

    例: add_suffix_to_filename('大磯町_normalized_final.csv', '_末尾')
        → '大磯町_normalized_final_末尾.csv'
    """
    if not suffix:
        return filename

    # .csv の前にサフィックスを挿入
    if filename.endswith('.csv'):
        return filename[:-4] + suffix + '.csv'
    return filename + suffix

def process_city_folder(service, city_folder_id: str, target_city_folder_id: str, city_name: str, dry_run: bool = False, suffix: str = ''):
    """市区町村フォルダ内のCSVファイルを処理"""
    items = list_drive_files(service, city_folder_id)

    for item in items:
        item_name = item['name'].strip()  # ファイル名の前後の空白を削除
        item_id = item['id']
        mime_type = item['mimeType']

        if mime_type == 'application/vnd.google-apps.folder':
            # 「2025参院選後」または「2025参議院選挙後」フォルダの特別処理
            if item_name == "2025参院選後" or item_name == "2025参議院選挙後":
                logger.info(f"  「{item_name}」フォルダを検出 - 中身の*_normalized_final_upd.csvを親フォルダにコピーします")

                inner_items = list_drive_files(service, item_id)
                logger.info(f"    フォルダ内のファイル数: {len(inner_items)}")
                for inner_item in inner_items:
                    inner_item_name = inner_item['name'].strip()  # ファイル名の前後の空白を削除
                    logger.info(f"    検出ファイル: {inner_item_name} (判定: {inner_item_name.endswith('_normalized_final_upd.csv')})")
                    if inner_item_name.endswith('_normalized_final_upd.csv'):
                        # 統計情報をカウント
                        statistics['normalized_final_upd_csv']['total'] += 1

                        # サフィックスを付加したファイル名を生成
                        target_file_name = add_suffix_to_filename(inner_item_name, suffix)

                        if dry_run:
                            logger.info(f"    [DRY-RUN] ファイルコピー: {inner_item_name} -> {target_file_name}")
                        else:
                            # 既存のファイルを確認
                            existing_file = find_existing_file(service, target_city_folder_id, target_file_name)

                            # 更新日時を比較してコピーすべきか判定
                            should_copy, reason = should_copy_file(service, inner_item['id'], existing_file)

                            if should_copy:
                                if existing_file:
                                    logger.info(f"    上書きコピー: {inner_item_name} -> {target_file_name} ({reason})")
                                    delete_file(service, existing_file['id'])
                                else:
                                    logger.info(f"    新規コピー: {inner_item_name} -> {target_file_name}")

                                # ファイルをコピー（サフィックス付きファイル名で）
                                copy_file(service, inner_item['id'], target_city_folder_id, new_name=target_file_name, show_progress=False)
                                statistics['normalized_final_upd_csv']['copied'] += 1
                                time.sleep(0.1)
                            else:
                                logger.info(f"    スキップ（更新なし）: {target_file_name} ({reason})")
                                statistics['normalized_final_upd_csv']['skipped'] += 1
            else:
                # その他のフォルダはスキップ
                logger.info(f"  スキップ（3階層以下のフォルダ）: {item_name}")
        elif item_name.endswith('_normalized_final.csv') or item_name.endswith('_normalized_final_upd.csv'):
            # 対象のCSVファイルをコピー
            # 統計情報をカウント（ファイルの種類を判定）
            if item_name.endswith('_normalized_final_upd.csv'):
                statistics['normalized_final_upd_csv']['total'] += 1
                file_type = 'normalized_final_upd_csv'
            else:  # _normalized_final.csv
                statistics['normalized_final_csv']['total'] += 1
                file_type = 'normalized_final_csv'

            # サフィックスを付加したファイル名を生成
            target_file_name = add_suffix_to_filename(item_name, suffix)

            if dry_run:
                logger.info(f"    [DRY-RUN] ファイルコピー: {item_name} -> {target_file_name}")
            else:
                # 既存のファイルを確認
                existing_file = find_existing_file(service, target_city_folder_id, target_file_name)

                # 更新日時を比較してコピーすべきか判定
                should_copy, reason = should_copy_file(service, item_id, existing_file)

                if should_copy:
                    if existing_file:
                        logger.info(f"    上書きコピー: {item_name} -> {target_file_name} ({reason})")
                        delete_file(service, existing_file['id'])
                    else:
                        logger.info(f"    新規コピー: {item_name} -> {target_file_name}")

                    # ファイルをコピー（サフィックス付きファイル名で）
                    copy_file(service, item_id, target_city_folder_id, new_name=target_file_name, show_progress=False)
                    statistics[file_type]['copied'] += 1
                    time.sleep(0.1)
                else:
                    logger.info(f"    スキップ（更新なし）: {target_file_name} ({reason})")
                    statistics[file_type]['skipped'] += 1
        else:
            # 対象外のファイルはスキップ
            logger.info(f"    スキップ（対象外のFile）: {item_name}")

def copy_structure(service, source_folder_id: str, target_folder_id: str, dry_run: bool = False, suffix: str = ''):
    """
    フォルダ構造をコピー

    処理対象:
    - {prefecture}/{city} の構造
    - 立候補者なし/{prefecture}/{city} の構造
    - *_normalized_final.csv または *_normalized_final_upd.csv のみ

    特別な処理:
    - 「2025参院選後」または「2025参議院選挙後」フォルダは除外、その中の*_normalized_final_upd.csvのみを親フォルダにコピー
    - suffixが指定されている場合、コピー先ファイル名に付加
    """
    logger.info("=== フォルダ構造のコピー開始 ===")

    # 第1階層（都道府県 または 立候補者なし）を取得
    first_level_items = list_drive_files(service, source_folder_id)

    # フォルダのみをフィルタリング
    first_level_folders = [item for item in first_level_items if item['mimeType'] == 'application/vnd.google-apps.folder']
    total_first_level = len(first_level_folders)

    logger.info(f"第1階層フォルダ数: {total_first_level}")

    for first_idx, first_item in enumerate(first_level_folders, 1):
        first_name = first_item['name'].strip()  # フォルダ名の前後の空白を削除
        first_id = first_item['id']

        if first_name == "立候補者なし":
            # 立候補者なしフォルダの場合
            logger.info(f"[{first_idx}/{total_first_level}] 立候補者なしフォルダ: {first_name}")

            # 立候補者なしフォルダを作成または既存フォルダを使用
            if dry_run:
                logger.info(f"  [DRY-RUN] フォルダ作成: {first_name}")
                target_nocandidate_id = None
            else:
                existing_nocandidate_id = find_existing_folder(service, target_folder_id, first_name)
                if existing_nocandidate_id:
                    logger.info(f"  既存のフォルダを使用: {first_name} (ID: {existing_nocandidate_id})")
                    target_nocandidate_id = existing_nocandidate_id
                else:
                    target_nocandidate_id = create_folder(service, first_name, target_folder_id)
                    if not target_nocandidate_id:
                        logger.error(f"  フォルダ作成に失敗: {first_name}")
                        continue

            # 立候補者なしフォルダの下の都道府県フォルダを処理
            nocandidate_prefecture_items = list_drive_files(service, first_id)
            nocandidate_prefecture_folders = [item for item in nocandidate_prefecture_items if item['mimeType'] == 'application/vnd.google-apps.folder']
            total_nocandidate_prefectures = len(nocandidate_prefecture_folders)

            logger.info(f"  立候補者なし配下の都道府県数: {total_nocandidate_prefectures}")

            for nc_pref_idx, nc_prefecture_item in enumerate(nocandidate_prefecture_folders, 1):
                nc_prefecture_name = nc_prefecture_item['name'].strip()  # フォルダ名の前後の空白を削除
                nc_prefecture_id = nc_prefecture_item['id']

                logger.info(f"  [{first_idx}/{total_first_level}][{nc_pref_idx}/{total_nocandidate_prefectures}] 都道府県フォルダ: {nc_prefecture_name}")

                # 都道府県フォルダを作成または既存フォルダを使用
                if dry_run:
                    logger.info(f"    [DRY-RUN] フォルダ作成: {nc_prefecture_name}")
                    target_nc_prefecture_id = None
                else:
                    existing_nc_prefecture_id = find_existing_folder(service, target_nocandidate_id, nc_prefecture_name)
                    if existing_nc_prefecture_id:
                        logger.info(f"    既存のフォルダを使用: {nc_prefecture_name} (ID: {existing_nc_prefecture_id})")
                        target_nc_prefecture_id = existing_nc_prefecture_id
                    else:
                        target_nc_prefecture_id = create_folder(service, nc_prefecture_name, target_nocandidate_id)
                        if not target_nc_prefecture_id:
                            logger.error(f"    フォルダ作成に失敗: {nc_prefecture_name}")
                            continue

                # 第3階層（市区町村）を取得
                nc_city_items = list_drive_files(service, nc_prefecture_id)
                nc_city_folders = [item for item in nc_city_items if item['mimeType'] == 'application/vnd.google-apps.folder']
                total_nc_cities = len(nc_city_folders)

                for nc_city_idx, nc_city_item in enumerate(nc_city_folders, 1):
                    nc_city_name = nc_city_item['name'].strip()  # フォルダ名の前後の空白を削除
                    nc_city_id = nc_city_item['id']

                    logger.info(f"    [{first_idx}/{total_first_level}][{nc_pref_idx}/{total_nocandidate_prefectures}][{nc_city_idx}/{total_nc_cities}] 市区町村フォルダ: {nc_city_name}")

                    # 市区町村フォルダを作成または既存フォルダを使用
                    if dry_run:
                        logger.info(f"      [DRY-RUN] フォルダ作成: {nc_city_name}")
                        # ドライランでもCSVファイルを検出して表示
                        process_city_folder(service, nc_city_id, None, nc_city_name, dry_run, suffix)
                    else:
                        existing_nc_city_id = find_existing_folder(service, target_nc_prefecture_id, nc_city_name)
                        if existing_nc_city_id:
                            logger.info(f"      既存のフォルダを使用: {nc_city_name} (ID: {existing_nc_city_id})")
                            target_nc_city_id = existing_nc_city_id
                        else:
                            target_nc_city_id = create_folder(service, nc_city_name, target_nc_prefecture_id)
                            if not target_nc_city_id:
                                logger.error(f"      フォルダ作成に失敗: {nc_city_name}")
                                continue

                        # CSVファイルを処理
                        process_city_folder(service, nc_city_id, target_nc_city_id, nc_city_name, dry_run, suffix)

        else:
            # 通常の都道府県フォルダ
            prefecture_name = first_name
            prefecture_id = first_id

            logger.info(f"[{first_idx}/{total_first_level}] 都道府県フォルダ: {prefecture_name}")

            # 都道府県フォルダを作成または既存フォルダを使用
            if dry_run:
                logger.info(f"  [DRY-RUN] フォルダ作成: {prefecture_name}")
                target_prefecture_id = None
            else:
                existing_prefecture_id = find_existing_folder(service, target_folder_id, prefecture_name)
                if existing_prefecture_id:
                    logger.info(f"  既存のフォルダを使用: {prefecture_name} (ID: {existing_prefecture_id})")
                    target_prefecture_id = existing_prefecture_id
                else:
                    target_prefecture_id = create_folder(service, prefecture_name, target_folder_id)
                    if not target_prefecture_id:
                        logger.error(f"  フォルダ作成に失敗: {prefecture_name}")
                        continue

            # 第2階層（市区町村）を取得
            city_items = list_drive_files(service, prefecture_id)
            city_folders = [item for item in city_items if item['mimeType'] == 'application/vnd.google-apps.folder']
            total_cities = len(city_folders)

            for city_idx, city_item in enumerate(city_folders, 1):
                city_name = city_item['name'].strip()  # フォルダ名の前後の空白を削除
                city_id = city_item['id']

                logger.info(f"  [{first_idx}/{total_first_level}][{city_idx}/{total_cities}] 市区町村フォルダ: {city_name}")

                # 市区町村フォルダを作成または既存フォルダを使用
                if dry_run:
                    logger.info(f"    [DRY-RUN] フォルダ作成: {city_name}")
                    # ドライランでもCSVファイルを検出して表示
                    process_city_folder(service, city_id, None, city_name, dry_run, suffix)
                else:
                    existing_city_id = find_existing_folder(service, target_prefecture_id, city_name)
                    if existing_city_id:
                        logger.info(f"    既存のフォルダを使用: {city_name} (ID: {existing_city_id})")
                        target_city_id = existing_city_id
                    else:
                        target_city_id = create_folder(service, city_name, target_prefecture_id)
                        if not target_city_id:
                            logger.error(f"    フォルダ作成に失敗: {city_name}")
                            continue

                    # CSVファイルを処理
                    process_city_folder(service, city_id, target_city_id, city_name, dry_run, suffix)

    logger.info("=== フォルダ構造のコピー完了 ===")

def main():
    parser = argparse.ArgumentParser(description='Googleドライブのフォルダを2026年衆院選用にコピー')
    parser.add_argument('target_folder_id', nargs='?', help='コピー先フォルダID（省略時はmy_settings.jsonのDEST_FOLDER_IDを使用）')
    parser.add_argument('--dry-run', action='store_true', help='実際のコピーは行わず、処理内容のみ表示')
    parser.add_argument('--suffix', type=str, default='', help='コピー先ファイル名に付加するサフィックス（例: --suffix=_末尾 → *_normalized_final_末尾.csv）')

    args = parser.parse_args()

    logger.info("=== フォルダコピー開始 ===")
    logger.info(f"ドライラン: {args.dry_run}")
    if args.suffix:
        logger.info(f"ファイル名サフィックス: {args.suffix}")

    try:
        # 認証情報を取得
        logger.info("認証情報を取得中...")
        creds = get_credentials()

        # Google Drive APIサービスを作成
        service = build('drive', 'v3', credentials=creds)

        # 設定を読み込み
        settings = load_settings()

        # コピー元フォルダIDを取得
        if 'BASE_FOLDER_ID' not in settings:
            logger.error("my_settings.jsonにBASE_FOLDER_IDが設定されていません")
            sys.exit(1)
        source_folder_id = settings['BASE_FOLDER_ID']
        logger.info(f"コピー元フォルダID: {source_folder_id}")

        # コピー先フォルダIDを決定
        if args.target_folder_id:
            target_folder_id = args.target_folder_id
            logger.info(f"コピー先フォルダID: {target_folder_id} (引数指定)")
        else:
            # my_settings.jsonから読み込み
            if 'DEST_FOLDER_ID' not in settings:
                logger.error("コピー先フォルダIDが指定されていません。引数で指定するか、my_settings.jsonにDEST_FOLDER_IDを設定してください")
                sys.exit(1)
            target_folder_id = settings['DEST_FOLDER_ID']
            logger.info(f"コピー先フォルダID: {target_folder_id} (my_settings.jsonから読み込み)")

        # コピー元フォルダの妥当性をチェック
        logger.info("コピー元フォルダの妥当性をチェック中...")
        if not validate_folder_id(service, source_folder_id):
            sys.exit(1)

        # コピー先フォルダの妥当性をチェック
        logger.info("コピー先フォルダの妥当性をチェック中...")
        if not validate_folder_id(service, target_folder_id):
            sys.exit(1)

        # コピー元フォルダ名を取得
        source_folder_name = get_folder_name(service, source_folder_id)
        if not source_folder_name:
            sys.exit(1)

        logger.info(f"コピー元フォルダ名: {source_folder_name}")

        # 統計情報をリセット
        statistics['normalized_final_csv'] = {'total': 0, 'copied': 0, 'skipped': 0}
        statistics['normalized_final_upd_csv'] = {'total': 0, 'copied': 0, 'skipped': 0}

        # フォルダ構造をコピー
        copy_structure(service, source_folder_id, target_folder_id, args.dry_run, args.suffix)

        # 統計情報を表示
        logger.info("=== 統計情報 ===")
        logger.info(f"*_normalized_final.csv:")
        logger.info(f"  総数: {statistics['normalized_final_csv']['total']}")
        logger.info(f"  コピー済み: {statistics['normalized_final_csv']['copied']}")
        logger.info(f"  スキップ: {statistics['normalized_final_csv']['skipped']}")
        logger.info(f"*_normalized_final_upd.csv:")
        logger.info(f"  総数: {statistics['normalized_final_upd_csv']['total']}")
        logger.info(f"  コピー済み: {statistics['normalized_final_upd_csv']['copied']}")
        logger.info(f"  スキップ: {statistics['normalized_final_upd_csv']['skipped']}")

        if args.dry_run:
            logger.info("=== ドライラン完了 ===")
        else:
            logger.info("=== フォルダコピー完了 ===")
            logger.info(f"コピー先フォルダID: {target_folder_id}")

    except Exception as e:
        logger.error(f"予期しないエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
