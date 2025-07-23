#!/usr/bin/env python3
"""
共有ドライブのフォルダを丸ごとコピーするスクリプト

使用方法:
    python backup_folder.py <コピー先フォルダID>

機能:
    - my_settings.jsonで指定された基底フォルダを
    - 引数で指定されたフォルダIDの直下に丸ごとコピー
    - フォルダ構造を維持して再帰的にコピー
"""

import argparse
import json
import logging
import os
import sys
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('backup_folder.log', encoding='utf-8'),
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

class CheckpointManager:
    """チェックポイント管理クラス"""
    
    def __init__(self, checkpoint_file: str = 'backup_checkpoint.json'):
        self.checkpoint_file = checkpoint_file
        self.processed_folders: Set[str] = set()
        self.backup_folder_id: Optional[str] = None
        self.source_folder_id: Optional[str] = None
        self.target_folder_id: Optional[str] = None
        self.backup_folder_name: Optional[str] = None
        self.start_time: Optional[datetime] = None
        
    def load_checkpoint(self) -> bool:
        """チェックポイントを読み込み"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_folders = set(data.get('processed_folders', []))
                    self.backup_folder_id = data.get('backup_folder_id')
                    self.source_folder_id = data.get('source_folder_id')
                    self.target_folder_id = data.get('target_folder_id')
                    self.backup_folder_name = data.get('backup_folder_name')
                    start_time_str = data.get('start_time')
                    if start_time_str:
                        self.start_time = datetime.fromisoformat(start_time_str)
                    
                    logger.info(f"チェックポイントを読み込みました: {len(self.processed_folders)}個のフォルダが処理済み")
                    return True
        except Exception as e:
            logger.warning(f"チェックポイントの読み込みに失敗: {e}")
        return False
    
    def save_checkpoint(self):
        """チェックポイントを保存"""
        try:
            data = {
                'processed_folders': list(self.processed_folders),
                'backup_folder_id': self.backup_folder_id,
                'source_folder_id': self.source_folder_id,
                'target_folder_id': self.target_folder_id,
                'backup_folder_name': self.backup_folder_name,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"チェックポイントの保存に失敗: {e}")
    
    def add_processed_folder(self, folder_name: str):
        """処理済みフォルダを追加"""
        self.processed_folders.add(folder_name)
        self.save_checkpoint()
    
    def is_processed(self, folder_name: str) -> bool:
        """フォルダが処理済みかチェック"""
        return folder_name in self.processed_folders
    
    def clear_checkpoint(self):
        """チェックポイントをクリア"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                logger.info("チェックポイントをクリアしました")
        except Exception as e:
            logger.error(f"チェックポイントのクリアに失敗: {e}")

class ProgressTracker:
    """進捗状況を追跡するクラス"""
    
    def __init__(self):
        self.total_items = 0
        self.processed_items = 0
        self.start_time = None
        self.folder_count = 0
        self.file_count = 0
        
    def start(self, total_items: int):
        """進捗追跡を開始"""
        self.total_items = total_items
        self.processed_items = 0
        self.start_time = time.time()
        self.folder_count = 0
        self.file_count = 0
        logger.info(f"=== 進捗追跡開始 ===")
        logger.info(f"総アイテム数: {total_items}")
        
    def update(self, item_type: str = "item"):
        """進捗を更新"""
        self.processed_items += 1
        if item_type == "folder":
            self.folder_count += 1
        elif item_type == "file":
            self.file_count += 1
            
        if self.start_time:
            elapsed_time = time.time() - self.start_time
            if self.processed_items > 0:
                items_per_sec = self.processed_items / elapsed_time
                remaining_items = self.total_items - self.processed_items
                if items_per_sec > 0:
                    estimated_remaining = remaining_items / items_per_sec
                    remaining_str = str(timedelta(seconds=int(estimated_remaining)))
                else:
                    remaining_str = "計算中..."
                
                progress_percent = (self.processed_items / self.total_items) * 100
                
                logger.info(f"進捗: {self.processed_items}/{self.total_items} ({progress_percent:.1f}%) "
                          f"- フォルダ: {self.folder_count}, ファイル: {self.file_count} "
                          f"- 速度: {items_per_sec:.1f} アイテム/秒 "
                          f"- 残り時間: {remaining_str}")
    
    def finish(self):
        """進捗追跡を完了"""
        if self.start_time:
            total_time = time.time() - self.start_time
            total_time_str = str(timedelta(seconds=int(total_time)))
            logger.info(f"=== 進捗追跡完了 ===")
            logger.info(f"総処理時間: {total_time_str}")
            logger.info(f"総処理アイテム数: {self.processed_items}")
            logger.info(f"フォルダ数: {self.folder_count}, ファイル数: {self.file_count}")

# グローバルな進捗追跡オブジェクト
progress_tracker = ProgressTracker()
checkpoint_manager = CheckpointManager()

# 認証情報の読み込み
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

def list_drive_files(service, folder_id: str, skip_shortcuts: bool = False) -> List[Dict]:
    """フォルダ内の全ファイル・フォルダを取得"""
    files = []
    page_token = None
    
    while True:
        def api_call():
            response = service.files().list(
                q=f"'{folder_id}' in parents",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, size, parents)',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token
            ).execute()
            return response
        
        try:
            response = retry_on_api_error(api_call)
            
            # ショートカットをスキップする場合
            if skip_shortcuts:
                filtered_files = [f for f in response.get('files', []) 
                                if f['mimeType'] != 'application/vnd.google-apps.shortcut']
                files.extend(filtered_files)
            else:
                files.extend(response.get('files', []))
            
            page_token = response.get('nextPageToken', None)
            
            if not page_token:
                break
                
        except Exception as error:
            logger.error(f"ファイル一覧取得エラー: {error}")
            break
    
    return files

def count_total_items_recursive(service, folder_id: str, skip_shortcuts: bool = False) -> int:
    """フォルダ内の総アイテム数を再帰的にカウント"""
    total = 0
    items = list_drive_files(service, folder_id, skip_shortcuts)
    
    for item in items:
        total += 1  # 現在のアイテムをカウント
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # サブフォルダの場合は再帰的にカウント
            total += count_total_items_recursive(service, item['id'], skip_shortcuts)
    
    return total

def copy_file(service, file_id: str, new_parent_id: str, new_name: Optional[str] = None) -> Optional[str]:
    """ファイルまたはフォルダをコピー"""
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
        
        logger.info(f"コピー完了: {file_metadata['name']} -> {copied_file['name']} (ID: {copied_file['id']})")
        
        # 進捗を更新
        progress_tracker.update("file")
        
        return copied_file['id']
        
    except Exception as error:
        logger.error(f"ファイルコピーエラー: {error}")
        return None

def copy_folder_recursive(service, source_folder_id: str, target_parent_id: str, 
                         folder_name: str, depth: int = 0, skip_shortcuts: bool = False) -> Optional[str]:
    """フォルダを再帰的にコピー"""
    indent = "  " * depth
    logger.info(f"{indent}フォルダ処理開始: {folder_name}")
    
    try:
        # 新しいフォルダを作成
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [target_parent_id]
        }
        
        def create_folder_api():
            new_folder = service.files().create(
                body=folder_metadata,
                supportsAllDrives=True,
                fields='id,name'
            ).execute()
            return new_folder
        
        new_folder = retry_on_api_error(create_folder_api)
        
        new_folder_id = new_folder['id']
        logger.info(f"{indent}フォルダ作成完了: {folder_name} (ID: {new_folder_id})")
        
        # 進捗を更新（フォルダ作成）
        progress_tracker.update("folder")
        
        # フォルダ内のアイテムを取得
        items = list_drive_files(service, source_folder_id, skip_shortcuts)
        
        if not items:
            logger.info(f"{indent}フォルダ {folder_name} は空です")
            return new_folder_id
        
        # 各アイテムを処理
        for item in items:
            item_name = item['name']
            item_id = item['id']
            mime_type = item['mimeType']
            
            # ショートカットの場合はスキップ（念のため）
            if skip_shortcuts and mime_type == 'application/vnd.google-apps.shortcut':
                logger.info(f"{indent}ショートカットをスキップ: {item_name}")
                continue
            
            if mime_type == 'application/vnd.google-apps.folder':
                # サブフォルダの場合は再帰的にコピー
                copy_folder_recursive(service, item_id, new_folder_id, item_name, depth + 1, skip_shortcuts)
            else:
                # ファイルの場合は直接コピー
                copy_file(service, item_id, new_folder_id)
                time.sleep(0.1)  # API制限を避けるため少し待機
        
        logger.info(f"{indent}フォルダ処理完了: {folder_name}")
        return new_folder_id
        
    except Exception as error:
        logger.error(f"{indent}フォルダコピーエラー: {error}")
        return None

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

def display_folder_structure_recursive(service, folder_id: str, folder_name: str, depth: int = 0, skip_shortcuts: bool = False):
    """フォルダ構造を再帰的に表示（ドライラン用）"""
    indent = "  " * depth
    items = list_drive_files(service, folder_id, skip_shortcuts)
    
    if not items:
        logger.info(f"{indent}📁 {folder_name}/ (空)")
        return
    
    logger.info(f"{indent}📁 {folder_name}/")
    
    for item in items:
        item_name = item['name']
        mime_type = item['mimeType']
        
        if mime_type == 'application/vnd.google-apps.folder':
            # フォルダの場合は再帰的に表示
            display_folder_structure_recursive(service, item['id'], item_name, depth + 1, skip_shortcuts)
        elif mime_type == 'application/vnd.google-apps.shortcut':
            logger.info(f"{indent}  🔗 {item_name} (ショートカット)")
        else:
            logger.info(f"{indent}  📄 {item_name}")

def count_items_recursive(service, folder_id: str, skip_shortcuts: bool = False) -> Tuple[int, int, int]:
    """フォルダ内のアイテム数を再帰的にカウント（フォルダ数、ファイル数、ショートカット数）"""
    total_folders = 0
    total_files = 0
    total_shortcuts = 0
    
    items = list_drive_files(service, folder_id, skip_shortcuts)
    
    for item in items:
        mime_type = item['mimeType']
        
        if mime_type == 'application/vnd.google-apps.folder':
            total_folders += 1
            # サブフォルダ内も再帰的にカウント
            sub_folders, sub_files, sub_shortcuts = count_items_recursive(service, item['id'], skip_shortcuts)
            total_folders += sub_folders
            total_files += sub_files
            total_shortcuts += sub_shortcuts
        elif mime_type == 'application/vnd.google-apps.shortcut':
            total_shortcuts += 1
        else:
            total_files += 1
    
    return total_folders, total_files, total_shortcuts

def calculate_folder_size_recursive(service, folder_id: str, skip_shortcuts: bool = False) -> Tuple[int, int, int, int]:
    """フォルダ内の総サイズを再帰的に計算（バイト、フォルダ数、ファイル数、ショートカット数）"""
    total_size = 0
    total_folders = 0
    total_files = 0
    total_shortcuts = 0
    
    items = list_drive_files(service, folder_id, skip_shortcuts)
    
    for item in items:
        mime_type = item['mimeType']
        
        if mime_type == 'application/vnd.google-apps.folder':
            total_folders += 1
            # サブフォルダ内も再帰的に計算
            sub_size, sub_folders, sub_files, sub_shortcuts = calculate_folder_size_recursive(service, item['id'], skip_shortcuts)
            total_size += sub_size
            total_folders += sub_folders
            total_files += sub_files
            total_shortcuts += sub_shortcuts
        elif mime_type == 'application/vnd.google-apps.shortcut':
            total_shortcuts += 1
            # ショートカットのサイズは0として扱う
        else:
            total_files += 1
            # ファイルサイズを追加
            if 'size' in item:
                total_size += int(item['size'])
    
    return total_size, total_folders, total_files, total_shortcuts

def format_size(size_bytes):
    """バイトサイズを人間が読みやすい形式に変換"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"

def process_size_calculation(service, folder_id: str, folder_name: str, skip_shortcuts: bool = False):
    """フォルダサイズ計算の処理"""
    logger.info(f"=== フォルダサイズ計算開始 ===")
    logger.info(f"対象フォルダ: {folder_name} (ID: {folder_id})")
    logger.info(f"ショートカットをスキップ: {skip_shortcuts}")
    
    try:
        # フォルダサイズを計算
        logger.info("フォルダサイズを計算中...")
        total_size, total_folders, total_files, total_shortcuts = calculate_folder_size_recursive(service, folder_id, skip_shortcuts)
        
        # 結果を表示
        logger.info("=== サイズ計算結果 ===")
        logger.info(f"総サイズ: {format_size(total_size)}")
        logger.info(f"フォルダ数: {total_folders}")
        logger.info(f"ファイル数: {total_files}")
        logger.info(f"ショートカット数: {total_shortcuts}")
        logger.info(f"総アイテム数: {total_folders + total_files + total_shortcuts}")
        
        return total_size, total_folders, total_files, total_shortcuts
        
    except Exception as e:
        logger.error(f"サイズ計算エラー: {e}")
        return 0, 0, 0, 0

def main():
    parser = argparse.ArgumentParser(description='共有ドライブのフォルダを丸ごとコピー')
    parser.add_argument('target_folder_id', help='フォルダID（コピー先またはサイズ計算対象）')
    parser.add_argument('--dry-run', action='store_true', help='実際のコピーは行わず、処理内容のみ表示')
    parser.add_argument('--source-folder-id', help='コピー元のフォルダID（指定しない場合はmy_settings.jsonから読み込み）')
    parser.add_argument('--skip-shortcuts', action='store_true', help='ショートカットをスキップしてコピー')
    parser.add_argument('--resume', action='store_true', help='チェックポイントから再開')
    parser.add_argument('--clear-checkpoint', action='store_true', help='チェックポイントをクリア')
    parser.add_argument('-s', '--size', action='store_true', help='フォルダサイズ計算モード')
    
    args = parser.parse_args()
    
    logger.info("=== フォルダバックアップ開始 ===")
    logger.info(f"サイズ計算モード: {args.size}")
    
    if args.size:
        # サイズ計算モード
        logger.info("=== サイズ計算モード ===")
        
        try:
            # 認証情報を取得
            logger.info("認証情報を取得中...")
            creds = get_credentials()
            
            # Google Drive APIサービスを作成
            service = build('drive', 'v3', credentials=creds)
            
            # 対象フォルダID（引数で指定されたフォルダID）
            folder_id = args.target_folder_id
            logger.info(f"対象フォルダID: {folder_id}")
            
            # フォルダの妥当性をチェック
            logger.info("フォルダの妥当性をチェック中...")
            if not validate_folder_id(service, folder_id):
                sys.exit(1)
            
            # フォルダ名を取得
            folder_name = get_folder_name(service, folder_id)
            if not folder_name:
                sys.exit(1)
            
            logger.info(f"対象フォルダ名: {folder_name}")
            
            # サイズ計算を実行
            process_size_calculation(service, folder_id, folder_name, args.skip_shortcuts)
            
        except Exception as e:
            logger.error(f"予期しないエラーが発生しました: {e}")
            sys.exit(1)
        
        return
    
    # 通常のバックアップモード
    logger.info(f"コピー先フォルダID: {args.target_folder_id}")
    logger.info(f"ドライラン: {args.dry_run}")
    logger.info(f"ショートカットをスキップ: {args.skip_shortcuts}")
    logger.info(f"再開モード: {args.resume}")
    
    # チェックポイントのクリア
    if args.clear_checkpoint:
        checkpoint_manager.clear_checkpoint()
        return
    
    try:
        # 認証情報を取得
        logger.info("認証情報を取得中...")
        creds = get_credentials()
        
        # Google Drive APIサービスを作成
        service = build('drive', 'v3', credentials=creds)
        
        # コピー先フォルダの妥当性をチェック
        logger.info("コピー先フォルダの妥当性をチェック中...")
        if not validate_folder_id(service, args.target_folder_id):
            sys.exit(1)
        
        # コピー元フォルダIDを決定
        if args.source_folder_id:
            source_folder_id = args.source_folder_id
            logger.info(f"コピー元フォルダID: {source_folder_id} (引数指定)")
        else:
            # my_settings.jsonから読み込み
            settings = load_settings()
            if 'BASE_FOLDER_ID' not in settings:
                logger.error("my_settings.jsonにBASE_FOLDER_IDが設定されていません")
                sys.exit(1)
            source_folder_id = settings['BASE_FOLDER_ID']
            logger.info(f"コピー元フォルダID: {source_folder_id} (my_settings.jsonから読み込み)")
        
        # コピー元フォルダの妥当性をチェック
        logger.info("コピー元フォルダの妥当性をチェック中...")
        if not validate_folder_id(service, source_folder_id):
            sys.exit(1)
        
        # コピー元フォルダ名を取得
        source_folder_name = get_folder_name(service, source_folder_id)
        if not source_folder_name:
            sys.exit(1)
        
        logger.info(f"コピー元フォルダ名: {source_folder_name}")
        
        if args.dry_run:
            # ドライラン: 処理内容のみ表示
            logger.info("=== ドライラン: 処理内容 ===")
            logger.info(f"コピー元: {source_folder_name} (ID: {source_folder_id})")
            logger.info(f"コピー先: {args.target_folder_id}")
            
            # フォルダ構造を再帰的に表示
            logger.info("=== フォルダ構造 ===")
            display_folder_structure_recursive(service, source_folder_id, source_folder_name, skip_shortcuts=args.skip_shortcuts)
            
            # アイテム数をカウント
            total_folders, total_files, total_shortcuts = count_items_recursive(service, source_folder_id, args.skip_shortcuts)
            total_items = total_folders + total_files + total_shortcuts
            logger.info(f"=== コピー対象統計 ===")
            logger.info(f"総アイテム数: {total_items}")
            logger.info(f"フォルダ数: {total_folders}")
            logger.info(f"ファイル数: {total_files}")
            logger.info(f"ショートカット数: {total_shortcuts}")
            
            logger.info("ドライラン完了")
            
        else:
            # 実際のコピー処理
            logger.info("=== フォルダコピー開始 ===")
            
            # チェックポイントの処理
            if args.resume:
                if checkpoint_manager.load_checkpoint():
                    logger.info("チェックポイントから再開します")
                    # チェックポイントの情報を使用
                    if checkpoint_manager.backup_folder_id:
                        logger.info(f"既存のバックアップフォルダを使用: {checkpoint_manager.backup_folder_name}")
                        backup_folder_id = checkpoint_manager.backup_folder_id
                        backup_folder_name = checkpoint_manager.backup_folder_name
                    else:
                        logger.error("チェックポイントにバックアップフォルダ情報がありません")
                        sys.exit(1)
                else:
                    logger.error("チェックポイントが見つかりません。通常モードで開始します。")
                    args.resume = False
            
            if not args.resume:
                # 新しいバックアップフォルダを作成
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_folder_name = f"{source_folder_name}_backup_{timestamp}"
                
                # チェックポイント情報を初期化
                checkpoint_manager.source_folder_id = source_folder_id
                checkpoint_manager.target_folder_id = args.target_folder_id
                checkpoint_manager.backup_folder_name = backup_folder_name
                checkpoint_manager.start_time = datetime.now()
                checkpoint_manager.processed_folders.clear()
                checkpoint_manager.save_checkpoint()
                
                logger.info(f"バックアップフォルダ名: {backup_folder_name}")
            
            # 総アイテム数を事前にカウント
            logger.info("総アイテム数をカウント中...")
            total_items = count_total_items_recursive(service, source_folder_id, args.skip_shortcuts)
            logger.info(f"総アイテム数: {total_items}")
            
            # 進捗追跡を開始
            progress_tracker.start(total_items)
            
            # フォルダを再帰的にコピー
            if args.resume:
                # 再開モード: 既存のバックアップフォルダを使用
                new_folder_id = backup_folder_id
            else:
                # 新規モード: 新しいバックアップフォルダを作成
                new_folder_id = copy_folder_recursive(
                    service, 
                    source_folder_id, 
                    args.target_folder_id, 
                    backup_folder_name,
                    skip_shortcuts=args.skip_shortcuts
                )
                
                # バックアップフォルダIDをチェックポイントに保存
                if new_folder_id:
                    checkpoint_manager.backup_folder_id = new_folder_id
                    checkpoint_manager.save_checkpoint()
            
            # 進捗追跡を完了
            progress_tracker.finish()
            
            if new_folder_id:
                logger.info("=== フォルダバックアップ完了 ===")
                logger.info(f"バックアップフォルダID: {new_folder_id}")
                logger.info(f"バックアップフォルダ名: {backup_folder_name}")
                
                # 完了時にチェックポイントをクリア
                checkpoint_manager.clear_checkpoint()
            else:
                logger.error("フォルダバックアップに失敗しました")
                sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("ユーザーによって中断されました")
        logger.info("チェックポイントが保存されているため、--resumeオプションで再開できます")
        sys.exit(1)
    except Exception as e:
        logger.error(f"予期しないエラーが発生しました: {e}")
        logger.info("チェックポイントが保存されているため、--resumeオプションで再開できます")
        sys.exit(1)

if __name__ == "__main__":
    main() 