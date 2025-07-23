#!/usr/bin/env python3
"""
backup_folder.pyのテストスクリプト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json
import tempfile
from io import StringIO
from datetime import datetime

# テスト対象のモジュールをインポート
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class TestProgressTracker(unittest.TestCase):
    """ProgressTrackerクラスのテスト"""
    
    def setUp(self):
        """テスト前の準備"""
        from backup_folder import ProgressTracker
        self.progress_tracker = ProgressTracker()
    
    @patch('backup_folder.logger')
    def test_progress_tracker_start(self, mock_logger):
        """ProgressTrackerの開始テスト"""
        self.progress_tracker.start(100)
        
        self.assertEqual(self.progress_tracker.total_items, 100)
        self.assertEqual(self.progress_tracker.processed_items, 0)
        self.assertIsNotNone(self.progress_tracker.start_time)
        self.assertEqual(self.progress_tracker.folder_count, 0)
        self.assertEqual(self.progress_tracker.file_count, 0)
        
        mock_logger.info.assert_any_call("=== 進捗追跡開始 ===")
        mock_logger.info.assert_any_call("総アイテム数: 100")
    
    @patch('backup_folder.logger')
    def test_progress_tracker_update_file(self, mock_logger):
        """ProgressTrackerのファイル更新テスト"""
        self.progress_tracker.start(10)
        
        # ファイルの進捗を更新
        self.progress_tracker.update("file")
        
        self.assertEqual(self.progress_tracker.processed_items, 1)
        self.assertEqual(self.progress_tracker.file_count, 1)
        self.assertEqual(self.progress_tracker.folder_count, 0)
        
        # ログ出力を確認
        mock_logger.info.assert_called()
    
    @patch('backup_folder.logger')
    def test_progress_tracker_update_folder(self, mock_logger):
        """ProgressTrackerのフォルダ更新テスト"""
        self.progress_tracker.start(10)
        
        # フォルダの進捗を更新
        self.progress_tracker.update("folder")
        
        self.assertEqual(self.progress_tracker.processed_items, 1)
        self.assertEqual(self.progress_tracker.folder_count, 1)
        self.assertEqual(self.progress_tracker.file_count, 0)
        
        # ログ出力を確認
        mock_logger.info.assert_called()
    
    @patch('backup_folder.logger')
    def test_progress_tracker_finish(self, mock_logger):
        """ProgressTrackerの完了テスト"""
        self.progress_tracker.start(10)
        self.progress_tracker.update("file")
        self.progress_tracker.update("folder")
        
        self.progress_tracker.finish()
        
        mock_logger.info.assert_any_call("=== 進捗追跡完了 ===")
        mock_logger.info.assert_any_call("総処理アイテム数: 2")
        mock_logger.info.assert_any_call("フォルダ数: 1, ファイル数: 1")

class TestCheckpointManager(unittest.TestCase):
    """CheckpointManagerクラスのテスト"""
    
    def setUp(self):
        """テスト前の準備"""
        from backup_folder import CheckpointManager
        self.checkpoint_file = 'test_checkpoint.json'
        self.checkpoint_manager = CheckpointManager(self.checkpoint_file)
    
    def tearDown(self):
        """テスト後のクリーンアップ"""
        # テスト用チェックポイントファイルを削除
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
        except:
            pass
    
    def test_checkpoint_manager_initialization(self):
        """CheckpointManagerの初期化テスト"""
        self.assertEqual(len(self.checkpoint_manager.processed_folders), 0)
        self.assertIsNone(self.checkpoint_manager.backup_folder_id)
        self.assertIsNone(self.checkpoint_manager.source_folder_id)
        self.assertIsNone(self.checkpoint_manager.target_folder_id)
        self.assertIsNone(self.checkpoint_manager.backup_folder_name)
        self.assertIsNone(self.checkpoint_manager.start_time)
    
    def test_add_processed_folder(self):
        """処理済みフォルダの追加テスト"""
        self.checkpoint_manager.add_processed_folder("テストフォルダ1")
        self.checkpoint_manager.add_processed_folder("テストフォルダ2")
        
        self.assertEqual(len(self.checkpoint_manager.processed_folders), 2)
        self.assertIn("テストフォルダ1", self.checkpoint_manager.processed_folders)
        self.assertIn("テストフォルダ2", self.checkpoint_manager.processed_folders)
    
    def test_is_processed(self):
        """処理済みフォルダのチェックテスト"""
        self.assertFalse(self.checkpoint_manager.is_processed("テストフォルダ"))
        
        self.checkpoint_manager.add_processed_folder("テストフォルダ")
        self.assertTrue(self.checkpoint_manager.is_processed("テストフォルダ"))
    
    def test_save_and_load_checkpoint(self):
        """チェックポイントの保存と読み込みテスト"""
        from backup_folder import CheckpointManager
        
        # テストデータを設定
        self.checkpoint_manager.processed_folders = {"フォルダ1", "フォルダ2"}
        self.checkpoint_manager.backup_folder_id = "test_backup_id"
        self.checkpoint_manager.source_folder_id = "test_source_id"
        self.checkpoint_manager.target_folder_id = "test_target_id"
        self.checkpoint_manager.backup_folder_name = "test_backup_name"
        self.checkpoint_manager.start_time = datetime.now()
        
        # 保存
        self.checkpoint_manager.save_checkpoint()
        
        # 新しいインスタンスで読み込み
        new_checkpoint_manager = CheckpointManager(self.checkpoint_file)
        result = new_checkpoint_manager.load_checkpoint()
        
        self.assertTrue(result)
        self.assertEqual(new_checkpoint_manager.processed_folders, {"フォルダ1", "フォルダ2"})
        self.assertEqual(new_checkpoint_manager.backup_folder_id, "test_backup_id")
        self.assertEqual(new_checkpoint_manager.source_folder_id, "test_source_id")
        self.assertEqual(new_checkpoint_manager.target_folder_id, "test_target_id")
        self.assertEqual(new_checkpoint_manager.backup_folder_name, "test_backup_name")
    
    def test_load_nonexistent_checkpoint(self):
        """存在しないチェックポイントの読み込みテスト"""
        result = self.checkpoint_manager.load_checkpoint()
        self.assertFalse(result)
    
    def test_clear_checkpoint(self):
        """チェックポイントのクリアテスト"""
        # テストファイルを作成
        with open(self.checkpoint_file, 'w') as f:
            f.write('{"test": "data"}')
        
        self.assertTrue(os.path.exists(self.checkpoint_file))
        
        # クリア
        self.checkpoint_manager.clear_checkpoint()
        
        self.assertFalse(os.path.exists(self.checkpoint_file))

class TestBackupFolder(unittest.TestCase):
    
    def setUp(self):
        """テスト前の準備"""
        # テスト用の一時ファイルを作成
        self.temp_secrets = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        self.temp_settings = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        
        # テスト用の認証情報
        test_secrets = {
            "OAUTH2_CLIENT_INFO": {
                "installed": {
                    "client_id": "test_client_id",
                    "client_secret": "test_client_secret",
                    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token"
                }
            }
        }
        
        # テスト用の設定
        test_settings = {
            "SKIP_LATLONG_UPDATE_LIST": [
                ["福岡県", "福岡市"]
            ],
            "BASE_FOLDER_ID": "test_base_folder_id"
        }
        
        # ファイルに書き込み
        json.dump(test_secrets, self.temp_secrets)
        json.dump(test_settings, self.temp_settings)
        self.temp_secrets.close()
        self.temp_settings.close()
    
    def tearDown(self):
        """テスト後のクリーンアップ"""
        # 一時ファイルを削除
        os.unlink(self.temp_secrets.name)
        os.unlink(self.temp_settings.name)
    
    @patch('builtins.open')
    def test_load_secrets_success(self, mock_open):
        """load_secrets関数の成功テスト"""
        # モックの設定
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
            "OAUTH2_CLIENT_INFO": {
                "installed": {
                    "client_id": "test_id",
                    "client_secret": "test_secret"
                }
            }
        })
        
        # モジュールをインポート（モック後に）
        from backup_folder import load_secrets
        result = load_secrets()
        self.assertEqual(result["installed"]["client_id"], "test_id")
    
    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_secrets_file_not_found(self, mock_open):
        """load_secrets関数のファイル未発見テスト"""
        from backup_folder import load_secrets
        with self.assertRaises(SystemExit):
            load_secrets()
    
    @patch('builtins.open')
    def test_load_settings_success(self, mock_open):
        """load_settings関数の成功テスト"""
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
            "BASE_FOLDER_ID": "test_id",
            "SKIP_LATLONG_UPDATE_LIST": []
        })
        
        from backup_folder import load_settings
        result = load_settings()
        self.assertEqual(result["BASE_FOLDER_ID"], "test_id")
    
    @patch('backup_folder.logger')
    def test_list_drive_files(self, mock_logger):
        """list_drive_files関数のテスト"""
        from backup_folder import list_drive_files
        
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # テスト用のファイルデータ
        test_files = [
            {'id': 'file1', 'name': 'test1.txt', 'mimeType': 'text/plain'},
            {'id': 'file2', 'name': 'test2.csv', 'mimeType': 'text/csv'},
            {'id': 'folder1', 'name': 'subfolder', 'mimeType': 'application/vnd.google-apps.folder'}
        ]
        
        mock_response = {
            'files': test_files,
            'nextPageToken': None
        }
        
        mock_files_list.list.return_value.execute.return_value = mock_response
        
        result = list_drive_files(mock_service, 'test_folder_id')
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['name'], 'test1.txt')
    
    @patch('backup_folder.logger')
    def test_list_drive_files_with_shortcuts(self, mock_logger):
        """list_drive_files関数のショートカット含むテスト"""
        from backup_folder import list_drive_files
        
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # テスト用のファイルデータ（ショートカット含む）
        test_files = [
            {'id': 'file1', 'name': 'test1.txt', 'mimeType': 'text/plain'},
            {'id': 'shortcut1', 'name': 'shortcut_to_file', 'mimeType': 'application/vnd.google-apps.shortcut'},
            {'id': 'folder1', 'name': 'subfolder', 'mimeType': 'application/vnd.google-apps.folder'}
        ]
        
        mock_response = {
            'files': test_files,
            'nextPageToken': None
        }
        
        mock_files_list.list.return_value.execute.return_value = mock_response
        
        # ショートカットをスキップしない場合
        result = list_drive_files(mock_service, 'test_folder_id', skip_shortcuts=False)
        self.assertEqual(len(result), 3)
        
        # ショートカットをスキップする場合
        result = list_drive_files(mock_service, 'test_folder_id', skip_shortcuts=True)
        self.assertEqual(len(result), 2)
        # ショートカットが除外されていることを確認
        shortcut_ids = [f['id'] for f in result if f['mimeType'] == 'application/vnd.google-apps.shortcut']
        self.assertEqual(len(shortcut_ids), 0)
    
    @patch('backup_folder.logger')
    def test_count_total_items_recursive(self, mock_logger):
        """count_total_items_recursive関数のテスト"""
        from backup_folder import count_total_items_recursive
        
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # テスト用のファイルデータ（階層構造）
        root_files = [
            {'id': 'file1', 'name': 'test1.txt', 'mimeType': 'text/plain'},
            {'id': 'folder1', 'name': 'subfolder', 'mimeType': 'application/vnd.google-apps.folder'}
        ]
        
        sub_files = [
            {'id': 'file2', 'name': 'test2.txt', 'mimeType': 'text/plain'}
        ]
        
        # モックの設定
        mock_files_list.list.return_value.execute.side_effect = [
            {'files': root_files, 'nextPageToken': None},
            {'files': sub_files, 'nextPageToken': None}
        ]
        
        result = count_total_items_recursive(mock_service, 'test_folder_id')
        
        # ルートフォルダ: 2アイテム + サブフォルダ内: 1アイテム = 3アイテム
        self.assertEqual(result, 3)
    
    @patch('backup_folder.logger')
    def test_count_total_items_recursive_with_shortcuts(self, mock_logger):
        """count_total_items_recursive関数のショートカット含むテスト"""
        from backup_folder import count_total_items_recursive
        
        mock_service = Mock()
        
        # テスト用のファイルデータ（ショートカット含む）
        root_files = [
            {'id': 'file1', 'name': 'test1.txt', 'mimeType': 'text/plain'},
            {'id': 'shortcut1', 'name': 'shortcut_to_file', 'mimeType': 'application/vnd.google-apps.shortcut'},
            {'id': 'folder1', 'name': 'subfolder', 'mimeType': 'application/vnd.google-apps.folder'}
        ]
        sub_files = [
            {'id': 'file2', 'name': 'test2.txt', 'mimeType': 'text/plain'},
            {'id': 'shortcut2', 'name': 'shortcut_to_folder', 'mimeType': 'application/vnd.google-apps.shortcut'}
        ]
        
        def list_drive_files_side_effect(service, folder_id, skip_shortcuts):
            if folder_id == 'test_folder_id':
                return [f for f in root_files if not (skip_shortcuts and f['mimeType'] == 'application/vnd.google-apps.shortcut')]
            elif folder_id == 'folder1':
                return [f for f in sub_files if not (skip_shortcuts and f['mimeType'] == 'application/vnd.google-apps.shortcut')]
            else:
                return []
        
        with patch('backup_folder.list_drive_files', side_effect=list_drive_files_side_effect):
            # ショートカットをスキップしない場合
            result = count_total_items_recursive(mock_service, 'test_folder_id', skip_shortcuts=False)
            # ルートフォルダ: 3アイテム + サブフォルダ内: 2アイテム = 5アイテム
            self.assertEqual(result, 5)
            # ショートカットをスキップする場合
            result = count_total_items_recursive(mock_service, 'test_folder_id', skip_shortcuts=True)
            # ルートフォルダ: 2アイテム + サブフォルダ内: 1アイテム = 3アイテム
            self.assertEqual(result, 3)
    
    @patch('backup_folder.logger')
    def test_copy_file(self, mock_logger):
        """copy_file関数のテスト"""
        from backup_folder import copy_file
        
        mock_service = Mock()
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        # ファイルメタデータのモック
        file_metadata = {'name': 'test.txt', 'mimeType': 'text/plain'}
        copied_file = {'id': 'new_file_id', 'name': 'test.txt'}
        
        mock_files.get.return_value.execute.return_value = file_metadata
        mock_files.copy.return_value.execute.return_value = copied_file
        
        result = copy_file(mock_service, 'old_file_id', 'new_parent_id')
        
        self.assertEqual(result, 'new_file_id')
        mock_files.copy.assert_called_once()
    
    @patch('backup_folder.logger')
    def test_copy_folder_recursive(self, mock_logger):
        """copy_folder_recursive関数のテスト"""
        from backup_folder import copy_folder_recursive
        
        mock_service = Mock()
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        # フォルダ作成のモック
        new_folder = {'id': 'new_folder_id', 'name': 'test_folder'}
        mock_files.create.return_value.execute.return_value = new_folder
        
        # フォルダ内アイテムのモック
        items = [
            {'id': 'file1', 'name': 'test.txt', 'mimeType': 'text/plain'},
            {'id': 'subfolder', 'name': 'sub', 'mimeType': 'application/vnd.google-apps.folder'}
        ]
        
        with patch('backup_folder.list_drive_files', return_value=items):
            with patch('backup_folder.copy_file', return_value='new_file_id'):
                with patch('backup_folder.copy_folder_recursive', return_value='new_subfolder_id'):
                    result = copy_folder_recursive(mock_service, 'source_id', 'target_id', 'test_folder', skip_shortcuts=False)
        
        self.assertEqual(result, 'new_folder_id')
        mock_files.create.assert_called_once()
    
    @patch('backup_folder.logger')
    def test_get_folder_name(self, mock_logger):
        """get_folder_name関数のテスト"""
        from backup_folder import get_folder_name
        
        mock_service = Mock()
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        folder_metadata = {'name': 'test_folder'}
        mock_files.get.return_value.execute.return_value = folder_metadata
        
        result = get_folder_name(mock_service, 'test_folder_id')
        
        self.assertEqual(result, 'test_folder')
    
    @patch('backup_folder.logger')
    def test_validate_folder_id_success(self, mock_logger):
        """validate_folder_id関数の成功テスト"""
        from backup_folder import validate_folder_id
        
        mock_service = Mock()
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        folder_metadata = {'mimeType': 'application/vnd.google-apps.folder'}
        mock_files.get.return_value.execute.return_value = folder_metadata
        
        result = validate_folder_id(mock_service, 'test_folder_id')
        
        self.assertTrue(result)
    
    @patch('backup_folder.logger')
    def test_validate_folder_id_not_folder(self, mock_logger):
        """validate_folder_id関数の非フォルダテスト"""
        from backup_folder import validate_folder_id
        
        mock_service = Mock()
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        file_metadata = {'mimeType': 'text/plain'}
        mock_files.get.return_value.execute.return_value = file_metadata
        
        result = validate_folder_id(mock_service, 'test_file_id')
        
        self.assertFalse(result)
    
    @patch('backup_folder.logger')
    def test_validate_folder_id_error(self, mock_logger):
        """validate_folder_id関数のエラーテスト"""
        from backup_folder import validate_folder_id
        from googleapiclient.errors import HttpError
        
        mock_service = Mock()
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        # HttpErrorを正しく作成
        mock_files.get.return_value.execute.side_effect = HttpError(
            resp=Mock(status=404),
            content=b'Not Found'
        )
        
        result = validate_folder_id(mock_service, 'invalid_id')
        
        self.assertFalse(result)

class TestRetryFunctionality(unittest.TestCase):
    """リトライ機能のテスト"""
    
    @patch('backup_folder.logger')
    def test_retry_on_api_error_success_first_try(self, mock_logger):
        """リトライ機能の初回成功テスト"""
        from backup_folder import retry_on_api_error
        
        def success_func():
            return "success"
        
        result = retry_on_api_error(success_func, max_retries=3)
        self.assertEqual(result, "success")
        mock_logger.warning.assert_not_called()
    
    @patch('backup_folder.logger')
    @patch('backup_folder.time.sleep')
    def test_retry_on_api_error_success_after_retry(self, mock_sleep, mock_logger):
        """リトライ機能のリトライ後成功テスト"""
        from backup_folder import retry_on_api_error
        from googleapiclient.errors import HttpError
        
        call_count = 0
        def failing_then_success():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise HttpError(resp=Mock(status=500), content=b'Server Error')
            return "success"
        
        result = retry_on_api_error(failing_then_success, max_retries=3)
        self.assertEqual(result, "success")
        self.assertEqual(call_count, 3)
        self.assertEqual(mock_logger.warning.call_count, 2)  # 2回のリトライ
    
    @patch('backup_folder.logger')
    @patch('backup_folder.time.sleep')
    def test_retry_on_api_error_max_retries_exceeded(self, mock_sleep, mock_logger):
        """リトライ機能の最大リトライ回数超過テスト"""
        from backup_folder import retry_on_api_error
        from googleapiclient.errors import HttpError
        
        def always_fail():
            raise HttpError(resp=Mock(status=500), content=b'Server Error')
        
        with self.assertRaises(HttpError):
            retry_on_api_error(always_fail, max_retries=3)
        
        self.assertEqual(mock_logger.warning.call_count, 3)  # 3回のリトライ
        mock_logger.error.assert_called_once()  # 最大リトライ回数エラー
    
    @patch('backup_folder.logger')
    def test_retry_on_api_error_non_http_error(self, mock_logger):
        """リトライ機能の非HTTPエラーテスト"""
        from backup_folder import retry_on_api_error
        
        def non_http_error():
            raise ValueError("Non-HTTP error")
        
        with self.assertRaises(ValueError):
            retry_on_api_error(non_http_error, max_retries=3)
        
        # 非HTTPエラーはリトライされない
        mock_logger.warning.assert_not_called()

class TestBackupFolderIntegration(unittest.TestCase):
    """統合テスト"""
    
    @patch('backup_folder.build')
    @patch('backup_folder.get_credentials')
    @patch('backup_folder.load_settings')
    @patch('backup_folder.validate_folder_id')
    @patch('backup_folder.get_folder_name')
    @patch('backup_folder.list_drive_files')
    @patch('backup_folder.count_total_items_recursive')
    @patch('backup_folder.copy_folder_recursive')
    def test_main_dry_run(self, mock_copy, mock_count, mock_list, mock_get_name, mock_validate, 
                         mock_load_settings, mock_get_creds, mock_build):
        """ドライラン機能のテスト"""
        # モックの設定
        mock_get_creds.return_value = Mock()
        mock_build.return_value = Mock()
        mock_load_settings.return_value = {'BASE_FOLDER_ID': 'test_source_id'}
        mock_validate.return_value = True
        mock_get_name.return_value = 'Test Folder'
        mock_list.return_value = [
            {'name': 'test.txt', 'mimeType': 'text/plain'},
            {'name': 'subfolder', 'mimeType': 'application/vnd.google-apps.folder'}
        ]
        mock_count.return_value = 5
        
        # コマンドライン引数をモック
        with patch('sys.argv', ['backup_folder.py', '--dry-run', 'test_target_id']):
            with patch('backup_folder.main') as mock_main:
                import backup_folder
                backup_folder.main()
                mock_main.assert_called_once()

if __name__ == '__main__':
    # テストを実行
    unittest.main(verbosity=2) 