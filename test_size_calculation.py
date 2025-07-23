import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# テスト対象のモジュールをインポート
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check_normalized_csv import format_size, calculate_folder_size, process_size_calculation

class TestSizeCalculation(unittest.TestCase):
    
    def test_format_size(self):
        """format_size関数のテスト"""
        # 基本的なサイズ変換テスト
        self.assertEqual(format_size(0), "0 B")
        self.assertEqual(format_size(1023), "1023.00 B")
        self.assertEqual(format_size(1024), "1.00 KB")
        self.assertEqual(format_size(1024 * 1024), "1.00 MB")
        self.assertEqual(format_size(1024 * 1024 * 1024), "1.00 GB")
        self.assertEqual(format_size(1024 * 1024 * 1024 * 1024), "1.00 TB")
        
        # 小数点を含むサイズテスト
        self.assertEqual(format_size(1536), "1.50 KB")  # 1024 + 512
        self.assertEqual(format_size(1572864), "1.50 MB")  # 1024*1024 + 512*1024
        
        # 大きなサイズテスト
        large_size = 1024 * 1024 * 1024 * 2 + 1024 * 1024 * 512  # 2.5GB
        self.assertEqual(format_size(large_size), "2.50 GB")

    @patch('check_normalized_csv.logger')
    def test_calculate_folder_size(self, mock_logger):
        """calculate_folder_size関数のテスト"""
        # モックサービスを作成
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # テスト用のファイルデータ
        test_files = [
            {
                'id': 'file1',
                'name': 'test1.csv',
                'mimeType': 'text/csv',
                'size': '1024'
            },
            {
                'id': 'file2', 
                'name': 'test2.csv',
                'mimeType': 'text/csv',
                'size': '2048'
            },
            {
                'id': 'folder1',
                'name': 'subfolder',
                'mimeType': 'application/vnd.google-apps.folder',
                'size': '512'  # フォルダのサイズは無視されるべき
            },
            {
                'id': 'file3',
                'name': 'test3.txt',
                'mimeType': 'text/plain',
                'size': '512'
            }
        ]
        
        # モックレスポンスを設定
        mock_response = {
            'files': test_files,
            'nextPageToken': None
        }
        
        mock_files_list.list.return_value.execute.return_value = mock_response
        
        # 関数を実行
        total_size, files = calculate_folder_size(mock_service, 'test_folder_id')
        
        # 結果を検証
        expected_size = 1024 + 2048 + 512  # フォルダ以外のファイルのサイズ合計
        self.assertEqual(total_size, expected_size)
        self.assertEqual(len(files), 3)  # フォルダ以外のファイル数
        
        # API呼び出しが正しく行われたかを確認
        mock_files_list.list.assert_called_once_with(
            q="'test_folder_id' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, size)',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=None
        )

    @patch('check_normalized_csv.logger')
    def test_calculate_folder_size_with_pagination(self, mock_logger):
        """ページネーションを含むcalculate_folder_size関数のテスト"""
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # 1ページ目のデータ
        first_page_files = [
            {'id': 'file1', 'name': 'test1.csv', 'mimeType': 'text/csv', 'size': '1024'},
            {'id': 'file2', 'name': 'test2.csv', 'mimeType': 'text/csv', 'size': '2048'}
        ]
        
        # 2ページ目のデータ
        second_page_files = [
            {'id': 'file3', 'name': 'test3.csv', 'mimeType': 'text/csv', 'size': '3072'}
        ]
        
        # ページネーション付きのレスポンスを設定
        mock_files_list.list.return_value.execute.side_effect = [
            {'files': first_page_files, 'nextPageToken': 'token123'},
            {'files': second_page_files, 'nextPageToken': None}
        ]
        
        # 関数を実行
        total_size, files = calculate_folder_size(mock_service, 'test_folder_id')
        
        # 結果を検証
        expected_size = 1024 + 2048 + 3072
        self.assertEqual(total_size, expected_size)
        self.assertEqual(len(files), 3)
        
        # API呼び出しが2回行われたかを確認
        self.assertEqual(mock_files_list.list.return_value.execute.call_count, 2)

    @patch('check_normalized_csv.logger')
    def test_calculate_folder_size_empty_folder(self, mock_logger):
        """空のフォルダのテスト"""
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # 空のフォルダのレスポンス
        mock_response = {
            'files': [],
            'nextPageToken': None
        }
        
        mock_files_list.list.return_value.execute.return_value = mock_response
        
        # 関数を実行
        total_size, files = calculate_folder_size(mock_service, 'empty_folder_id')
        
        # 結果を検証
        self.assertEqual(total_size, 0)
        self.assertEqual(len(files), 0)

    @patch('check_normalized_csv.logger')
    def test_calculate_folder_size_api_error(self, mock_logger):
        """APIエラーのテスト"""
        mock_service = Mock()
        mock_files_list = Mock()
        mock_service.files.return_value = mock_files_list
        
        # APIエラーをシミュレート
        mock_files_list.list.return_value.execute.side_effect = Exception("API Error")
        
        # 関数を実行してエラーが発生することを確認
        with self.assertRaises(Exception):
            calculate_folder_size(mock_service, 'error_folder_id')

    @patch('check_normalized_csv.logger')
    @patch('check_normalized_csv.calculate_folder_size')
    def test_process_size_calculation(self, mock_calculate_size, mock_logger):
        """process_size_calculation関数のテスト"""
        # モックサービスを作成
        mock_drive_service = Mock()
        
        # テスト用のターゲットデータ
        test_targets = [
            (2, '東京都', '新宿区', 'folder1'),
            (3, '東京都', '渋谷区', 'folder2'),
            (4, '大阪府', '大阪市', 'folder3')
        ]
        
        # calculate_folder_sizeの戻り値を設定
        mock_calculate_size.side_effect = [
            (1024 * 1024 * 15, [{'name': 'file1.csv'}] * 45),  # 15MB, 45ファイル
            (1024 * 1024 * 12, [{'name': 'file2.csv'}] * 38),  # 12MB, 38ファイル
            (1024 * 1024 * 8, [{'name': 'file3.csv'}] * 25)    # 8MB, 25ファイル
        ]
        
        # 関数を実行
        total_size = process_size_calculation(test_targets, mock_drive_service)
        
        # 結果を検証
        expected_total = (15 + 12 + 8) * 1024 * 1024  # 35MB
        self.assertEqual(total_size, expected_total)
        
        # calculate_folder_sizeが正しく呼ばれたかを確認
        self.assertEqual(mock_calculate_size.call_count, 3)
        
        # ログ出力が正しく行われたかを確認
        mock_logger.info.assert_any_call("=== フォルダサイズ計算開始 ===")
        mock_logger.info.assert_any_call("=== サイズ計算結果 ===")
        mock_logger.info.assert_any_call("=== サイズ上位10件 ===")

    @patch('check_normalized_csv.logger')
    @patch('check_normalized_csv.calculate_folder_size')
    def test_process_size_calculation_with_error(self, mock_calculate_size, mock_logger):
        """エラーが発生する場合のprocess_size_calculation関数のテスト"""
        mock_drive_service = Mock()
        
        test_targets = [
            (2, '東京都', '新宿区', 'folder1'),
            (3, '東京都', '渋谷区', 'folder2')
        ]
        
        # 1つ目のフォルダは成功、2つ目はエラー
        mock_calculate_size.side_effect = [
            (1024 * 1024 * 10, [{'name': 'file1.csv'}] * 20),  # 成功
            Exception("API Error")  # エラー
        ]
        
        # 関数を実行
        total_size = process_size_calculation(test_targets, mock_drive_service)
        
        # エラーが発生しても処理は続行される
        expected_total = 10 * 1024 * 1024  # 成功したフォルダのサイズのみ
        self.assertEqual(total_size, expected_total)
        
        # エラーログが出力されたかを確認
        mock_logger.error.assert_called_once()

    @patch('check_normalized_csv.logger')
    @patch('check_normalized_csv.calculate_folder_size')
    def test_process_size_calculation_empty_targets(self, mock_calculate_size, mock_logger):
        """空のターゲットリストのテスト"""
        mock_drive_service = Mock()
        
        # 空のターゲットリスト
        test_targets = []
        
        # 関数を実行
        total_size = process_size_calculation(test_targets, mock_drive_service)
        
        # 結果を検証
        self.assertEqual(total_size, 0)
        
        # calculate_folder_sizeが呼ばれていないことを確認
        mock_calculate_size.assert_not_called()

if __name__ == '__main__':
    # テストを実行
    unittest.main(verbosity=2) 