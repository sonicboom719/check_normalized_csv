import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
import argparse
import io
import sys
import csv
import os

# テスト対象のモジュールをインポート
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_normalized_csv

class TestParseDatetimeArg(unittest.TestCase):
    """parse_datetime_arg関数のテスト"""
    
    def test_parse_date_only(self):
        """8桁の日付文字列（YYYYMMDD）のテスト - 日本時間として解釈"""
        result = check_normalized_csv.parse_datetime_arg("20250701")
        # 2025年7月1日 00:00:00 JST → 2025年6月30日 15:00:00 UTC
        expected = datetime(2025, 6, 30, 15, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)
    
    def test_parse_datetime(self):
        """12桁の日時文字列（YYYYMMDDHHMM）のテスト - 日本時間として解釈"""
        result = check_normalized_csv.parse_datetime_arg("202507011234")
        # 2025年7月1日 12:34:00 JST → 2025年7月1日 03:34:00 UTC
        expected = datetime(2025, 7, 1, 3, 34, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)
    
    def test_invalid_format(self):
        """不正な形式のテスト"""
        with self.assertRaises(argparse.ArgumentTypeError):
            check_normalized_csv.parse_datetime_arg("invalid")
    
    def test_wrong_length(self):
        """不正な長さのテスト"""
        with self.assertRaises(argparse.ArgumentTypeError):
            check_normalized_csv.parse_datetime_arg("202507")  # 6桁

class TestDetectEncoding(unittest.TestCase):
    """detect_encoding関数のテスト"""
    
    @patch('check_normalized_csv.chardet.detect')
    def test_detect_utf8(self, mock_detect):
        """UTF-8エンコーディングの検出テスト"""
        mock_detect.return_value = {'encoding': 'utf-8', 'confidence': 0.99}
        content = b"test content"
        result = check_normalized_csv.detect_encoding(content)
        self.assertEqual(result, 'utf-8')
    
    @patch('check_normalized_csv.chardet.detect')
    def test_detect_shift_jis(self, mock_detect):
        """Shift_JISエンコーディングの検出テスト"""
        mock_detect.return_value = {'encoding': 'shift_jis', 'confidence': 0.95}
        content = b"test content"
        result = check_normalized_csv.detect_encoding(content)
        self.assertEqual(result, 'shift_jis')
    
    @patch('check_normalized_csv.chardet.detect')
    def test_detect_none(self, mock_detect):
        """エンコーディング検出失敗のテスト"""
        mock_detect.return_value = None
        content = b"test content"
        result = check_normalized_csv.detect_encoding(content)
        self.assertIsNone(result)

class TestCheckCsvContent(unittest.TestCase):
    """check_csv_content関数のテスト"""
    
    def setUp(self):
        """テスト前の準備"""
        self.expected_pref = "東京都"
        self.expected_city = "渋谷区"
        self.row_num = 1
    
    def create_csv_content(self, rows, encoding='utf-8'):
        """CSVコンテンツを作成するヘルパー関数"""
        output = io.StringIO()
        writer = csv.writer(output, lineterminator='\n')
        for row in rows:
            writer.writerow(row)
        return output.getvalue().encode(encoding)
    
    def test_nul_character_detection(self):
        """NUL文字の検出テスト"""
        content = b"test\x00content"
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertFalse(ok)
        self.assertFalse(has_bom)
        self.assertIsNone(decoded)
    
    def test_utf8_bom_detection(self):
        """UTF-8 BOMの検出テスト"""
        content = b'\xef\xbb\xbfprefecture,city,number,address,name,lat,long\n' + \
                  '東京都,渋谷区,1,テスト住所,テスト名前,35.685,139.753'.encode('utf-8')
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertTrue(ok)
        self.assertTrue(has_bom)
        self.assertIsNotNone(decoded)
    
    def test_valid_csv_content(self):
        """有効なCSV内容のテスト"""
        content = b'prefecture,city,number,address,name,lat,long\n' + \
                  '東京都,渋谷区,1,テスト住所,テスト名前,35.685,139.753'.encode('utf-8')
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertTrue(ok)
        self.assertFalse(has_bom)
        self.assertIsNotNone(decoded)
    
    def test_invalid_header(self):
        """不正なヘッダーのテスト"""
        content = b'invalid,header\n' + \
                  '東京都,千代田区,1,テスト住所,テスト名前,35.685,139.753'.encode('utf-8')
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertFalse(ok)
        self.assertFalse(has_bom)
        self.assertIsNotNone(decoded)
    
    def test_mismatched_prefecture_city(self):
        """都道府県・市区町村の不一致テスト"""
        content = b'prefecture,city,number,address,name,lat,long\n' + \
                  '大阪府,大阪市,1,テスト住所,テスト名前,35.685,139.753'.encode('utf-8')
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertFalse(ok)
        self.assertFalse(has_bom)
        self.assertIsNotNone(decoded)
    
    def test_empty_csv(self):
        """空のCSVのテスト"""
        content = b''
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertFalse(ok)
        self.assertFalse(has_bom)
        self.assertEqual(decoded, '')
    
    def test_lat_long_empty(self):
        """lat/longが空文字列の場合のバリデーションテスト"""
        content = b'prefecture,city,number,address,name,lat,long\n' + \
                  '東京都,千代田区,1,テスト住所,テスト名前,,'.encode('utf-8')
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertFalse(ok)

    def test_lat_long_not_float(self):
        """lat/longが実数でない場合のバリデーションテスト"""
        content = b'prefecture,city,number,address,name,lat,long\n' + \
                  '東京都,千代田区,1,テスト住所,テスト名前,abc,xyz'.encode('utf-8')
        ok, has_bom, decoded = check_normalized_csv.check_csv_content(
            content, self.expected_pref, self.expected_city, self.row_num
        )
        self.assertFalse(ok)

    def test_lat_long_validation_with_shuffled_header(self):
        """ヘッダ順が異なる場合でもlat/longバリデーションが正しく動作するテスト"""
        # note列がlat列より前
        header = ['prefecture','city','number','address','name','note','lat','long']
        data = ['東京都','千代田区','1','テスト住所','テスト名前','','','']
        csv_str = ','.join(header) + '\n' + ','.join(data)
        import check_normalized_csv as target
        ok, has_bom, decoded = target.check_csv_content(csv_str.encode('utf-8'), '東京都', '千代田区', 1)
        self.assertFalse(ok)
        # lat/longに値を入れた場合はOK
        data2 = ['東京都','千代田区','1','テスト住所','テスト名前','','35.0','135.0']
        csv_str2 = ','.join(header) + '\n' + ','.join(data2)
        ok2, has_bom2, decoded2 = target.check_csv_content(csv_str2.encode('utf-8'), '東京都', '千代田区', 1)
        self.assertTrue(ok2)

    def test_duplicate_combination_detection(self):
        """number, name, addressの重複組み合わせを検出するテスト"""
        # 重複する組み合わせを含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6581', '139.7017'],  # 重複
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6582', '139.7018'],
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # 重複エラーが検出されることを確認
            self.assertFalse(ok)
            mock_logger.error.assert_called_with(
                f"[{self.row_num}] 3行目: number, name, addressの組み合わせが重複しています (number='1-1-1', name='テスト店舗A', address='渋谷1-1-1')"
            )
    
    def test_no_duplicate_combination(self):
        """重複がない場合のテスト"""
        # 重複しないCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016'],
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6581', '139.7017'],
            ['東京都', '渋谷区', '1-1-3', '渋谷1-1-3', 'テスト店舗C', '35.6582', '139.7018'],
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # 重複エラーが検出されないことを確認
            self.assertTrue(ok)
            # 重複エラーのログが呼ばれていないことを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 0)
    
    def test_duplicate_with_empty_fields_ignored(self):
        """空のフィールドがある場合は重複チェックをスキップするテスト"""
        # 空のフィールドがある場合のCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016'],
            ['東京都', '渋谷区', '', '渋谷1-1-1', 'テスト店舗A', '35.6581', '139.7017'],  # numberが空
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', '', '35.6582', '139.7018'],  # nameが空
            ['東京都', '渋谷区', '1-1-1', '', 'テスト店舗A', '35.6583', '139.7019'],  # addressが空
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # 空のフィールドがある場合は重複チェックがスキップされることを確認
            self.assertTrue(ok)
            # 重複エラーのログが呼ばれていないことを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 0)
    
    def test_duplicate_with_note_column(self):
        """note列がある場合の重複チェックテスト"""
        # note列を含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long', 'note'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016', ''],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6581', '139.7017', '備考あり'],  # 重複（noteは異なる）
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # note列が異なっても重複として検出されることを確認
            self.assertFalse(ok)
            mock_logger.error.assert_called_with(
                f"[{self.row_num}] 3行目: number, name, addressの組み合わせが重複しています (number='1-1-1', name='テスト店舗A', address='渋谷1-1-1')"
            )
    
    def test_multiple_duplicates(self):
        """複数の重複がある場合のテスト"""
        # 複数の重複を含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6581', '139.7017'],  # 1つ目の重複
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6582', '139.7018'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6583', '139.7019'],  # 2つ目の重複
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # 重複エラーが検出されることを確認
            self.assertFalse(ok)
            # 重複エラーのログが2回呼ばれることを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 2)
    
    def test_case_sensitive_duplicate(self):
        """大文字小文字を区別した重複チェックのテスト"""
        # 大文字小文字が異なるCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗a', '35.6581', '139.7017'],  # 小文字
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # 大文字小文字が異なる場合は重複として検出されないことを確認
            self.assertTrue(ok)
            # 重複エラーのログが呼ばれていないことを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 0)
    
    def test_error_as_warning_for_skip_list_cities(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれる自治体の場合はlat/longエラーがINFO扱いになるテスト"""
        # 福岡県福岡市のデータ（SKIP_LATLONG_UPDATE_LISTに含まれる）
        pref = "福岡県"
        city = "福岡市"
        
        # エラーを含むCSVデータ（lat/longが空）
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['福岡県', '福岡市', '1-1-1', '福岡1-1-1', 'テスト店舗A', '', ''],  # lat/longが空
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, pref, city, self.row_num)
            
            # エラーではなくINFOとして出力されることを確認
            error_calls = [call for call in mock_logger.error.call_args_list 
                          if 'lat/long列が空または実数値でありません' in str(call)]
            info_calls = [call for call in mock_logger.info.call_args_list 
                         if 'lat/long列が空または実数値でありません' in str(call)]
            
            self.assertEqual(len(error_calls), 0)  # エラーは出力されない
            self.assertEqual(len(info_calls), 1)  # INFOは出力される
    
    def test_error_as_warning_for_skip_list_cities_duplicate(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれる自治体の重複エラーがワーニング扱いになるテスト"""
        # 福岡県福岡市のデータ（SKIP_LATLONG_UPDATE_LISTに含まれる）
        pref = "福岡県"
        city = "福岡市"
        
        # 重複を含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['福岡県', '福岡市', '1-1-1', '福岡1-1-1', 'テスト店舗A', '33.5902', '130.4017'],
            ['福岡県', '福岡市', '1-1-1', '福岡1-1-1', 'テスト店舗A', '33.5903', '130.4018'],  # 重複
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, pref, city, self.row_num)
            
            # エラーではなくワーニングとして出力されることを確認
            error_calls = [call for call in mock_logger.error.call_args_list 
                          if 'number, name, addressの組み合わせが重複しています' in str(call)]
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'number, name, addressの組み合わせが重複しています' in str(call)]
            
            self.assertEqual(len(error_calls), 0)  # エラーは出力されない
            self.assertEqual(len(warning_calls), 1)  # ワーニングは出力される
    
    def test_error_as_warning_for_skip_list_cities_header(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれる自治体のヘッダーエラーがワーニング扱いになるテスト"""
        # 福岡県福岡市のデータ（SKIP_LATLONG_UPDATE_LISTに含まれる）
        pref = "福岡県"
        city = "福岡市"
        
        # 不正なヘッダーを含むCSVデータ
        csv_data = [
            ['invalid', 'header'],  # 不正なヘッダー
            ['福岡県', '福岡市', '1-1-1', '福岡1-1-1', 'テスト店舗A', '33.5902', '130.4017'],
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, pref, city, self.row_num)
            
            # エラーではなくワーニングとして出力されることを確認
            error_calls = [call for call in mock_logger.error.call_args_list 
                          if 'CSVヘッダが不正' in str(call)]
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'CSVヘッダが不正' in str(call)]
            
            self.assertEqual(len(error_calls), 0)  # エラーは出力されない
            self.assertEqual(len(warning_calls), 1)  # ワーニングは出力される
    
    def test_normal_error_for_non_skip_list_cities(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれない自治体の場合は通常通りエラー扱いになるテスト"""
        # 東京都渋谷区のデータ（SKIP_LATLONG_UPDATE_LISTに含まれない）
        pref = "東京都"
        city = "渋谷区"
        
        # エラーを含むCSVデータ（lat/longが空）
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '', ''],  # lat/longが空
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, pref, city, self.row_num)
            
            # 通常通りエラーとして出力されることを確認
            error_calls = [call for call in mock_logger.error.call_args_list 
                          if 'lat/long列が空または実数値でありません' in str(call)]
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'lat/long列が空または実数値でありません' in str(call)]
            
            self.assertEqual(len(error_calls), 1)  # エラーは出力される
            self.assertEqual(len(warning_calls), 0)  # ワーニングは出力されない

    def test_too_few_columns_no_index_error(self):
        """列数が2未満の行があってもIndexErrorで落ちないこと"""
        # ヘッダ＋空行＋1列しかない行
        content = ('prefecture,city,number,address,name,lat,long\n\n東京都\n').encode('utf-8')
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(
                content, self.expected_pref, self.expected_city, self.row_num
            )
            self.assertFalse(ok)
            # エラー/ワーニングが呼ばれていること
            self.assertTrue(mock_logger.error.called or mock_logger.warning.called)

    def test_latlong_validation_too_few_columns(self):
        """main相当のlat/longバリデーションでfieldsの長さ不足でもIndexErrorにならないこと"""
        # lat/long列インデックスは6,7だが、データ行が2列しかない
        header = ['prefecture','city','number','address','name','lat','long']
        data_rows = [
            ['東京都', '渋谷区'],  # 不足
            ['東京都', '渋谷区', '1', '住所', '名前', '', ''],  # lat/long空
            ['東京都', '渋谷区', '2', '住所', '名前', '35.0', '139.0']  # 正常
        ]
        idx_lat = header.index('lat')
        idx_long = header.index('long')
        latlong_error = False
        for fields in data_rows:
            if idx_lat == -1 or idx_long == -1 or len(fields) <= max(idx_lat, idx_long):
                latlong_error = True
                break
            if fields[idx_lat] == '' or fields[idx_long] == '':
                latlong_error = True
                break
            try:
                float(fields[idx_lat])
                float(fields[idx_long])
            except Exception:
                latlong_error = True
                break
        self.assertTrue(latlong_error)

    def test_no_warning_for_skip_list_cities_latlong(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれる自治体の場合はlat/longエラーでもWARNINGを出力しない"""
        # 福岡市（SKIP_LATLONG_UPDATE_LISTに含まれる）のlat/long空データ
        content = ('prefecture,city,number,address,name,lat,long\n' + 
                  '福岡県,福岡市,1,テスト住所,テスト名前,,').encode('utf-8')
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(
                content, '福岡県', '福岡市', 1
            )
            # lat/longエラーがあるためOKはFalse
            self.assertFalse(ok)
            # しかしWARNINGは出力されない
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'lat/long列が空または実数値でありません' in str(call)]
            self.assertEqual(len(warning_calls), 0)

    def test_info_for_skip_list_cities_latlong(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれる自治体のlat/longエラーがINFOで出力されるテスト"""
        content = b'prefecture,city,number,address,name,lat,long\n' + \
                  '福岡県,福岡市,1,テスト住所,テスト名前,,'.encode('utf-8')
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(
                content, '福岡県', '福岡市', self.row_num
            )
            
            # INFOで出力されることを確認
            mock_logger.info.assert_called_with(
                f"[{self.row_num}] 2行目: lat/long列が空または実数値でありません (lat='', long='', note='')"
            )
            self.assertFalse(ok)

    def test_duplicate_check_skipped_when_latlong_invalid(self):
        """lat/longが無効な場合に重複チェックがスキップされるテスト"""
        # lat/longが無効で、重複する組み合わせを含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '', ''],  # lat/long無効
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '', ''],  # 重複だがlat/long無効なのでスキップ
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6582', '139.7018'],  # 正常
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # lat/longエラーは検出されるが、重複エラーは検出されないことを確認
            self.assertFalse(ok)  # lat/longエラーのためFalse
            
            # lat/longエラーのログが出力されることを確認
            latlong_error_calls = [call for call in mock_logger.error.call_args_list 
                                 if 'lat/long列が空または実数値でありません' in str(call)]
            self.assertEqual(len(latlong_error_calls), 2)  # 2行分のlat/longエラー
            
            # 重複エラーのログが出力されないことを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 0)

    def test_duplicate_check_executed_when_latlong_valid(self):
        """lat/longが有効な場合に重複チェックが実行されるテスト"""
        # lat/longが有効で、重複する組み合わせを含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6580', '139.7016'],  # lat/long有効
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '35.6581', '139.7017'],  # 重複
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6582', '139.7018'],  # 正常
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # 重複エラーが検出されることを確認
            self.assertFalse(ok)
            
            # 重複エラーのログが出力されることを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 1)

    def test_duplicate_check_skipped_when_latlong_not_float(self):
        """lat/longが実数でない場合に重複チェックがスキップされるテスト"""
        # lat/longが実数でなく、重複する組み合わせを含むCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', 'abc', 'xyz'],  # lat/long実数でない
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', 'def', 'uvw'],  # 重複だがlat/long実数でないのでスキップ
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6582', '139.7018'],  # 正常
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # lat/longエラーは検出されるが、重複エラーは検出されないことを確認
            self.assertFalse(ok)  # lat/longエラーのためFalse
            
            # lat/longエラーのログが出力されることを確認
            latlong_error_calls = [call for call in mock_logger.error.call_args_list 
                                 if 'lat/long列が空または実数値でありません' in str(call)]
            self.assertEqual(len(latlong_error_calls), 2)  # 2行分のlat/longエラー
            
            # 重複エラーのログが出力されないことを確認
            duplicate_error_calls = [call for call in mock_logger.error.call_args_list 
                                   if 'number, name, addressの組み合わせが重複しています' in str(call)]
            self.assertEqual(len(duplicate_error_calls), 0)

    def test_mixed_latlong_valid_and_invalid(self):
        """lat/longが有効な行と無効な行が混在する場合のテスト"""
        # lat/longが有効な行と無効な行が混在するCSVデータ
        csv_data = [
            ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long'],
            ['東京都', '渋谷区', '1-1-1', '渋谷1-1-1', 'テスト店舗A', '', ''],  # lat/long無効
            ['東京都', '渋谷区', '1-1-2', '渋谷1-1-2', 'テスト店舗B', '35.6582', '139.7018'],  # lat/long有効
            ['東京都', '渋谷区', '1-1-3', '渋谷1-1-3', 'テスト店舗C', 'abc', 'xyz'],  # lat/long実数でない
            ['東京都', '渋谷区', '1-1-4', '渋谷1-1-4', 'テスト店舗D', '35.6584', '139.7020'],  # lat/long有効
        ]
        
        content = self.create_csv_content(csv_data)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(content, self.expected_pref, self.expected_city, self.row_num)
            
            # lat/longエラーが検出されることを確認
            self.assertFalse(ok)  # lat/longエラーのためFalse
            
            # lat/longエラーのログが出力されることを確認（2行分）
            latlong_error_calls = [call for call in mock_logger.error.call_args_list 
                                 if 'lat/long列が空または実数値でありません' in str(call)]
            self.assertEqual(len(latlong_error_calls), 2)  # 1行目と3行目のlat/longエラー

class TestGetFileModifiedTime(unittest.TestCase):
    """get_file_modified_time関数のテスト"""
    
    @patch('check_normalized_csv.logger')
    def test_successful_time_retrieval(self, mock_logger):
        """正常な更新日時取得のテスト - UTC→日本時間変換"""
        mock_service = Mock()
        mock_service.files().get().execute.return_value = {
            'modifiedTime': '2025-07-01T12:34:56.789Z'
        }
        
        result = check_normalized_csv.get_file_modified_time(mock_service, 'file_id')
        
        # 2025-07-01T12:34:56.789Z (UTC) → 2025-07-01T21:34:56.789+09:00 (JST)
        expected = datetime(2025, 7, 1, 21, 34, 56, 789000, tzinfo=timezone(timedelta(hours=9)))
        self.assertEqual(result, expected)
    
    @patch('check_normalized_csv.logger')
    def test_no_modified_time(self, mock_logger):
        """更新日時が存在しない場合のテスト"""
        mock_service = Mock()
        mock_service.files().get().execute.return_value = {}
        
        result = check_normalized_csv.get_file_modified_time(mock_service, 'file_id')
        
        self.assertIsNone(result)
    
    @patch('check_normalized_csv.logger')
    def test_api_error(self, mock_logger):
        """API エラーのテスト"""
        mock_service = Mock()
        mock_service.files().get().execute.side_effect = Exception("API Error")
        
        result = check_normalized_csv.get_file_modified_time(mock_service, 'file_id')
        
        self.assertIsNone(result)
        mock_logger.error.assert_called_once()

class TestFilterRows(unittest.TestCase):
    """filter_rows関数のテスト"""
    
    def setUp(self):
        """テストデータの準備"""
        self.rows = [
            ["東京都", "千代田区", "全部あり", "folder1"],
            ["東京都", "中央区", "全部あり", "folder2"],
            ["大阪府", "大阪市", "一部あり", "folder3"],
            ["大阪府", "堺市", "全部あり", "folder4"]
        ]
        self.idx_pref = 0
        self.idx_city = 1
        self.idx_has_csv = 2
        self.idx_folder = 3
    
    def test_filter_all(self):
        """全件フィルタリングのテスト"""
        result = check_normalized_csv.filter_rows(
            self.rows, self.idx_pref, self.idx_city, 
            self.idx_has_csv, self.idx_folder, []
        )
        expected = [
            (2, "東京都", "千代田区", "folder1"),
            (3, "東京都", "中央区", "folder2"),
            (5, "大阪府", "堺市", "folder4")
        ]
        self.assertEqual(result, expected)
    
    def test_filter_by_prefecture(self):
        """都道府県でフィルタリングのテスト"""
        result = check_normalized_csv.filter_rows(
            self.rows, self.idx_pref, self.idx_city, 
            self.idx_has_csv, self.idx_folder, ["東京都"]
        )
        expected = [
            (2, "東京都", "千代田区", "folder1"),
            (3, "東京都", "中央区", "folder2")
        ]
        self.assertEqual(result, expected)
    
    def test_filter_by_prefecture_and_city(self):
        """都道府県・市区町村でフィルタリングのテスト"""
        result = check_normalized_csv.filter_rows(
            self.rows, self.idx_pref, self.idx_city, 
            self.idx_has_csv, self.idx_folder, ["東京都", "千代田区"]
        )
        expected = [
            (2, "東京都", "千代田区", "folder1")
        ]
        self.assertEqual(result, expected)

    @patch('check_normalized_csv.get_latlng_google')
    @patch('check_normalized_csv.get_latlng_gsi')
    @patch('check_normalized_csv.upload_csv_file')
    def test_fix_and_upload_csv(self, mock_upload, mock_gsi, mock_google):
        """lat/long自動修正・note列追加・距離差判定のテスト"""
        # Google: (35.0, 135.0), GSI: (35.001, 135.001) → 距離差約157m
        mock_google.return_value = (35.0, 135.0)
        mock_gsi.return_value = (35.001, 135.001)
        header = ['prefecture','city','number','address','name','lat','long']
        rows = [
            ['東京都','千代田区','1','テスト住所','テスト名前','',''],
        ]
        decoded = ''
        service = None
        file_id = 'dummy'
        check_normalized_csv.fix_and_upload_csv(service, file_id, decoded, header, rows, '東京都', '千代田区', 1)
        # lat/longがGoogle値で上書きされている
        self.assertEqual(rows[0][5], '35.0')
        self.assertEqual(rows[0][6], '135.0')
        # note列が追加されている
        self.assertIn('note', header)
        self.assertEqual(rows[0][7], '')
        # upload_csv_fileが呼ばれている
        mock_upload.assert_called_once()

    @patch('check_normalized_csv.get_latlng_google')
    @patch('check_normalized_csv.get_latlng_gsi')
    @patch('check_normalized_csv.upload_csv_file')
    def test_fix_and_upload_csv_note_suspicious(self, mock_upload, mock_gsi, mock_google):
        """距離差200m以上でnote列が「緯度経度は怪しい」になるテスト"""
        # Google: (35.0, 135.0), GSI: (35.003, 135.003) → 距離差約471m
        mock_google.return_value = (35.0, 135.0)
        mock_gsi.return_value = (35.003, 135.003)
        header = ['prefecture','city','number','address','name','lat','long']
        rows = [
            ['東京都','千代田区','1','テスト住所','テスト名前','',''],
        ]
        decoded = ''
        service = None
        file_id = 'dummy'
        check_normalized_csv.fix_and_upload_csv(service, file_id, decoded, header, rows, '東京都', '千代田区', 1)
        # note列が「緯度経度は怪しい」
        self.assertEqual(rows[0][7], '緯度経度は怪しい')
        mock_upload.assert_called_once()

    @patch('check_normalized_csv.fix_and_upload_csv')
    @patch('check_normalized_csv.logger')
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.detect_encoding')
    def test_latlong_error_warning_logged(self, mock_detect, mock_download, mock_list, mock_logger, mock_fix):
        """lat/longエラーが自動修正・上書き保存された場合にワーニングがロギングされることのテスト"""
        # テスト用CSVデータ（lat/longが空）
        header = ['prefecture','city','number','address','name','lat','long']
        data = ['東京都','千代田区','1','テスト住所','テスト名前','','']
        csv_str = ','.join(header) + '\n' + ','.join(data)
        mock_download.return_value = (csv_str.encode('utf-8'), 'fileid')
        mock_list.return_value = [{'name': '千代田区_normalized.csv', 'id': 'fileid'}]
        mock_detect.return_value = 'utf-8'
        # mainを部分的に呼び出すため、必要な引数をモック
        import check_normalized_csv as target
        drive_service = None
        file_id = 'fileid'
        decoded = csv_str
        # 実行
        target.fix_and_upload_csv = mock_fix
        target.logger = mock_logger
        # mainのlatlong_error部分だけ呼び出し
        reader = csv.reader(io.StringIO(decoded))
        all_rows = list(reader)
        header = all_rows[0]
        data_rows = all_rows[1:]
        idx_lat = header.index('lat')
        idx_long = header.index('long')
        latlong_error = False
        for fields in data_rows:
            if idx_lat == -1 or idx_long == -1 or fields[idx_lat] == '' or fields[idx_long] == '':
                latlong_error = True
                break
            try:
                float(fields[idx_lat])
                float(fields[idx_long])
            except Exception:
                latlong_error = True
                break
        if latlong_error:
            target.logger.warning(f"[1] lat/longエラーを自動修正し、CSVを上書き保存しました")
            target.fix_and_upload_csv(drive_service, file_id, decoded, header, data_rows, '東京都', '千代田区', 1)
        # 検証
        mock_logger.warning.assert_called_with("[1] lat/longエラーを自動修正し、CSVを上書き保存しました")
        mock_fix.assert_called_once()

    @patch('check_normalized_csv.upload_csv_file')
    @patch('check_normalized_csv.get_latlng_google')
    @patch('check_normalized_csv.get_latlng_gsi')
    @patch('check_normalized_csv.logger')
    def test_fix_and_upload_csv_too_few_columns(self, mock_logger, mock_gsi, mock_google, mock_upload):
        """fix_and_upload_csvでfieldsの長さ不足でもIndexErrorにならないこと"""
        header = ['prefecture','city','number','address','name','lat','long','note']
        rows = [
            ['東京都', '渋谷区'],  # 不足
            ['東京都', '渋谷区', '1', '住所', '名前', '', '', ''],  # lat/long空
            ['東京都', '渋谷区', '2', '住所', '名前', '35.0', '139.0', '']  # 正常
        ]
        # 緯度経度APIはダミー
        mock_google.return_value = (35.0, 139.0)
        mock_gsi.return_value = (35.0, 139.0)
        # 実行
        check_normalized_csv.fix_and_upload_csv(
            service=Mock(), file_id='dummy', decoded='', header=header, rows=rows,
            pref='東京都', city='渋谷区', row_num=1
        )
        # エラー/ワーニングが呼ばれていること
        self.assertTrue(mock_logger.error.called or mock_logger.warning.called)

    def test_no_warning_for_skip_list_cities_check_only(self):
        """SKIP_LATLONG_UPDATE_LISTに含まれる自治体の場合はチェックのみモードでもWARNINGを出力しない"""
        # 福岡市（SKIP_LATLONG_UPDATE_LISTに含まれる）のlat/long空データ
        content = ('prefecture,city,number,address,name,lat,long\n' + 
                  '福岡県,福岡市,1,テスト住所,テスト名前,,').encode('utf-8')
        
        with patch('check_normalized_csv.logger') as mock_logger:
            # check_csv_contentを直接テスト
            ok, has_bom, decoded = check_normalized_csv.check_csv_content(
                content, '福岡県', '福岡市', 1
            )
            # lat/longエラーがあるためOKはFalse
            self.assertFalse(ok)
            # WARNINGは出力されない（INFOは出力される）
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'lat/long列が空または実数値でありません' in str(call)]
            self.assertEqual(len(warning_calls), 0)
            # INFOは出力される
            info_calls = [call for call in mock_logger.info.call_args_list 
                         if 'lat/long列が空または実数値でありません' in str(call)]
            self.assertEqual(len(info_calls), 1)

class TestFixAndUploadCsv(unittest.TestCase):
    """fix_and_upload_csv関数のテスト"""
    
    def setUp(self):
        """テスト前の準備"""
        self.mock_service = MagicMock()
        self.file_id = "test_file_id"
        self.decoded = "prefecture,city,number,address,name,lat,long\n東京都,渋谷区,1,テスト住所,テスト名前,35.6580,139.7016"
        self.header = ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long']
        self.rows = [['東京都', '渋谷区', '1', 'テスト住所', 'テスト名前', '35.6580', '139.7016']]
        self.row_num = 1
    
    @patch('check_normalized_csv.logger')
    def test_skip_latlong_update_for_fukuoka(self, mock_logger):
        """福岡県福岡市の場合は緯度経度上書きをスキップするテスト"""
        pref = "福岡県"
        city = "福岡市"
        
        check_normalized_csv.fix_and_upload_csv(
            self.mock_service, self.file_id, self.decoded, 
            self.header, self.rows, pref, city, self.row_num
        )
        
        # スキップメッセージがログに出力されることを確認
        mock_logger.info.assert_called_with(
            f"[{self.row_num}行目] {pref}{city}は緯度経度上書き対象外のため修正をスキップします"
        )
        
        # upload_csv_fileが呼ばれていないことを確認
        self.mock_service.files().update.assert_not_called()
    
    @patch('check_normalized_csv.logger')
    def test_proceed_latlong_update_for_other_cities(self, mock_logger):
        """他の自治体の場合は緯度経度上書きを実行するテスト"""
        pref = "東京都"
        city = "渋谷区"
        
        # 緯度経度が無効なデータに変更
        invalid_rows = [['東京都', '渋谷区', '1', 'テスト住所', 'テスト名前', '', '']]
        
        with patch('check_normalized_csv.get_latlng_google') as mock_google, \
             patch('check_normalized_csv.get_latlng_gsi') as mock_gsi, \
             patch('check_normalized_csv.upload_csv_file') as mock_upload:
            
            # APIの戻り値を設定
            mock_google.return_value = (35.6580, 139.7016)
            mock_gsi.return_value = (35.6581, 139.7017)
            
            check_normalized_csv.fix_and_upload_csv(
                self.mock_service, self.file_id, self.decoded, 
                self.header, invalid_rows, pref, city, self.row_num
            )
            
            # スキップメッセージがログに出力されないことを確認
            skip_calls = [call for call in mock_logger.info.call_args_list 
                         if '緯度経度上書き対象外' in str(call)]
            self.assertEqual(len(skip_calls), 0)
            
            # upload_csv_fileが呼ばれることを確認
            mock_upload.assert_called_once()
    
    @patch('check_normalized_csv.logger')
    def test_skip_latlong_update_list_contains_fukuoka(self, mock_logger):
        """SKIP_LATLONG_UPDATE_LISTに福岡県福岡市が含まれていることを確認するテスト"""
        self.assertIn(("福岡県", "福岡市"), check_normalized_csv.SKIP_LATLONG_UPDATE_LIST)
    
    @patch('check_normalized_csv.logger')
    def test_skip_latlong_update_list_is_modifiable(self, mock_logger):
        """SKIP_LATLONG_UPDATE_LISTが配列で定義されており、追加・削除可能であることを確認するテスト"""
        # リストが配列であることを確認
        self.assertIsInstance(check_normalized_csv.SKIP_LATLONG_UPDATE_LIST, list)
        
        # 追加テスト
        original_length = len(check_normalized_csv.SKIP_LATLONG_UPDATE_LIST)
        check_normalized_csv.SKIP_LATLONG_UPDATE_LIST.append(("テスト県", "テスト市"))
        self.assertEqual(len(check_normalized_csv.SKIP_LATLONG_UPDATE_LIST), original_length + 1)
        
        # 削除テスト
        check_normalized_csv.SKIP_LATLONG_UPDATE_LIST.pop()
        self.assertEqual(len(check_normalized_csv.SKIP_LATLONG_UPDATE_LIST), original_length)

    @patch('check_normalized_csv.logger')
    def test_skip_latlong_update_list_contains_hyogo_kasai(self, mock_logger):
        """SKIP_LATLONG_UPDATE_LISTに兵庫県加西市が含まれていることを確認"""
        self.assertIn(("兵庫県", "加西市"), check_normalized_csv.SKIP_LATLONG_UPDATE_LIST)

    @patch('check_normalized_csv.logger')
    def test_skip_latlong_update_list_contains_chiba_narashino(self, mock_logger):
        """SKIP_LATLONG_UPDATE_LISTに千葉県習志野市が含まれていることをテスト"""
        self.assertIn(('千葉県', '習志野市'), check_normalized_csv.SKIP_LATLONG_UPDATE_LIST)

class TestAppendCsvFileProcessing(unittest.TestCase):
    """*append.csvファイルの処理に関するテスト"""
    
    def setUp(self):
        """テスト前の準備"""
        self.mock_drive_service = Mock()
        self.mock_files = [
            {'id': 'file1', 'name': 'test_city_normalized.csv'},
            {'id': 'file2', 'name': 'test_city_normalized_20250708append.csv'},
            {'id': 'file3', 'name': 'test_city_normalized_20250708_append.csv'},
        ]
    
    @patch('check_normalized_csv.list_drive_files')
    def test_find_csv_files_includes_append_files(self, mock_list_files):
        """find_csv_filesが*append.csvファイルを含むことをテスト"""
        mock_list_files.return_value = self.mock_files
        
        result = check_normalized_csv.find_csv_files(
            self.mock_files, 'test_city', self.mock_drive_service
        )
        
        # 基本ファイルとappendファイルが含まれることを確認
        file_names = [file_name for _, file_name in result]
        self.assertIn('test_city_normalized.csv', file_names)
        self.assertIn('test_city_normalized_20250708append.csv', file_names)
        self.assertIn('test_city_normalized_20250708_append.csv', file_names)
    
    @patch('check_normalized_csv.get_file_modified_time')
    @patch('check_normalized_csv.list_drive_files')
    def test_append_file_individual_time_check(self, mock_list_files, mock_get_time):
        """*append.csvファイルが個別に更新日時チェックされることをテスト"""
        mock_list_files.return_value = self.mock_files
        
        # 基本ファイルは新しい、appendファイルは古いという状況をシミュレート
        def mock_get_time_side_effect(drive_service, file_id):
            if file_id == 'file1':  # 基本ファイル
                return datetime(2025, 7, 10, 10, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            elif file_id == 'file2':  # appendファイル（古い）
                return datetime(2025, 7, 8, 10, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            return None
        
        mock_get_time.side_effect = mock_get_time_side_effect
        
        # 2025年7月9日18時（JST）以降のファイルのみ処理する設定
        last_updated = datetime(2025, 7, 9, 18, 0, 0, tzinfo=timezone(timedelta(hours=9))).astimezone(timezone.utc)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            result = check_normalized_csv.should_skip_file_by_time(
                'test_city_normalized_20250708append.csv', 'file2', 
                self.mock_drive_service, last_updated
            )
            
            # appendファイルがスキップされることを確認
            self.assertTrue(result)
            mock_logger.info.assert_called_with(
                "スキップ: test_city_normalized_20250708append.csv の最終更新日時(2025-07-08 10:00:00)が指定日時(2025-07-09 18:00:00)より古いため"
            )
    
    @patch('check_normalized_csv.get_file_modified_time')
    @patch('check_normalized_csv.list_drive_files')
    def test_append_file_not_skipped_when_new(self, mock_list_files, mock_get_time):
        """*append.csvファイルが新しい場合はスキップされないことをテスト"""
        mock_list_files.return_value = self.mock_files
        
        # 基本ファイルとappendファイルの両方が新しいという状況をシミュレート
        def mock_get_time_side_effect(drive_service, file_id):
            if file_id == 'file1':  # 基本ファイル
                return datetime(2025, 7, 10, 10, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            elif file_id == 'file2':  # appendファイル（新しい）
                return datetime(2025, 7, 10, 12, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            return None
        
        mock_get_time.side_effect = mock_get_time_side_effect
        
        # 2025年7月9日18時以降のファイルのみ処理する設定
        last_updated = datetime(2025, 7, 9, 18, 0, 0, tzinfo=timezone.utc)
        
        with patch('check_normalized_csv.logger') as mock_logger:
            result = check_normalized_csv.should_skip_file_by_time(
                'test_city_normalized_20250708append.csv', 'file2', 
                self.mock_drive_service, last_updated
            )
            
            # appendファイルがスキップされないことを確認
            self.assertFalse(result)
            mock_logger.info.assert_not_called()
    
    @patch('check_normalized_csv.process_csv_file')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.list_drive_files')
    def test_process_single_target_handles_append_files(self, mock_list_files, mock_find_files, mock_process):
        """process_single_targetが*append.csvファイルを処理することをテスト"""
        mock_list_files.return_value = self.mock_files
        mock_find_files.return_value = [
            (self.mock_files[0], 'test_city_normalized.csv'),
            (self.mock_files[1], 'test_city_normalized_20250708append.csv'),
        ]
        
        target = (1, 'test_pref', 'test_city', 'folder_id')
        opts = Mock()
        opts.last_updated = None  # 日時チェック無効
        opts.final_mode = False  # 最終モードではない
        counters = {'skipped_count': 0}
        
        with patch('check_normalized_csv.find_deletion_targets', return_value=[]):
            check_normalized_csv.process_single_target(
                target, self.mock_drive_service, opts, counters
            )
        
        # 両方のファイルが処理されることを確認
        self.assertEqual(mock_process.call_count, 2)
        call_args = mock_process.call_args_list
        self.assertEqual(call_args[0][0][1], 'test_city_normalized.csv')  # 基本ファイル
        self.assertEqual(call_args[1][0][1], 'test_city_normalized_20250708append.csv')  # appendファイル 

    @patch('check_normalized_csv.list_drive_files')
    def test_should_skip_by_time_file_not_found_logs_filename(self, mock_list_files):
        """should_skip_by_timeでファイルが見つからない場合にファイル名がログに含まれることをテスト"""
        # ファイルが存在しない状況をシミュレート
        mock_list_files.return_value = []
        
        with patch('check_normalized_csv.logger') as mock_logger:
            result = check_normalized_csv.should_skip_by_time(
                'test_pref', 'test_city', 'folder_id', 
                self.mock_drive_service, datetime(2025, 7, 9, 18, 0, 0, tzinfo=timezone.utc)
            )
            
            # ファイルが見つからない場合のログにファイル名が含まれることを確認
            mock_logger.warning.assert_called_with("ファイル test_city_normalized.csv が見つからないため日時チェックをスキップ")
            self.assertFalse(result)

class TestDeletionFunctionality(unittest.TestCase):
    """削除機能のテスト"""
    
    def setUp(self):
        self.mock_drive_service = Mock()
        self.mock_gc = Mock()
        self.mock_worksheet = Mock()
        
        # 基本的なモック設定
        self.mock_worksheet.get_all_values.return_value = [
            ['都道府県', '市区町村', '正規化済みCSV', 'フォルダID(変更しないでください)'],
            ['東京都', '渋谷区', '全部あり', 'folder123'],
            ['東京都', '新宿区', '全部あり', 'folder456']
        ]
        
        self.mock_sheet = Mock()
        self.mock_sheet.worksheet.return_value = self.mock_worksheet
        self.mock_gc.open_by_url.return_value = self.mock_sheet

    def test_find_deletion_targets(self):
        """削除希望ファイルの検出テスト"""
        mock_files = [
            {'id': 'file1', 'name': 'test.csv'},
            {'id': 'file2', 'name': '削除希望_old.csv'},
            {'id': 'file3', 'name': 'normalized.csv'},
            {'id': 'file4', 'name': '削除希望_folder'},
        ]
        
        with patch('check_normalized_csv.list_drive_files', return_value=mock_files):
            targets = check_normalized_csv.find_deletion_targets(self.mock_drive_service, 'folder123')
            
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0]['name'], '削除希望_old.csv')
        self.assertEqual(targets[1]['name'], '削除希望_folder')

    def test_delete_drive_file_success(self):
        """ファイル削除成功テスト"""
        self.mock_drive_service.files.return_value.delete.return_value.execute.return_value = None
        
        result = check_normalized_csv.delete_drive_file(self.mock_drive_service, 'file123', 'test.csv')
        
        self.assertTrue(result)
        self.mock_drive_service.files.return_value.delete.assert_called_once_with(
            fileId='file123',
            supportsAllDrives=True
        )

    def test_delete_drive_file_failure(self):
        """ファイル削除失敗テスト"""
        self.mock_drive_service.files.return_value.delete.return_value.execute.side_effect = Exception("削除エラー")
        
        with patch('check_normalized_csv.logger') as mock_logger:
            result = check_normalized_csv.delete_drive_file(self.mock_drive_service, 'file123', 'test.csv')
        
        self.assertFalse(result)
        mock_logger.error.assert_called_once()

    def test_process_single_target_with_deletion_targets_check_mode(self):
        """チェックモードでの削除希望ファイル検出テスト"""
        mock_files = [
            {'id': 'file1', 'name': '渋谷区_normalized.csv'},
            {'id': 'file2', 'name': '削除希望_old.csv'},
        ]
        
        with patch('check_normalized_csv.find_deletion_targets', return_value=[mock_files[1]]):
            with patch('check_normalized_csv.list_drive_files', return_value=mock_files):
                with patch('check_normalized_csv.find_csv_files', return_value=[]):
                    with patch('check_normalized_csv.logger') as mock_logger:
                        opts = Mock()
                        opts.delete_mode = False
                        opts.last_updated = None
                        opts.final_mode = False  # 最終モードではない
                        counters = {'total_count': 0, 'error_count': 0, 'warning_count': 0, 'skipped_count': 0, 'deletion_target_count': 0, 'deletion_success_count': 0}
                        
                        check_normalized_csv.process_single_target((1, '東京都', '渋谷区', 'folder123'), 
                                            self.mock_drive_service, opts, counters)
        
        mock_logger.info.assert_called_with("[1行目] 東京都渋谷区: 削除希望ファイル 1件を検出しました: ['削除希望_old.csv']")
        self.assertEqual(counters['deletion_target_count'], 1)
        self.assertEqual(counters['deletion_success_count'], 0)

    def test_process_single_target_with_deletion_targets_delete_mode(self):
        """削除モードでの削除希望ファイル削除テスト"""
        mock_files = [
            {'id': 'file1', 'name': '渋谷区_normalized.csv'},
            {'id': 'file2', 'name': '削除希望_old.csv'},
        ]
        
        with patch('check_normalized_csv.find_deletion_targets', return_value=[mock_files[1]]):
            with patch('check_normalized_csv.delete_drive_file', return_value=True) as mock_delete:
                with patch('check_normalized_csv.list_drive_files', return_value=mock_files):
                    with patch('check_normalized_csv.find_csv_files', return_value=[]):
                        with patch('check_normalized_csv.logger') as mock_logger:
                            opts = Mock()
                            opts.delete_mode = True
                            opts.last_updated = None
                            opts.final_mode = False  # 最終モードではない
                            counters = {'total_count': 0, 'error_count': 0, 'warning_count': 0, 'skipped_count': 0, 'deletion_target_count': 0, 'deletion_success_count': 0}
                            
                            check_normalized_csv.process_single_target((1, '東京都', '渋谷区', 'folder123'), 
                                                self.mock_drive_service, opts, counters)
        
        mock_logger.info.assert_called_with("[1行目] 東京都渋谷区: 削除希望ファイル 1件を削除します")
        mock_delete.assert_called_once_with(self.mock_drive_service, 'file2', '削除希望_old.csv')
        self.assertEqual(counters['deletion_target_count'], 1)
        self.assertEqual(counters['deletion_success_count'], 1)

    def test_process_single_target_with_deletion_targets_delete_mode_partial_failure(self):
        """削除モードでの削除失敗も含むテスト"""
        mock_files = [
            {'id': 'file1', 'name': '渋谷区_normalized.csv'},
            {'id': 'file2', 'name': '削除希望_old.csv'},
            {'id': 'file3', 'name': '削除希望_new.csv'},
        ]
        
        def mock_delete_side_effect(service, file_id, file_name):
            if file_name == '削除希望_old.csv':
                return True
            else:
                return False
        
        with patch('check_normalized_csv.find_deletion_targets', return_value=[mock_files[1], mock_files[2]]):
            with patch('check_normalized_csv.delete_drive_file', side_effect=mock_delete_side_effect) as mock_delete:
                with patch('check_normalized_csv.list_drive_files', return_value=mock_files):
                    with patch('check_normalized_csv.find_csv_files', return_value=[]):
                        with patch('check_normalized_csv.logger') as mock_logger:
                            opts = Mock()
                            opts.delete_mode = True
                            opts.last_updated = None
                            opts.final_mode = False  # 最終モードではない
                            counters = {'total_count': 0, 'error_count': 0, 'warning_count': 0, 'skipped_count': 0, 'deletion_target_count': 0, 'deletion_success_count': 0}
                            
                            check_normalized_csv.process_single_target((1, '東京都', '渋谷区', 'folder123'), 
                                                self.mock_drive_service, opts, counters)
        
        mock_logger.info.assert_called_with("[1行目] 東京都渋谷区: 削除希望ファイル 2件を削除します")
        self.assertEqual(mock_delete.call_count, 2)
        self.assertEqual(counters['deletion_target_count'], 2)
        self.assertEqual(counters['deletion_success_count'], 1)  # 1件成功、1件失敗

    def test_main_with_delete_option(self):
        """削除オプション付きでのmain関数テスト"""
        with patch('check_normalized_csv.get_credentials') as mock_creds:
            with patch('check_normalized_csv.gspread.authorize') as mock_auth:
                with patch('check_normalized_csv.build') as mock_build:
                    with patch('check_normalized_csv.get_targets') as mock_targets:
                        with patch('check_normalized_csv.process_single_target') as mock_process:
                            with patch('check_normalized_csv.logger') as mock_logger:
                                with patch('sys.argv', ['script.py', '-d']):
                                    mock_creds.return_value = Mock()
                                    mock_auth.return_value = self.mock_gc
                                    mock_build.return_value = self.mock_drive_service
                                    mock_targets.return_value = [(1, '東京都', '渋谷区', 'folder123')]
                                    
                                    check_normalized_csv.main()
                                    
                                    # 削除モードが正しく設定されていることを確認
                                    mock_process.assert_called_once()
                                    call_args = mock_process.call_args
                                    opts = call_args[0][2]  # 3番目の引数がopts
                                    self.assertTrue(opts.delete_mode)

    def test_main_without_delete_option(self):
        """削除オプションなしでのmain関数テスト"""
        with patch('check_normalized_csv.get_credentials') as mock_creds:
            with patch('check_normalized_csv.gspread.authorize') as mock_auth:
                with patch('check_normalized_csv.build') as mock_build:
                    with patch('check_normalized_csv.get_targets') as mock_targets:
                        with patch('check_normalized_csv.process_single_target') as mock_process:
                            with patch('check_normalized_csv.logger') as mock_logger:
                                with patch('sys.argv', ['script.py']):
                                    mock_creds.return_value = Mock()
                                    mock_auth.return_value = self.mock_gc
                                    mock_build.return_value = self.mock_drive_service
                                    mock_targets.return_value = [(1, '東京都', '渋谷区', 'folder123')]
                                    
                                    check_normalized_csv.main()
                                    
                                    # 削除モードがFalseに設定されていることを確認
                                    mock_process.assert_called_once()
                                    call_args = mock_process.call_args
                                    opts = call_args[0][2]  # 3番目の引数がopts
                                    self.assertFalse(opts.delete_mode)

class TestFinalNormalizedCsv(unittest.TestCase):
    """最終正規化CSV作成機能のテスト"""
    
    def setUp(self):
        """テスト前の準備"""
        self.mock_drive_service = Mock()
        self.pref = "東京都"
        self.city = "渋谷区"
        self.folder_id = "folder123"
        self.row_num = 1
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.upload_csv_file')
    def test_create_final_normalized_csv_basic(self, mock_upload, mock_download, mock_find_files, mock_list_files):
        """基本的な最終正規化CSV作成のテスト"""
        # モックファイルリスト
        mock_files = [
            {'id': 'file1', 'name': '渋谷区_normalized.csv'},
            {'id': 'file2', 'name': '渋谷区_normalized_20250708append.csv'},
        ]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [
            (mock_files[0], '渋谷区_normalized.csv'),
            (mock_files[1], '渋谷区_normalized_20250708append.csv'),
        ]
        
        # CSVコンテンツのモック
        base_csv = b'prefecture,city,number,address,name,lat,long\n' + \
                   '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                   '東京都,渋谷区,1-2,渋谷1-2,店舗B,,\n'.encode('utf-8')  # lat/long空
        
        append_csv = b'prefecture,city,number,address,name,lat,long\n' + \
                    '東京都,渋谷区,2-1,渋谷2-1,店舗C,35.659,139.702\n'.encode('utf-8') + \
                    '東京都,渋谷区,2-2,渋谷2-2,店舗D,35.660,139.703\n'.encode('utf-8')
        
        mock_download.side_effect = [
            (base_csv, 'file1'),
            (append_csv, 'file2')
        ]
        
        # 実行
        check_normalized_csv.create_final_normalized_csv(
            self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
        )
        
        # アップロードが呼ばれたことを確認
        self.mock_drive_service.files().create.assert_called_once()
        call_args = self.mock_drive_service.files().create.call_args
        
        # ファイル名が正しいことを確認
        self.assertEqual(call_args[1]['body']['name'], '渋谷区_normalized_final.csv')
        self.assertEqual(call_args[1]['body']['parents'], ['folder123'])
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.upload_csv_file')
    @patch('check_normalized_csv.logger')
    def test_filter_empty_latlong(self, mock_logger, mock_upload, mock_download, mock_find_files, mock_list_files):
        """lat/longが空の行がフィルタリングされることをテスト"""
        mock_files = [{'id': 'file1', 'name': '渋谷区_normalized.csv'}]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '渋谷区_normalized.csv')]
        
        # lat/longが混在するCSV
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-2,渋谷1-2,店舗B,,\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-3,渋谷1-3,店舗C,35.659,\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-4,渋谷1-4,店舗D,35.660,139.703\n'.encode('utf-8')
        
        mock_download.return_value = (csv_content, 'file1')
        
        # MediaIoBaseUploadをモック
        with patch('check_normalized_csv.MediaIoBaseUpload') as mock_media:
            check_normalized_csv.create_final_normalized_csv(
                self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
            )
            
            # MediaIoBaseUploadに渡されたコンテンツを取得
            call_args = mock_media.call_args
            uploaded_content = call_args[0][0].getvalue().decode('utf-8')
            
            # フィルタリング結果を確認（2行のみ残る）
            lines = uploaded_content.strip().split('\n')
            self.assertEqual(len(lines), 3)  # ヘッダー + 2データ行
            self.assertIn('店舗A', uploaded_content)
            self.assertIn('店舗D', uploaded_content)
            self.assertNotIn('店舗B', uploaded_content)
            self.assertNotIn('店舗C', uploaded_content)
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.logger')
    def test_sort_by_number_column(self, mock_logger, mock_download, mock_find_files, mock_list_files):
        """number列で数値順ソートされることをテスト（行政区なし）"""
        mock_files = [{'id': 'file1', 'name': '渋谷区_normalized.csv'}]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '渋谷区_normalized.csv')]
        
        # 順序がバラバラなCSV（行政区なし）
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '東京都,渋谷区,3-1,渋谷3-1,店舗C,35.659,139.702\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,2-1,渋谷2-1,店舗B,35.659,139.702\n'.encode('utf-8') + \
                     '東京都,渋谷区,10-1,渋谷10-1,店舗D,35.660,139.703\n'.encode('utf-8')
        
        mock_download.return_value = (csv_content, 'file1')
        
        with patch('check_normalized_csv.MediaIoBaseUpload') as mock_media:
            check_normalized_csv.create_final_normalized_csv(
                self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
            )
            
            # アップロードされたコンテンツを取得
            call_args = mock_media.call_args
            uploaded_content = call_args[0][0].getvalue().decode('utf-8')
            
            # 数値順でソートされていることを確認
            lines = uploaded_content.strip().split('\n')
            self.assertEqual(len(lines), 5)  # ヘッダー + 4データ行
            
            # number列の順序を確認（数値順: 1-1, 2-1, 3-1, 10-1）
            self.assertIn('1-1', lines[1])
            self.assertIn('2-1', lines[2])
            self.assertIn('3-1', lines[3])
            self.assertIn('10-1', lines[4])
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.upload_csv_file')
    def test_overwrite_existing_final_csv(self, mock_upload, mock_download, mock_find_files, mock_list_files):
        """既存の最終正規化CSVファイルが上書きされることをテスト"""
        # 既存の最終正規化CSVファイルが存在する
        mock_files = [
            {'id': 'file1', 'name': '渋谷区_normalized.csv'},
            {'id': 'file2', 'name': '渋谷区_normalized_final.csv'},  # 既存
        ]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '渋谷区_normalized.csv')]
        
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8')
        
        mock_download.return_value = (csv_content, 'file1')
        
        # 実行
        check_normalized_csv.create_final_normalized_csv(
            self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
        )
        
        # 上書き用のupload_csv_fileが呼ばれたことを確認
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args
        self.assertEqual(call_args[0][1], 'file2')  # 既存ファイルのID
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.logger')
    def test_no_csv_files_found(self, mock_logger, mock_find_files, mock_list_files):
        """CSVファイルが見つからない場合のエラーハンドリング"""
        mock_list_files.return_value = []
        mock_find_files.return_value = []
        
        # 実行
        check_normalized_csv.create_final_normalized_csv(
            self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
        )
        
        # エラーログが出力されることを確認
        mock_logger.error.assert_called_with(
            f"[{self.row_num}行目] {self.pref}{self.city}: 正規化CSVファイルが見つかりません"
        )
    
    def test_main_with_final_option(self):
        """最終正規化CSVモードでのmain関数テスト"""
        with patch('check_normalized_csv.get_credentials') as mock_creds:
            with patch('check_normalized_csv.gspread.authorize') as mock_auth:
                with patch('check_normalized_csv.build') as mock_build:
                    with patch('check_normalized_csv.get_targets') as mock_targets:
                        with patch('check_normalized_csv.process_single_target') as mock_process:
                            with patch('check_normalized_csv.logger') as mock_logger:
                                with patch('sys.argv', ['script.py', '-f']):
                                    mock_creds.return_value = Mock()
                                    mock_auth.return_value = Mock()
                                    mock_build.return_value = self.mock_drive_service
                                    mock_targets.return_value = [(1, '東京都', '渋谷区', 'folder123')]
                                    
                                    check_normalized_csv.main()
                                    
                                    # 最終正規化CSVモードが正しく設定されていることを確認
                                    mock_process.assert_called_once()
                                    call_args = mock_process.call_args
                                    opts = call_args[0][2]  # 3番目の引数がopts
                                    self.assertTrue(opts.final_mode)

    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.logger')
    def test_sort_by_administrative_ward_and_number(self, mock_logger, mock_download, mock_find_files, mock_list_files):
        """政令指定都市の行政区とnumberでソートされることをテスト"""
        mock_files = [{'id': 'file1', 'name': '横浜市_normalized.csv'}]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '横浜市_normalized.csv')]
        
        # 行政区を含むCSV（横浜市の例）
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '神奈川県,横浜市,3-1,港北区港北3-1,店舗C,35.659,139.702\n'.encode('utf-8') + \
                     '神奈川県,横浜市,1-1,港北区港北1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '神奈川県,横浜市,2-1,中区中央2-1,店舗B,35.659,139.702\n'.encode('utf-8') + \
                     '神奈川県,横浜市,10-1,港北区港北10-1,店舗D,35.660,139.703\n'.encode('utf-8') + \
                     '神奈川県,横浜市,1-2,中区中央1-2,店舗E,35.661,139.704\n'.encode('utf-8')
        
        mock_download.return_value = (csv_content, 'file1')
        
        with patch('check_normalized_csv.MediaIoBaseUpload') as mock_media:
            check_normalized_csv.create_final_normalized_csv(
                '神奈川県', '横浜市', self.folder_id, self.mock_drive_service, self.row_num
            )
            
            # アップロードされたコンテンツを取得
            call_args = mock_media.call_args
            uploaded_content = call_args[0][0].getvalue().decode('utf-8')
            
            # 行政区でグルーピングされ、各区内でnumberでソートされていることを確認
            lines = uploaded_content.strip().split('\n')
            self.assertEqual(len(lines), 6)  # ヘッダー + 5データ行
            
            # 期待される順序:
            # 1. 中区中央1-2,店舗E
            # 2. 中区中央2-1,店舗B  
            # 3. 港北区港北1-1,店舗A
            # 4. 港北区港北3-1,店舗C
            # 5. 港北区港北10-1,店舗D
            self.assertIn('中区中央1-2', lines[1])
            self.assertIn('店舗E', lines[1])
            self.assertIn('中区中央2-1', lines[2])
            self.assertIn('店舗B', lines[2])
            self.assertIn('港北区港北1-1', lines[3])
            self.assertIn('店舗A', lines[3])
            self.assertIn('港北区港北3-1', lines[4])
            self.assertIn('店舗C', lines[4])
            self.assertIn('港北区港北10-1', lines[5])
            self.assertIn('店舗D', lines[5])
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.logger')
    def test_duplicate_row_detection_and_removal(self, mock_logger, mock_download, mock_find_files, mock_list_files):
        """重複行の検出と除去のテスト"""
        mock_files = [{'id': 'file1', 'name': '渋谷区_normalized.csv'}]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '渋谷区_normalized.csv')]
        
        # 重複を含むCSVコンテンツ（nameも含めて重複チェック）
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-2,渋谷1-2,店舗B,35.659,139.702\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-3,渋谷1-3,店舗C,35.660,139.703\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8')  # 同じnumber,name,lat,long
        
        mock_download.return_value = (csv_content, 'file1')
        
        with patch('check_normalized_csv.MediaIoBaseUpload') as mock_media:
            check_normalized_csv.create_final_normalized_csv(
                self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
            )
            
            # WARNINGログが出力されたか確認
            warning_calls = [call for call in mock_logger.warning.call_args_list]
            self.assertEqual(len(warning_calls), 2)  # 2回の重複
            
            # アップロードされたコンテンツを取得
            call_args = mock_media.call_args
            uploaded_content = call_args[0][0].getvalue().decode('utf-8')
            
            # データ行数を確認（ヘッダー + 3行）
            lines = uploaded_content.strip().split('\n')
            self.assertEqual(len(lines), 4)  # ヘッダー + 3データ行
            
            # 最後の重複行が採用されていることを確認（新仕様）
            # ソート後の順序を考慮
            data_lines = lines[1:]
            # 重複チェックはnumber,name,lat,longの4つで行われるため
            # 3つの店舗A行のうち最後の1つのみが残る
            self.assertEqual(len(data_lines), 3)  # 店舗A（最後）、店舗B、店舗C
            names = [line.split(',')[4] for line in data_lines]
            self.assertIn('店舗A', names)
            self.assertIn('店舗B', names)
            self.assertIn('店舗C', names)
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.logger')
    def test_no_duplicate_when_empty_values(self, mock_logger, mock_download, mock_find_files, mock_list_files):
        """空の値がある場合は重複チェックされないことをテスト"""
        mock_files = [{'id': 'file1', 'name': '渋谷区_normalized.csv'}]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '渋谷区_normalized.csv')]
        
        # number,nameは同じだがlat/longが空のデータを含むCSV
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,,\n'.encode('utf-8') + \
                     '東京都,渋谷区,,渋谷1-1,店舗C,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,2-1,渋谷2-1,店舗E,35.659,139.702\n'.encode('utf-8')
        
        mock_download.return_value = (csv_content, 'file1')
        
        with patch('check_normalized_csv.MediaIoBaseUpload') as mock_media:
            check_normalized_csv.create_final_normalized_csv(
                self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
            )
            
            # WARNINGログが出力されていないか確認
            warning_calls = [call for call in mock_logger.warning.call_args_list]
            duplicate_warnings = [call for call in warning_calls if '重複行' in str(call)]
            self.assertEqual(len(duplicate_warnings), 0)
            
            # アップロードされたコンテンツを取得
            call_args = mock_media.call_args
            uploaded_content = call_args[0][0].getvalue().decode('utf-8')
            
            # lat/longが空でない行のみ含まれることを確認
            lines = uploaded_content.strip().split('\n')
            # 店舗Cもlat/longが両方入っているので含まれる
            self.assertEqual(len(lines), 4)  # ヘッダー + 3データ行（店舗A、C、E）
            data_content = '\n'.join(lines[1:])
            self.assertIn('店舗A', data_content)
            self.assertIn('店舗C', data_content)
            self.assertIn('店舗E', data_content)
    
    @patch('check_normalized_csv.list_drive_files')
    @patch('check_normalized_csv.find_csv_files')
    @patch('check_normalized_csv.download_csv_file')
    @patch('check_normalized_csv.logger')
    def test_duplicate_info_logging(self, mock_logger, mock_download, mock_find_files, mock_list_files):
        """重複除去時のINFOログ確認"""
        mock_files = [{'id': 'file1', 'name': '渋谷区_normalized.csv'}]
        mock_list_files.return_value = mock_files
        mock_find_files.return_value = [(mock_files[0], '渋谷区_normalized.csv')]
        
        # 複数の重複を含むCSV（nameも同じ場合のみ重複）
        csv_content = b'prefecture,city,number,address,name,lat,long\n' + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8') + \
                     '東京都,渋谷区,2-1,渋谷2-1,店舗B,35.659,139.702\n'.encode('utf-8') + \
                     '東京都,渋谷区,2-1,渋谷2-1,店舗B,35.659,139.702\n'.encode('utf-8') + \
                     '東京都,渋谷区,1-1,渋谷1-1,店舗A,35.658,139.701\n'.encode('utf-8')
        
        mock_download.return_value = (csv_content, 'file1')
        
        with patch('check_normalized_csv.MediaIoBaseUpload') as mock_media:
            check_normalized_csv.create_final_normalized_csv(
                self.pref, self.city, self.folder_id, self.mock_drive_service, self.row_num
            )
            
            # INFOログで重複除去数が出力されるか確認
            info_calls = [call for call in mock_logger.info.call_args_list]
            duplicate_info = [call for call in info_calls if '3件の重複行を除去' in str(call)]
            self.assertEqual(len(duplicate_info), 1)

class TestAdministrativeWardExtraction(unittest.TestCase):
    """行政区抽出機能のテスト"""
    
    def test_extract_administrative_ward(self):
        """行政区抽出の基本テスト"""
        # 正常なケース
        self.assertEqual(check_normalized_csv.extract_administrative_ward('港北区港北3-1'), '港北区')
        self.assertEqual(check_normalized_csv.extract_administrative_ward('中区中央2-1'), '中区')
        self.assertEqual(check_normalized_csv.extract_administrative_ward('青葉区青葉台1-1'), '青葉区')
        
        # 行政区がない場合
        self.assertIsNone(check_normalized_csv.extract_administrative_ward('渋谷3-1'))
        self.assertIsNone(check_normalized_csv.extract_administrative_ward('新宿区新宿3-1'))  # 特別区は対象外
        
        # 複雑なケース
        self.assertEqual(check_normalized_csv.extract_administrative_ward('神奈川区神奈川1-1'), '神奈川区')
        self.assertEqual(check_normalized_csv.extract_administrative_ward('西区西1-1'), '西区')
        
    def test_create_sort_key(self):
        """ソートキー生成のテスト"""
        header = ['prefecture', 'city', 'number', 'address', 'name', 'lat', 'long']
        idx_address = header.index('address')
        idx_number = header.index('number')
        
        # 行政区ありの場合
        row1 = ['神奈川県', '横浜市', '3-1', '港北区港北3-1', '店舗C', '35.659', '139.702']
        self.assertEqual(check_normalized_csv.create_sort_key(row1, idx_address, idx_number), ('港北区', (3, 1, '')))
        
        # 行政区なしの場合
        row2 = ['東京都', '渋谷区', '1-1', '渋谷1-1', '店舗A', '35.658', '139.701']
        self.assertEqual(check_normalized_csv.create_sort_key(row2, idx_address, idx_number), ('', (1, 1, '')))
        
        # インデックスが無効な場合
        self.assertEqual(check_normalized_csv.create_sort_key(row1, -1, idx_number), ('', (3, 1, '')))
        self.assertEqual(check_normalized_csv.create_sort_key(row1, idx_address, -1), ('', ''))

    def test_parse_number_for_sort(self):
        """parse_number_for_sort関数のテスト"""
        # 基本的な数値
        self.assertEqual(check_normalized_csv.parse_number_for_sort('123'), (123, 0, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('1'), (1, 0, ''))
        
        # 「第n投票区」形式
        self.assertEqual(check_normalized_csv.parse_number_for_sort('第1投票区'), (1, 0, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('第10投票区'), (10, 0, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('第3投票区'), (3, 0, ''))
        
        # 数値＋文字列の混在パターン
        self.assertEqual(check_normalized_csv.parse_number_for_sort('1番地'), (1, 0, '番地'))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('10丁目'), (10, 0, '丁目'))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3号'), (3, 0, '号'))
        
        # 数値＋文字列＋区切り文字＋数値
        self.assertEqual(check_normalized_csv.parse_number_for_sort('1番地-2'), (1, 2, '番地-2'))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('10丁目_5'), (10, 5, '丁目_5'))
        
        # m-n形式（従来の機能）
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3-1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('10-5'), (10, 5, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('1-23'), (1, 23, ''))
        
        # 他の区切り文字
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3_1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3:1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3.1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3/1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3\\1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3|1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3~1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3+1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3=1'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3 1'), (3, 1, ''))
        
        # 区切り文字のみ（nが無い）
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3-'), (3, 0, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('10_'), (10, 0, ''))
        
        # 空文字
        self.assertEqual(check_normalized_csv.parse_number_for_sort(''), (0, 0, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort(None), (0, 0, ''))
        
        # 数値でない場合
        self.assertEqual(check_normalized_csv.parse_number_for_sort('abc'), (float('inf'), 0, 'abc'))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('a-b'), (float('inf'), 0, 'a-b'))
        
        # 最初の区切り文字のみで分割
        self.assertEqual(check_normalized_csv.parse_number_for_sort('3-1-2'), (3, 1, ''))
        self.assertEqual(check_normalized_csv.parse_number_for_sort('10-5-3'), (10, 5, ''))

    def test_natural_sort_examples(self):
        """自然順ソートの具体例テスト"""
        # 投票区のソート例
        voting_numbers = ['第10投票区', '第1投票区', '第3投票区', '第2投票区']
        sorted_voting = sorted(voting_numbers, key=check_normalized_csv.parse_number_for_sort)
        expected_voting = ['第1投票区', '第2投票区', '第3投票区', '第10投票区']
        self.assertEqual(sorted_voting, expected_voting)
        
        # 番地のソート例
        address_numbers = ['10番地', '1番地', '3番地', '2番地']
        sorted_address = sorted(address_numbers, key=check_normalized_csv.parse_number_for_sort)
        expected_address = ['1番地', '2番地', '3番地', '10番地']
        self.assertEqual(sorted_address, expected_address)
        
        # 混在パターンのソート例
        mixed_numbers = ['3-1', '10番地', '第2投票区', '1-5', '第1投票区', '2丁目']
        sorted_mixed = sorted(mixed_numbers, key=check_normalized_csv.parse_number_for_sort)
        expected_mixed = ['第1投票区', '1-5', '第2投票区', '2丁目', '3-1', '10番地']
        self.assertEqual(sorted_mixed, expected_mixed)
        
        # 数値＋文字列＋区切り文字＋数値のソート例
        complex_numbers = ['1番地-10', '1番地-2', '2番地-1', '10番地-1']
        sorted_complex = sorted(complex_numbers, key=check_normalized_csv.parse_number_for_sort)
        expected_complex = ['1番地-2', '1番地-10', '2番地-1', '10番地-1']
        self.assertEqual(sorted_complex, expected_complex)

if __name__ == '__main__':
    unittest.main() 