#!/usr/bin/env python3
"""
サイズ計算機能の統合テスト
実際のGoogle Drive APIを使用してテストを行います
"""

import sys
import os
import argparse

# テスト対象のモジュールをインポート
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check_normalized_csv import format_size, calculate_folder_size, process_size_calculation, get_credentials, get_targets
from googleapiclient.discovery import build
import gspread

def test_format_size():
    """format_size関数の動作確認"""
    print("=== format_size関数のテスト ===")
    
    test_cases = [
        (0, "0 B"),
        (1023, "1023.00 B"),
        (1024, "1.00 KB"),
        (1024 * 1024, "1.00 MB"),
        (1024 * 1024 * 1024, "1.00 GB"),
        (1536, "1.50 KB"),
        (1572864, "1.50 MB")
    ]
    
    for size_bytes, expected in test_cases:
        result = format_size(size_bytes)
        status = "✓" if result == expected else "✗"
        print(f"{status} {size_bytes} bytes -> {result} (expected: {expected})")

def test_with_real_api():
    """実際のAPIを使用したテスト（認証情報が必要）"""
    print("\n=== 実際のAPIを使用したテスト ===")
    
    try:
        # 認証情報の取得
        print("認証情報を取得中...")
        creds = get_credentials()
        
        # サービスオブジェクトの作成
        print("Google Drive APIサービスを作成中...")
        drive_service = build('drive', 'v3', credentials=creds)
        gc = gspread.authorize(creds)
        
        # テスト用の小さなターゲットリストを取得
        print("テスト用のターゲットを取得中...")
        class MockOpts:
            def __init__(self):
                self.args = ['東京都']  # 東京都のみをテスト
        
        opts = MockOpts()
        targets = get_targets(gc, opts)
        
        if not targets:
            print("テスト対象のフォルダが見つかりませんでした")
            return
        
        # 最初の3つのフォルダのみをテスト
        test_targets = targets[:3]
        print(f"テスト対象: {len(test_targets)}件のフォルダ")
        
        # サイズ計算の実行
        print("サイズ計算を実行中...")
        total_size = process_size_calculation(test_targets, drive_service)
        
        print(f"テスト完了: 合計サイズ = {format_size(total_size)}")
        
    except Exception as e:
        print(f"APIテストでエラーが発生しました: {e}")
        print("認証情報（my_secrets.json）が正しく設定されているか確認してください")

def test_without_auth():
    """認証なしでの基本機能テスト"""
    print("\n=== 認証なしでの基本機能テスト ===")
    
    # モックデータを使用したテスト
    from unittest.mock import Mock, patch
    
    mock_drive_service = Mock()
    
    # テスト用のターゲットデータ
    test_targets = [
        (2, '東京都', '新宿区', 'test_folder_1'),
        (3, '東京都', '渋谷区', 'test_folder_2'),
        (4, '大阪府', '大阪市', 'test_folder_3')
    ]
    
    # calculate_folder_sizeをモック化
    with patch('check_normalized_csv.calculate_folder_size') as mock_calc:
        mock_calc.side_effect = [
            (1024 * 1024 * 15, [{'name': 'file1.csv'}] * 45),  # 15MB
            (1024 * 1024 * 12, [{'name': 'file2.csv'}] * 38),  # 12MB
            (1024 * 1024 * 8, [{'name': 'file3.csv'}] * 25)    # 8MB
        ]
        
        total_size = process_size_calculation(test_targets, mock_drive_service)
        
        print(f"モックテスト結果: 合計サイズ = {format_size(total_size)}")
        print("✓ モックテストが正常に完了しました")

def main():
    parser = argparse.ArgumentParser(description='サイズ計算機能の統合テスト')
    parser.add_argument('--with-auth', action='store_true', 
                       help='実際のGoogle Drive APIを使用してテスト（認証情報が必要）')
    parser.add_argument('--basic-only', action='store_true',
                       help='基本機能のみテスト（認証不要）')
    
    args = parser.parse_args()
    
    # format_size関数のテスト（常に実行）
    test_format_size()
    
    if args.with_auth:
        # 実際のAPIを使用したテスト
        test_with_real_api()
    elif args.basic_only:
        # 認証なしでの基本機能テスト
        test_without_auth()
    else:
        # デフォルト: 基本機能テスト
        test_without_auth()
        print("\n実際のAPIを使用したテストを実行するには --with-auth オプションを使用してください")

if __name__ == '__main__':
    main() 