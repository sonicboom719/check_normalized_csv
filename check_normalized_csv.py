import argparse
import csv
import io
import logging
import os
import sys
import chardet
import time
import re
from datetime import datetime, timezone, timedelta
import requests
import math
import json

import gspread
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# my_secrets.jsonから認証情報を読み込む
with open('my_secrets.json', encoding='utf-8') as f:
    _secrets = json.load(f)
OAUTH2_CLIENT_INFO = _secrets['OAUTH2_CLIENT_INFO']
GOOGLE_API_KEY = _secrets['GOOGLE_API_KEY']

# my_settings.jsonからSKIP_LATLONG_UPDATE_LISTを読み込む
with open('my_settings.json', encoding='utf-8') as f:
    _settings = json.load(f)
SKIP_LATLONG_UPDATE_LIST = [tuple(x) for x in _settings['SKIP_LATLONG_UPDATE_LIST']]

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]
TOKEN_PATH = 'token.json'
LOGFILE = 'check_normalized_csv.log'

# ログ設定（ファイルオープンエラーハンドリング付き）
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(LOGFILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
except Exception as e:
    # ログファイルがオープンできない場合は、コンソール出力のみでログ設定
    print(f"警告: ログファイル '{LOGFILE}' をオープンできませんでした: {e}")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.error(f"ログファイル '{LOGFILE}' をオープンできませんでした: {e}")

def get_credentials():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_config(OAUTH2_CLIENT_INFO, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w', encoding='utf-8') as token:
            token.write(creds.to_json())
    return creds

def get_spreadsheet_rows(gc, url):
    sh = gc.open_by_url(url)
    worksheet = sh.worksheet("全自治体一覧")
    all_rows = worksheet.get_all_values()
    header = all_rows[0]
    rows = all_rows[1:]
    idx_pref = header.index("都道府県")
    idx_city = header.index("市区町村")
    idx_has_csv = header.index("正規化済みCSV")
    idx_folder = header.index("フォルダID(変更しないでください)")
    return rows, idx_pref, idx_city, idx_has_csv, idx_folder

def filter_rows(rows, idx_pref, idx_city, idx_has_csv, idx_folder, args):
    filtered = []
    for i, row in enumerate(rows):
        pref = row[idx_pref]
        city = row[idx_city]
        has_csv = row[idx_has_csv]
        folder = row[idx_folder]
        if len(args) == 0:
            pass
        elif len(args) == 1 and pref != args[0]:
            continue
        elif len(args) == 2 and (pref != args[0] or city != args[1]):
            continue
        if has_csv != "全部あり":
            continue
        filtered.append((i+2, pref, city, folder))
    return filtered

def list_drive_files(service, folder_id):
    files = []
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token
        ).execute()
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break
    return files

def find_deletion_targets(service, folder_id):
    """削除希望ファイル・フォルダを検出する"""
    files = list_drive_files(service, folder_id)
    deletion_targets = []
    
    for file in files:
        if '削除希望' in file['name']:
            deletion_targets.append(file)
    
    return deletion_targets

def delete_drive_file(service, file_id, file_name):
    """Google Driveファイルを削除する"""
    try:
        service.files().delete(
            fileId=file_id,
            supportsAllDrives=True
        ).execute()
        logger.info(f"削除完了: {file_name}")
        return True
    except Exception as e:
        logger.error(f"削除失敗: {file_name} - {e}")
        return False

def rename_drive_file(service, file_id, new_name):
    try:
        service.files().update(
            fileId=file_id,
            body={'name': new_name},
            supportsAllDrives=True
        ).execute()
        logger.info(f"ファイル名リネーム成功: {new_name}")
    except Exception as e:
        logger.error(f"ファイル名リネーム失敗: {e}")

def download_csv_file(service, folder_id, filename):
    files = list_drive_files(service, folder_id)
    target = next((f for f in files if f['name'].lower() == filename.lower()), None)
    if not target:
        return None, None
    file_id = target['id']
    try:
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read(), file_id
    except HttpError as error:
        logger.error(f"Google Drive API error: {error}")
        return None, None

def upload_csv_file(service, file_id, content):
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype='text/csv', resumable=True)
    try:
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        logger.info(f"UTF-8(BOM無し)でDrive上書き成功")
    except Exception as e:
        logger.error(f"Drive上書き失敗: {e}")

def detect_encoding(content):
    res = chardet.detect(content)
    if res is None:
        return None
    return res['encoding']

def haversine(lat1, lon1, lat2, lon2):
    """2点間の距離（メートル）を計算"""
    R = 6371000  # 地球半径(m)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def clean_address(pref, city, address):
    """address先頭の重複都道府県・市区町村を除去"""
    addr = address
    if addr.startswith(pref):
        addr = addr[len(pref):]
    if addr.startswith(city):
        addr = addr[len(city):]
    return f"{pref}{city}{addr}"

def get_latlng_google(full_address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={full_address}&key={GOOGLE_API_KEY}"
    resp = requests.get(url)
    data = resp.json()
    if data['status'] == 'OK' and data['results']:
        loc = data['results'][0]['geometry']['location']
        return float(loc['lat']), float(loc['lng'])
    return None, None

def get_latlng_gsi(full_address):
    url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={full_address}"
    resp = requests.get(url)
    data = resp.json()
    if data and isinstance(data, list) and 'geometry' in data[0]:
        coords = data[0]['geometry']['coordinates']
        return float(coords[1]), float(coords[0])
    return None, None

def check_csv_content(content, expected_pref, expected_city, row_num):
    # SKIP_LATLONG_UPDATE_LISTに含まれる自治体かどうかをチェック
    is_skip_target = (expected_pref, expected_city) in SKIP_LATLONG_UPDATE_LIST
    
    # NUL文字チェック
    if b'\x00' in content:
        if is_skip_target:
            logger.warning(f"[{row_num}] CSVにNUL文字(\\x00)が含まれています")
        else:
            logger.error(f"[{row_num}] CSVにNUL文字(\\x00)が含まれています")
        return False, False, None
    if not content:
        if is_skip_target:
            logger.warning(f"[{row_num}] CSVが空です。")
        else:
            logger.error(f"[{row_num}] CSVが空です。")
        return False, False, ''
    try:
        if content.startswith(b'\xef\xbb\xbf'):
            logger.info(f"[{row_num}] BOM有(UTF-8-SIG) 検出")
            decoded = content.decode('utf-8-sig')
            has_bom = True
            encoding = 'utf-8-sig'
        else:
            detect = chardet.detect(content)
            if detect is None:
                if is_skip_target:
                    logger.warning(f"[{row_num}] エンコード検出失敗")
                else:
                    logger.error(f"[{row_num}] エンコード検出失敗")
                return False, False, None
            encoding = detect['encoding']
            confidence = detect.get('confidence', 0)
            if encoding and encoding.lower() in ('utf-8', 'ascii'):
                decoded = content.decode('utf-8')
                has_bom = False
            elif encoding and encoding.lower() in ('shift_jis', 'cp932', 'ms932', 'sjis'):
                logger.warning(f"[{row_num}] Shift_JIS系({encoding})として仮デコード→UTF-8変換")
                decoded = content.decode(encoding)
                has_bom = False
            else:
                if is_skip_target:
                    logger.warning(f"[{row_num}] サポート外のエンコード: {encoding} (confidence={confidence})")
                else:
                    logger.error(f"[{row_num}] サポート外のエンコード: {encoding} (confidence={confidence})")
                return False, False, None
    except Exception as e:
        if is_skip_target:
            logger.warning(f"[{row_num}] CSV decode失敗: {e}")
        else:
            logger.error(f"[{row_num}] CSV decode失敗: {e}")
        return False, False, None

    reader = csv.reader(io.StringIO(decoded))
    rows = list(reader)
    if not rows:
        has_bom = False
        if is_skip_target:
            logger.warning(f"[{row_num}] CSVが空です。")
        else:
            logger.error(f"[{row_num}] CSVが空です。")
        return False, has_bom, ''
    header = rows[0]
    base_header = ['prefecture','city','number','address','name','lat','long']
    option_header = base_header + ['note']
    # 列名の集合で比較
    if set(header) == set(base_header):
        if header != base_header:
            logger.info(f"[{row_num}] ヘッダの順番が異なります: {header}")
        col_count = len(base_header)
    elif set(header) == set(option_header):
        if header != option_header:
            logger.info(f"[{row_num}] ヘッダの順番が異なります: {header}")
        col_count = len(option_header)
    else:
        if is_skip_target:
            logger.warning(f"[{row_num}] CSVヘッダが不正: {header}")
        else:
            logger.error(f"[{row_num}] CSVヘッダが不正: {header}")
        return False, has_bom, decoded
    ok = True
    idx_lat = header.index('lat') if 'lat' in header else -1
    idx_long = header.index('long') if 'long' in header else -1
    idx_addr = header.index('address') if 'address' in header else -1
    idx_note = header.index('note') if 'note' in header else -1
    idx_number = header.index('number') if 'number' in header else -1
    idx_name = header.index('name') if 'name' in header else -1
    
    # 重複チェック用のセット
    seen_combinations = set()
    
    for i, fields in enumerate(rows[1:], 2):
        if col_count == 8 and len(fields) == 7:
            continue
        if len(fields) != col_count:
            if is_skip_target:
                logger.warning(f"[{row_num}] {i}行目: 列数不一致({len(fields)} != {col_count})")
            else:
                logger.error(f"[{row_num}] {i}行目: 列数不一致({len(fields)} != {col_count})")
            ok = False
            # 要素数不足の場合は以降のチェックをスキップ
            if len(fields) < 2:
                continue
        # 追加: 要素数チェック
        if len(fields) < 2:
            if is_skip_target:
                logger.warning(f"[{row_num}] {i}行目: 列数が2未満のためprefecture/cityチェックをスキップ")
            else:
                logger.error(f"[{row_num}] {i}行目: 列数が2未満のためprefecture/cityチェックをスキップ")
            ok = False
            continue
        if fields[0] != expected_pref or fields[1] != expected_city:
            if is_skip_target:
                logger.warning(f"[{row_num}] {i}行目: prefecture/city列値が不一致 ({fields[0]}/{fields[1]})")
            else:
                logger.error(f"[{row_num}] {i}行目: prefecture/city列値が不一致 ({fields[0]}/{fields[1]})")
            ok = False
        # 追加: fieldsの長さチェック
        if len(fields) <= max(idx_lat, idx_long, idx_addr, idx_note):
            logger.error(f"[{row_num}] {i}行目: 列数不足のためlat/long修正をスキップ")
            continue
        # lat, longのバリデーション
        if idx_lat == -1 or idx_long == -1:
            if is_skip_target:
                logger.warning(f"[{row_num}] lat/long列が見つかりません")
            else:
                logger.error(f"[{row_num}] lat/long列が見つかりません")
            ok = False
            continue
        
        # 共通ロジックを使用
        latlong_invalid = validate_latlong(fields, idx_lat, idx_long)
        if latlong_invalid:
            lat = fields[idx_lat]
            long = fields[idx_long]
            note_val = fields[idx_note] if idx_note != -1 and len(fields) > idx_note else ''
            if note_val in ('削除', '不明'):
                logger.info(f"[{row_num}] {i}行目: lat/long列が空または実数値でありません (lat='{lat}', long='{long}', note='{note_val}')")
            elif is_skip_target:
                # SKIP_LATLONG_UPDATE_LISTに含まれる自治体の場合はlat/longエラーをINFOで出力
                logger.info(f"[{row_num}] {i}行目: lat/long列が空または実数値でありません (lat='{lat}', long='{long}', note='{note_val}')")
            else:
                logger.error(f"[{row_num}] {i}行目: lat/long列が空または実数値でありません (lat='{lat}', long='{long}', note='{note_val}')")
            ok = False
            # lat/longが無効な場合は重複チェックをスキップ
            continue
        
        # number, name, addressの重複チェック（lat/longが有効な場合のみ）
        if idx_number != -1 and idx_name != -1 and idx_addr != -1:
            number = fields[idx_number]
            name = fields[idx_name]
            address = fields[idx_addr]
            
            # 3列がすべて空でない場合のみ重複チェック
            if number != '' and name != '' and address != '':
                combination = (number, name, address)
                if combination in seen_combinations:
                    if is_skip_target:
                        logger.warning(f"[{row_num}] {i}行目: number, name, addressの組み合わせが重複しています (number='{number}', name='{name}', address='{address}')")
                    else:
                        logger.error(f"[{row_num}] {i}行目: number, name, addressの組み合わせが重複しています (number='{number}', name='{name}', address='{address}')")
                    ok = False
                else:
                    seen_combinations.add(combination)
    
    return ok, has_bom, decoded

def parse_datetime_arg(datetime_str):
    """日時文字列を解析してdatetimeオブジェクトを返す（日本時間で指定された日時をUTCに変換）"""
    try:
        if len(datetime_str) == 8:  # YYYYMMDD
            dt = datetime.strptime(datetime_str, '%Y%m%d')
            # 日本時間として解釈し、UTCタイムゾーン情報を追加
            jst_time = dt.replace(tzinfo=timezone(timedelta(hours=9)))
            return jst_time.astimezone(timezone.utc)
        elif len(datetime_str) == 12:  # YYYYMMDDHHMM
            dt = datetime.strptime(datetime_str, '%Y%m%d%H%M')
            # 日本時間として解釈し、UTCタイムゾーン情報を追加
            jst_time = dt.replace(tzinfo=timezone(timedelta(hours=9)))
            return jst_time.astimezone(timezone.utc)
        else:
            raise ValueError(f"不正な日時形式: {datetime_str}")
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"日時形式が不正です: {e}")

def get_file_modified_time(service, file_id):
    """Google Driveファイルの最終更新日時を取得（日本時間で返す）"""
    try:
        file_metadata = service.files().get(
            fileId=file_id,
            fields='modifiedTime',
            supportsAllDrives=True
        ).execute()
        modified_time_str = file_metadata.get('modifiedTime')
        if modified_time_str:
            # Google Driveの日時形式: 2025-07-01T12:34:56.789Z
            utc_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
            # UTCから日本時間（JST）に変換
            jst_time = utc_time.astimezone(timezone(timedelta(hours=9)))
            return jst_time
        return None
    except Exception as e:
        logger.error(f"ファイル更新日時取得失敗: {e}")
        return None

def fix_and_upload_csv(service, file_id, decoded, header, rows, pref, city, row_num):
    # 緯度経度上書きしないリストに含まれる自治体の場合はスキップ
    if (pref, city) in SKIP_LATLONG_UPDATE_LIST:
        logger.info(f"[{row_num}行目] {pref}{city}は緯度経度上書き対象外のため修正をスキップします")
        return
    
    # note列がなければ追加
    if 'note' not in header:
        header.append('note')
        for row in rows:
            row.append('')
    idx_lat = header.index('lat')
    idx_long = header.index('long')
    idx_addr = header.index('address')
    idx_note = header.index('note')
    changed = False
    any_api_failed = False
    for i, fields in enumerate(rows):
        # 追加: fieldsの長さチェック
        if len(fields) <= max(idx_lat, idx_long, idx_addr, idx_note):
            logger.error(f"[{row_num}行目] {i+2}行目: 列数不足のためlat/long修正をスキップ")
            continue
        lat = fields[idx_lat]
        long = fields[idx_long]
        latlong_invalid = False
        if lat == '' or long == '' or lat == 'None' or long == 'None':
            latlong_invalid = True
        else:
            try:
                float(lat)
                float(long)
            except Exception:
                latlong_invalid = True
        if latlong_invalid:
            # フル住所生成
            full_addr = clean_address(pref, city, fields[idx_addr])
            g_lat, g_lng = get_latlng_google(full_addr)
            j_lat, j_lng = get_latlng_gsi(full_addr)
            if g_lat is None or g_lng is None or j_lat is None or j_lng is None:
                any_api_failed = True
                continue
            fields[idx_lat] = str(g_lat)
            fields[idx_long] = str(g_lng)
            changed = True
            # note列初期化
            fields[idx_note] = ''
            # 距離差判定
            dist = haversine(g_lat, g_lng, j_lat, j_lng)
            if dist >= 200:
                fields[idx_note] = '緯度経度は怪しい'
    if any_api_failed:
        logger.warning(f"[{row_num}行目] APIで緯度経度が取得できなかったため上書き保存をスキップ")
        return
    # CSV再生成・上書き
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(header)
    writer.writerows(rows)
    content = output.getvalue().encode('utf-8')
    upload_csv_file(service, file_id, content)
    logger.info(f"[{row_num}行目] lat/long修正・Drive上書き保存完了")

def validate_latlong(fields, idx_lat, idx_long):
    """lat/longバリデーションの共通ロジック"""
    if idx_lat == -1 or idx_long == -1 or len(fields) <= max(idx_lat, idx_long):
        return True  # エラー
    if fields[idx_lat] == '' or fields[idx_long] == '':
        return True  # エラー
    try:
        float(fields[idx_lat])
        float(fields[idx_long])
        return False  # OK
    except Exception:
        return True  # エラー

def extract_administrative_ward(address):
    """addressから行政区を抽出する（政令指定都市の行政区のみ）"""
    # 東京都特別区を除外（一般的な特別区名をリストアップ）
    tokyo_special_wards = [
        '千代田区', '中央区', '港区', '新宿区', '文京区', '台東区', '墨田区', '江東区',
        '品川区', '目黒区', '大田区', '世田谷区', '渋谷区', '中野区', '杉並区', '豊島区',
        '北区', '荒川区', '板橋区', '練馬区', '足立区', '葛飾区', '江戸川区'
    ]
    
    # 区で終わる部分を抽出
    ward_pattern = r'([^市\s]+区)'
    
    match = re.search(ward_pattern, address)
    if match:
        ward = match.group(1)
        # 東京都特別区を除外
        if ward in tokyo_special_wards:
            return None
        return ward
    
    return None

def create_sort_key(row, idx_address, idx_number):
    """ソート用のキーを生成する"""
    if idx_number == -1:
        return ('', '')
    
    number = row[idx_number] if len(row) > idx_number else ''
    
    if idx_address == -1:
        return ('', parse_number_for_sort(number))
    
    address = row[idx_address] if len(row) > idx_address else ''
    
    # 行政区を抽出
    ward = extract_administrative_ward(address)
    if ward:
        return (ward, parse_number_for_sort(number))
    else:
        return ('', parse_number_for_sort(number))

def parse_number_for_sort(number):
    """numberをソート用にパースする（自然順ソート対応）"""
    if not number:
        return (0, 0, '')
    
    import re
    
    # 「第n投票区」形式の処理
    voting_match = re.search(r'第(\d+)投票区', number)
    if voting_match:
        n = int(voting_match.group(1))
        return (n, 0, '')
    
    # m*n形式（*は区切り文字）の場合（純粋な数値の組み合わせ）
    separators = ['-', '_', ':', '.', '/', '\\', '|', '~', '+', '=', ' ']
    
    for sep in separators:
        if sep in number:
            parts = number.split(sep, 1)  # 最初の区切り文字のみで分割
            try:
                m = int(parts[0])
                # 2番目の部分が空でない場合のみ整数変換を試行
                if len(parts) > 1 and parts[1].strip():
                    # 2番目の部分に区切り文字が含まれる場合は最初の数値のみを使用
                    second_part = parts[1]
                    for inner_sep in separators:
                        if inner_sep in second_part:
                            second_part = second_part.split(inner_sep)[0]
                            break
                    n = int(second_part)
                else:
                    n = 0
                return (m, n, '')
            except (ValueError, IndexError):
                continue
    
    # 数値＋文字列の混在パターンを抽出（例：「1番地」「10丁目」など）
    number_text_match = re.search(r'^(\d+)([^\d].*)', number)
    if number_text_match:
        num_part = int(number_text_match.group(1))
        text_part = number_text_match.group(2)
        
        # 区切り文字が含まれる場合の処理
        for sep in separators:
            if sep in text_part:
                # 区切り文字の後に数値があるかチェック
                parts = text_part.split(sep, 1)
                if len(parts) > 1:
                    after_sep = parts[1]
                    second_num_match = re.search(r'^(\d+)', after_sep)
                    if second_num_match:
                        second_num = int(second_num_match.group(1))
                        return (num_part, second_num, text_part)
                break
        
        return (num_part, 0, text_part)
    
    # 数値のみの場合
    try:
        return (int(number), 0, '')
    except ValueError:
        # 数値でない場合は文字列として扱う
        return (float('inf'), 0, number)

def create_final_normalized_csv(pref, city, folder_id, drive_service, row_num):
    """最終正規化CSVを作成する"""
    logger.info(f"[{row_num}行目] {pref}{city}: 最終正規化CSV作成開始")
    
    files = list_drive_files(drive_service, folder_id)
    base_csv_name = f"{city}_normalized.csv"
    final_csv_name = f"{city}_normalized_final.csv"
    
    # 既存の最終正規化CSVファイルを探す
    existing_final = next((f for f in files if f['name'] == final_csv_name), None)
    
    # CSVファイルを収集（基本ファイルとappendファイル）
    target_files = find_csv_files(files, city, drive_service)
    if not target_files:
        logger.error(f"[{row_num}行目] {pref}{city}: 正規化CSVファイルが見つかりません")
        return
    
    # 全データを格納するリスト
    all_data_rows = []
    header = None
    
    # 各CSVファイルを読み込んで結合
    for file_obj, csv_name in target_files:
        logger.info(f"[{row_num}行目] {csv_name} を読み込み中")
        content, file_id = download_csv_file(drive_service, folder_id, csv_name)
        if content is None:
            logger.error(f"[{row_num}行目] {csv_name} のダウンロードに失敗しました")
            continue
        
        # デコード処理
        try:
            if content.startswith(b'\xef\xbb\xbf'):
                decoded = content.decode('utf-8-sig')
            else:
                decoded = content.decode('utf-8')
        except Exception as e:
            logger.error(f"[{row_num}行目] {csv_name} のデコードに失敗しました: {e}")
            continue
        
        # CSV読み込み
        reader = csv.reader(io.StringIO(decoded))
        rows = list(reader)
        if not rows:
            continue
        
        # ヘッダーの確認
        if header is None:
            header = rows[0]
            if 'lat' not in header or 'long' not in header:
                logger.error(f"[{row_num}行目] lat/long列が見つかりません")
                return
        else:
            # ヘッダーの一貫性チェック
            if rows[0] != header:
                logger.warning(f"[{row_num}行目] {csv_name} のヘッダーが異なります")
        
        # データ行を追加
        all_data_rows.extend(rows[1:])
    
    if not all_data_rows:
        logger.error(f"[{row_num}行目] 有効なデータ行がありません")
        return
    
    # 列のインデックスを取得
    idx_lat = header.index('lat')
    idx_long = header.index('long')
    idx_number = header.index('number') if 'number' in header else -1
    idx_address = header.index('address') if 'address' in header else -1
    
    # lat/longが空でない行のみフィルタリング + 重複行検出
    filtered_rows = []
    duplicates_removed = 0
    seen_keys = {}  # キー: (number, lat, long), 値: 最初に出現した行のインデックス
    
    for i, row in enumerate(all_data_rows):
        if len(row) > max(idx_lat, idx_long):
            if row[idx_lat] != '' and row[idx_long] != '':
                # 重複チェック用のキーを作成
                if idx_number != -1 and len(row) > idx_number:
                    number_val = row[idx_number]
                    lat_val = row[idx_lat]
                    long_val = row[idx_long]
                    idx_name = header.index('name') if 'name' in header else -1
                    name_val = row[idx_name] if idx_name != -1 and len(row) > idx_name else ''
                    
                    # number, name, lat, longの4つとも空でない場合のみ重複チェック
                    if number_val != '' and name_val != '' and lat_val != '' and long_val != '':
                        dup_key = (number_val, name_val, lat_val, long_val)
                        
                        if dup_key in seen_keys:
                            # 重複行を検出
                            logger.warning(f"[{row_num}行目] {pref}{city}: 重複行を検出しました - number: {number_val}, name: {name_val}, lat: {lat_val}, long: {long_val} (今回: {i+1}行目)")
                            duplicates_removed += 1
                            # 既存の行を削除して最新の行に置き換え（最後に現れた行を採用）
                            prev_idx = seen_keys[dup_key]
                            filtered_rows[prev_idx] = None  # 削除マーク
                            seen_keys[dup_key] = len(filtered_rows)
                            filtered_rows.append(row)
                        else:
                            # 初めて出現したキーを記録
                            seen_keys[dup_key] = len(filtered_rows)
                            filtered_rows.append(row)
                    else:
                        # number/name/lat/longのいずれかが空の場合は重複チェックしない
                        filtered_rows.append(row)
                else:
                    # number列がない場合は重複チェックしない
                    filtered_rows.append(row)
    
    # Noneマークされた行を除去
    filtered_rows = [row for row in filtered_rows if row is not None]
    
    if duplicates_removed > 0:
        logger.info(f"[{row_num}行目] {pref}{city}: {duplicates_removed}件の重複行を除去しました")
    
    logger.info(f"[{row_num}行目] フィルタリング前: {len(all_data_rows)}行, フィルタリング後: {len(filtered_rows)}行")
    
    # 行政区とnumberでソート
    if idx_number != -1:
        filtered_rows.sort(key=lambda x: create_sort_key(x, idx_address, idx_number))
    
    # CSV生成
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(header)
    writer.writerows(filtered_rows)
    final_content = output.getvalue().encode('utf-8')
    
    # Google Driveにアップロード
    if existing_final:
        # 既存ファイルを上書き
        upload_csv_file(drive_service, existing_final['id'], final_content)
        logger.info(f"[{row_num}行目] {pref}{city}: 最終正規化CSV上書き完了 ({len(filtered_rows)}行)")
    else:
        # 新規ファイルを作成
        file_metadata = {
            'name': final_csv_name,
            'parents': [folder_id],
            'mimeType': 'text/csv'
        }
        media = MediaIoBaseUpload(io.BytesIO(final_content), mimetype='text/csv', resumable=True)
        try:
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True,
                fields='id'
            ).execute()
            logger.info(f"[{row_num}行目] {pref}{city}: 最終正規化CSV作成完了 ({len(filtered_rows)}行)")
        except Exception as e:
            logger.error(f"[{row_num}行目] 最終正規化CSVの作成に失敗しました: {e}")

def find_csv_files(files, city, drive_service):
    """CSVファイル検索の共通ロジック"""
    base_csv_name = f"{city}_normalized.csv"
    misspelled1 = f"{city}_nomalized.csv"   # l抜け
    misspelled2 = f"{city}_normarized.csv"   # l→r typo
    
    target_files = []
    
    # 1. 基本ファイル（{市区町村名}_normalized.csv）
    base_file = next((f for f in files if f['name'] == base_csv_name), None)
    if base_file:
        target_files.append((base_file, base_csv_name))
    
    # 2. appendファイル（{市区町村名}_normalized_*append.csv）
    import fnmatch
    append_pattern = f"{city}_normalized_*append.csv"
    append_files = [f for f in files if fnmatch.fnmatchcase(f['name'], append_pattern)]
    if append_files:
        logger.info(f"{city}: appendファイル {len(append_files)}件検出: {[f['name'] for f in append_files]}")
    for append_file in append_files:
        target_files.append((append_file, append_file['name']))
    
    # 3. 旧typoファイルの処理（基本ファイルが存在しない場合のみ）
    if not base_file:
        miss_file1 = next((f for f in files if f['name'] == misspelled1), None)
        miss_file2 = next((f for f in files if f['name'] == misspelled2), None)
        
        # ⑯: normarized誤記リネーム（競合ならリネームせず既存ファイル使う・エラーなし）
        if miss_file2:
            logger.info(f"{misspelled2} を {base_csv_name} にDrive上でリネームします")
            rename_drive_file(drive_service, miss_file2['id'], base_csv_name)
            time.sleep(1)
            target_files.append((miss_file2, base_csv_name))

        # ⑫: nomalized誤記リネーム（競合ならリネームせず既存ファイル使う・エラーなし）
        if miss_file1 and not miss_file2:
            logger.info(f"{misspelled1} を {base_csv_name} にDrive上でリネームします")
            rename_drive_file(drive_service, miss_file1['id'], base_csv_name)
            time.sleep(1)
            target_files.append((miss_file1, base_csv_name))

        # ⑮: 先頭余計な文字列リネーム（競合ならリネームせず既存ファイル使う・エラーなし）
        pat = re.compile(r"^.+?" + re.escape(base_csv_name) + r"$")
        bad_head_files = [f for f in files if pat.match(f['name']) and f['name'] != base_csv_name]
        for bad_file in bad_head_files:
            if not any(f[1] == base_csv_name for f in target_files):
                logger.info(f"{bad_file['name']} を {base_csv_name} にDrive上でリネームします")
                rename_drive_file(drive_service, bad_file['id'], base_csv_name)
                time.sleep(1)
                target_files.append((bad_file, base_csv_name))
                break
    
    return target_files

def process_csv_file(file_obj, csv_name, pref, city, row_num, drive_service, folder_id, opts):
    """単一CSVファイルの処理"""
    logger.info(f"[{row_num}行目] {csv_name} 処理開始")
    
    content, file_id = download_csv_file(drive_service, folder_id, csv_name)
    if content is None:
        logger.error(f"[{row_num}行目] エラー: {csv_name}がGoogle Drive内に存在しません")
        return

    encoding = detect_encoding(content)
    ok, has_bom, decoded = check_csv_content(content, pref, city, row_num)
    
    # ここでlat/longバリデーションを再チェックし、エラーなら修正処理
    reader = csv.reader(io.StringIO(decoded))
    all_rows = list(reader)
    if not all_rows:
        logger.error(f"[{row_num}行目] CSVが空です。")
        return
    
    header = all_rows[0]
    data_rows = all_rows[1:]
    idx_lat = header.index('lat') if 'lat' in header else -1
    idx_long = header.index('long') if 'long' in header else -1
    
    # 共通ロジックを使用
    latlong_error = any(validate_latlong(fields, idx_lat, idx_long) for fields in data_rows)

    # 追加: lat/longエラー行が全てnote=削除または不明かどうか判定
    all_latlong_error_rows_deleted_or_unknown = False
    if latlong_error and idx_long != -1 and idx_lat != -1 and header and 'note' in header:
        idx_note = header.index('note')
        error_rows = [fields for fields in data_rows if validate_latlong(fields, idx_lat, idx_long)]
        if error_rows and all((fields[idx_note] in ('削除', '不明')) for fields in error_rows if len(fields) > idx_note):
            all_latlong_error_rows_deleted_or_unknown = True

    if latlong_error:
        if opts.check_only:
            # SKIP_LATLONG_UPDATE_LISTに含まれる自治体の場合はWARNINGを出力しない
            if (pref, city) not in SKIP_LATLONG_UPDATE_LIST:
                if all_latlong_error_rows_deleted_or_unknown:
                    logger.info(f"[{row_num}行目] {csv_name}: lat/longエラーを検出しました（チェックのみモードのため修正・保存は行いません） ※全てnote=削除または不明")
                else:
                    logger.warning(f"[{row_num}行目] {csv_name}: lat/longエラーを検出しました（チェックのみモードのため修正・保存は行いません）")
            return
        else:
            logger.warning(f"[{row_num}行目] {csv_name}: lat/longエラーを自動修正し、CSVを上書き保存しました")
            fix_and_upload_csv(drive_service, file_id, decoded, header, data_rows, pref, city, row_num)
            return

    if decoded is None:
        logger.error(f"[{row_num}行目] CSV内容の解析に失敗しました")
        return
    
    if has_bom or (encoding and encoding.lower() in ('shift_jis', 'cp932', 'ms932', 'sjis')):
        cleaned_bytes = decoded.encode('utf-8')
        upload_csv_file(drive_service, file_id, cleaned_bytes)
        logger.info(f"[{row_num}行目] {csv_name}: BOM有/Shift-JIS系CSVをBOM無しUTF-8でDrive上書きしました")
    else:
        if encoding is None or encoding.lower() not in ('utf-8', 'ascii'):
            logger.error(f"[{row_num}行目] エンコード不正: {encoding}")
            return

    if ok:
        logger.info(f"[{row_num}行目] {csv_name}: OK")
    time.sleep(0.1)

def process_single_target(target, drive_service, opts, counters):
    """単一ターゲット（自治体）の処理"""
    row_num, pref, city, folder_id = target
    
    # 最終正規化CSVモードの場合
    if opts.final_mode:
        create_final_normalized_csv(pref, city, folder_id, drive_service, row_num)
        return
    
    # 削除希望ファイルの検出と処理
    deletion_targets = find_deletion_targets(drive_service, folder_id)
    if deletion_targets:
        counters['deletion_target_count'] += len(deletion_targets)
        if opts.delete_mode:
            logger.info(f"[{row_num}行目] {pref}{city}: 削除希望ファイル {len(deletion_targets)}件を削除します")
            for target_file in deletion_targets:
                if delete_drive_file(drive_service, target_file['id'], target_file['name']):
                    counters['deletion_success_count'] += 1
        else:
            logger.info(f"[{row_num}行目] {pref}{city}: 削除希望ファイル {len(deletion_targets)}件を検出しました: {[f['name'] for f in deletion_targets]}")
    
    files = list_drive_files(drive_service, folder_id)
    target_files = find_csv_files(files, city, drive_service)
    
    # 日時チェック機能が有効な場合
    if opts.last_updated:
        # 基本ファイルの日時チェック（従来の動作を維持）
        base_csv_name = f"{city}_normalized.csv"
        misspelled1 = f"{city}_nomalized.csv"
        misspelled2 = f"{city}_normarized.csv"
        
        correct_file = next((f for f in files if f['name'] == base_csv_name), None)
        if not correct_file:
            correct_file = next((f for f in files if f['name'] == misspelled1), None)
        if not correct_file:
            correct_file = next((f for f in files if f['name'] == misspelled2), None)
        
        if correct_file:
            file_modified_time = get_file_modified_time(drive_service, correct_file['id'])
            if file_modified_time and file_modified_time < opts.last_updated:
                logger.info(f"スキップ: [{row_num}行目] {base_csv_name} の最終更新日時({file_modified_time.strftime('%Y-%m-%d %H:%M:%S')})が指定日時({opts.last_updated.astimezone(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')})より古いため")
                # 基本ファイルが古い場合でも、appendファイルは個別にチェックするためreturnしない
                # 基本ファイルをtarget_filesから除外し、スキップカウントを増やす
                target_files = [(f, n) for f, n in target_files if n != base_csv_name]
                counters['skipped_count'] += 1
    
    # 各CSVファイルを処理
    for file_obj, csv_name in target_files:
        # 日時チェック機能が有効な場合、個別ファイルの日時チェック
        if opts.last_updated:
            if should_skip_file_by_time(csv_name, file_obj['id'], drive_service, opts.last_updated):
                counters['skipped_count'] += 1
                continue
        process_csv_file(file_obj, csv_name, pref, city, row_num, drive_service, folder_id, opts)

def should_skip_by_time(pref, city, folder_id, drive_service, last_updated):
    """日時チェックによるスキップ判定"""
    files = list_drive_files(drive_service, folder_id)
    base_csv_name = f"{city}_normalized.csv"
    misspelled1 = f"{city}_nomalized.csv"
    misspelled2 = f"{city}_normarized.csv"
    
    # 正しいファイル名のファイルを探す
    correct_file = next((f for f in files if f['name'] == base_csv_name), None)
    if not correct_file:
        # typoファイルもチェック
        correct_file = next((f for f in files if f['name'] == misspelled1), None)
    if not correct_file:
        correct_file = next((f for f in files if f['name'] == misspelled2), None)
    
    if correct_file:
        file_modified_time = get_file_modified_time(drive_service, correct_file['id'])
        if file_modified_time and file_modified_time < last_updated:
            # ログ出力時は日本時間で表示
            logger.info(f"スキップ: {base_csv_name} の最終更新日時({file_modified_time.strftime('%Y-%m-%d %H:%M:%S')})が指定日時({last_updated.astimezone(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')})より古いため")
            return True
    else:
        logger.warning(f"ファイル {base_csv_name} が見つからないため日時チェックをスキップ")
    
    return False

def should_skip_file_by_time(file_name, file_id, drive_service, last_updated):
    """個別ファイルの日時チェックによるスキップ判定"""
    file_modified_time = get_file_modified_time(drive_service, file_id)
    if file_modified_time and file_modified_time < last_updated:
        # ログ出力時は日本時間で表示
        logger.info(f"スキップ: {file_name} の最終更新日時({file_modified_time.strftime('%Y-%m-%d %H:%M:%S')})が指定日時({last_updated.astimezone(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')})より古いため")
        return True
    return False

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

def calculate_folder_size(service, folder_id):
    """フォルダ内の全ファイルのサイズを計算"""
    total_size = 0
    files = []
    page_token = None
    
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, size)',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token
        ).execute()
        
        for file in response.get('files', []):
            if file['mimeType'] != 'application/vnd.google-apps.folder':  # フォルダ以外
                if 'size' in file:
                    total_size += int(file['size'])
                files.append(file)
        
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break
    
    return total_size, files

def process_size_calculation(targets, drive_service):
    """サイズ計算モードの処理"""
    logger.info("=== フォルダサイズ計算開始 ===")
    
    total_size_all = 0
    folder_sizes = []
    
    for row_num, pref, city, folder_id in targets:
        try:
            folder_size, files = calculate_folder_size(drive_service, folder_id)
            total_size_all += folder_size
            
            folder_sizes.append({
                'row_num': row_num,
                'pref': pref,
                'city': city,
                'size': folder_size,
                'file_count': len(files)
            })
            
            logger.info(f"[{row_num}行目] {pref}{city}: {format_size(folder_size)} ({len(files)}ファイル)")
            
        except Exception as e:
            logger.error(f"[{row_num}行目] {pref}{city}: サイズ計算エラー - {e}")
    
    # 結果の表示
    logger.info("=== サイズ計算結果 ===")
    logger.info(f"対象自治体数: {len(folder_sizes)}件")
    logger.info(f"合計サイズ: {format_size(total_size_all)}")
    
    # サイズ順でソートして上位10件を表示
    sorted_sizes = sorted(folder_sizes, key=lambda x: x['size'], reverse=True)
    logger.info("=== サイズ上位10件 ===")
    for i, folder in enumerate(sorted_sizes[:10], 1):
        logger.info(f"{i:2d}. [{folder['row_num']}行目] {folder['pref']}{folder['city']}: {format_size(folder['size'])} ({folder['file_count']}ファイル)")
    
    return total_size_all

def setup_logger_counters():
    """ログカウンターの設定"""
    counters = {
        'total_count': 0,
        'error_count': 0,
        'warning_count': 0,
        'skipped_count': 0,
        'deletion_target_count': 0,
        'deletion_success_count': 0
    }
    
    # loggerのラップ
    orig_error = logger.error
    orig_warning = logger.warning
    def count_error(msg, *args, **kwargs):
        counters['error_count'] += 1
        orig_error(msg, *args, **kwargs)
    def count_warning(msg, *args, **kwargs):
        counters['warning_count'] += 1
        orig_warning(msg, *args, **kwargs)
    logger.error = count_error
    logger.warning = count_warning
    
    return counters

def get_targets(gc, opts):
    """ターゲットの取得"""
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1j5DBYCVMPWREjWjr5767o_B2S0fN8jLbMgM7XWc0u7w/edit?gid=592197778#gid=592197778"
    logger.info("スプレッドシート取得中...")
    rows, idx_pref, idx_city, idx_has_csv, idx_folder = get_spreadsheet_rows(gc, SPREADSHEET_URL)
    logger.info(f"データ件数: {len(rows)}")

    targets = filter_rows(rows, idx_pref, idx_city, idx_has_csv, idx_folder, opts.args)
    logger.info(f"チェック対象件数: {len(targets)}")
    return targets

def main():
    parser = argparse.ArgumentParser(description='Googleスプシ参照・CSV妥当性チェック')
    parser.add_argument('args', nargs='*', help='[都道府県] [市区町村]')
    parser.add_argument('-lu', '--last-updated', type=parse_datetime_arg, 
                       help='指定日時以降に更新されたファイルのみ処理 (形式: YYYYMMDD または YYYYMMDDHHMM)')
    parser.add_argument('-u', '--update', action='store_true', help='CSVの上書き保存を行う（指定しない場合はチェックのみ）')
    parser.add_argument('-d', '--delete', action='store_true', help='削除希望ファイルを削除する')
    parser.add_argument('-f', '--final', action='store_true', help='最終正規化CSV作成モード')
    parser.add_argument('-s', '--size', action='store_true', help='フォルダサイズ計算モード')
    opts = parser.parse_args()
    
    # チェックのみモードの論理を反転
    opts.check_only = not opts.update
    # 削除モードの設定
    opts.delete_mode = opts.delete
    # 最終正規化CSVモードの設定
    opts.final_mode = opts.final
    # サイズ計算モードの設定
    opts.size_mode = opts.size

    # ログ設定（ファイルオープンエラーハンドリング付き）
    try:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            handlers=[
                logging.FileHandler(LOGFILE, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        logger = logging.getLogger(__name__)
    except Exception as e:
        # ログファイルがオープンできない場合は、コンソール出力のみでログ設定
        print(f"警告: ログファイル '{LOGFILE}' をオープンできませんでした: {e}")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        logger = logging.getLogger(__name__)
        logger.error(f"ログファイル '{LOGFILE}' をオープンできませんでした: {e}")

    # コマンドライン引数をログ出力
    args_str = ' '.join(opts.args) if opts.args else '(全件)'
    if opts.last_updated:
        # UTC時刻を日本時間（JST）に変換して表示
        jst_time = opts.last_updated.astimezone(timezone(timedelta(hours=9)))
        last_updated_str = jst_time.strftime('%Y%m%d%H%M')
    else:
        last_updated_str = '(指定なし)'
    mode_str = '上書きモード' if opts.update else 'チェックのみモード'
    delete_str = '削除モード' if opts.delete_mode else '削除なし'
    final_str = '最終正規化CSV作成モード' if opts.final_mode else ''
    size_str = 'サイズ計算モード' if opts.size_mode else ''
    logger.info(f"=== 実行開始 ===")
    logger.info(f"コマンドライン引数: {args_str}")
    logger.info(f"最終更新日時: {last_updated_str}")
    logger.info(f"実行モード: {mode_str}")
    logger.info(f"削除モード: {delete_str}")
    if opts.final_mode:
        logger.info(f"モード: {final_str}")
    if opts.size_mode:
        logger.info(f"モード: {size_str}")

    creds = get_credentials()
    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)

    counters = setup_logger_counters()
    targets = get_targets(gc, opts)

    # サイズ計算モードの場合
    if opts.size_mode:
        process_size_calculation(targets, drive_service)
        return

    for target in targets:
        counters['total_count'] += 1
        process_single_target(target, drive_service, opts, counters)

    logger.info("全チェック終了")
    logger.info(f"処理自治体数: {counters['total_count']}件, エラー件数: {counters['error_count']}件, ワーニング件数: {counters['warning_count']}件, スキップ件数: {counters['skipped_count']}件")
    if counters['deletion_target_count'] > 0:
        logger.info(f"削除対象件数: {counters['deletion_target_count']}件, 削除成功件数: {counters['deletion_success_count']}件")

if __name__ == "__main__":
    main()
