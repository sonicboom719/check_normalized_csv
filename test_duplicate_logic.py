# 福岡市の西新第一データで新しい重複除去ロジックをテスト
import csv
import io

# テストデータ（nameも含めて同じ）
test_data = [
    ['福岡県', '福岡市', '西新第一-1', '城南区早良区西新六丁目4番1号', '西新小学校東側塀', '33.586474', '130.3549308', ''],
    ['福岡県', '福岡市', '西新第一-2', '城南区早良区西新六丁目2番92号', '西南学院大学西側塀北端', '33.5845855', '130.3536489', ''],
    ['福岡県', '福岡市', '西新第一-3', '城南区早良区西新六丁目1番10号', '修猷館高等学校南側塀西端', '33.5838873', '130.3563736', ''],
    ['福岡県', '福岡市', '西新第一-4', '城南区早良区西新六丁目1番10号', '修猷館高等学校南側塀東端', '33.5838873', '130.3563736', ''],
    ['福岡県', '福岡市', '西新第一-1', '早良区西新六丁目4番1号', '西新小学校東側塀', '33.586474', '130.3549308', ''],
    ['福岡県', '福岡市', '西新第一-2', '早良区西新六丁目2番92号', '西南学院大学西側塀北端', '33.5845855', '130.3536489', ''],
    ['福岡県', '福岡市', '西新第一-3', '早良区西新六丁目1番10号', '修猷館高等学校南側塀西端', '33.5838873', '130.3563736', ''],
    ['福岡県', '福岡市', '西新第一-4', '早良区西新六丁目1番10号', '修猷館高等学校南側塀東端', '33.5838873', '130.3563736', ''],
]

# 新しい重複除去ロジック（number, name, lat, long）
filtered_rows = []
seen_keys = {}
duplicates_removed = 0

for i, row in enumerate(test_data):
    number_val = row[2]
    name_val = row[4]
    lat_val = row[5]
    long_val = row[6]
    
    if number_val != '' and name_val != '' and lat_val != '' and long_val != '':
        dup_key = (number_val, name_val, lat_val, long_val)
        
        if dup_key in seen_keys:
            print(f'重複検出: {number_val}, {name_val} (前回: 行{seen_keys[dup_key]+1}, 今回: 行{i+1})')
            duplicates_removed += 1
            # 最後の行を採用
            prev_idx = seen_keys[dup_key]
            filtered_rows[prev_idx] = None
            seen_keys[dup_key] = len(filtered_rows)
            filtered_rows.append(row)
        else:
            seen_keys[dup_key] = len(filtered_rows)
            filtered_rows.append(row)

# Noneを除去
filtered_rows = [row for row in filtered_rows if row is not None]

print(f'\n重複除去後: {len(filtered_rows)}行')
for row in filtered_rows:
    print(f'  {row[2]}: {row[3][:20]}... ({row[4]})')

# 西新第一-3と-4が同じ緯度経度を持つことを確認
print('\n西新第一-3と-4の比較:')
for row in filtered_rows:
    if '西新第一-3' in row[2] or '西新第一-4' in row[2]:
        print(f'  {row[2]}: name={row[4]}, lat={row[5]}, long={row[6]}')