import sys
import signal
import configparser
import argparse
from pathlib import Path
from edman import DB

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='DBから検索したデータをコンバートしてDBに入れるスクリプト')
# parser.add_argument('-c', '--collection', help='collection name.')
parser.add_argument('objectid', help='objectid str.')
# parser.add_argument('-s', '--structure', default='ref',
#                     help='Select ref(Reference, default) or emb(embedded).')
parser.add_argument('new_collection', help='new collection name.')
args = parser.parse_args()

# iniファイル読み込み
settings = configparser.ConfigParser()
settings.read(Path.cwd() / 'ini' / 'db.ini')
con = dict([i for i in settings['DB'].items()])

db = DB(con)
# 対象oidの所属コレクションを自動的に取得 ※動作が遅い場合は使用しないこと
collection = db.find_collection_from_objectid(args.objectid)

# 対象のドキュメントがrefかembかを調べる
# (ただし、子要素が存在しないドキュメントの場合は必ずembと表示される)
current_structure = db.get_structure(collection, args.objectid)
print(f'このドキュメントは {current_structure} 形式です')
structures = ['emb', 'ref']
structures.remove(current_structure)

while True:
    convert_selected = input(f'{structures[0]}に変更しますか？ y/n(exit) >> ')
    if convert_selected == 'y':
        result = db.structure(collection, args.objectid, structures[0],
                              args.new_collection)
        print('\n', result)
        break
    elif convert_selected == 'n':
        sys.exit()
    else:
        continue
