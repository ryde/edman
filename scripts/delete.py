import sys
import signal
import configparser
import argparse
from pathlib import Path
from edman.db import DB
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='ドキュメントの項目を削除するスクリプト')
# parser.add_argument('-c', '--collection', help='collection name.')
# parser.add_argument('-o', '--objectid', help='objectid str.')
parser.add_argument('objectid', help='objectid str.')
# クエリは structureがembの時だけ
parser.add_argument('-q', '--query', default=None,
                    help='Ref is ObjectId or Emb is query list strings.')
parser.add_argument('-s', '--structure', default='ref',
                    help='Select ref(Reference, default) or emb(embedded).')
args = parser.parse_args()

# クエリの変換
query = Action.file_query_eval(args.query, args.structure)

# iniファイル読み込み
settings = configparser.ConfigParser()
settings.read(Path.cwd() / 'ini' / 'db.ini')
con = dict([i for i in settings['DB'].items()])

db = DB()
edman = db.connect(**con)

# 対象oidの所属コレクションを自動的に取得 ※動作が遅い場合は使用しないこと
collection = db.find_collection_from_objectid(args.objectid)

# ドキュメント取得
doc = db.doc(collection, args.objectid, query)
doc_keys = list(doc.keys())

# 項目を画面表示
for idx, (key, value) in enumerate(doc.items()):
    print('(' + str(idx) + ')', key, ':', value)

# 表示されている選択番号を入力
if len(doc) > 0:
    while True:
        selected_idx = input('0 - ' + str(len(doc) - 1) + ' > ')
        if selected_idx.isdecimal() and (
                0 <= int(selected_idx) < len(doc)):
            break
        else:
            print('Required!')
else:
    sys.exit('ドキュメントが取得できていません')

# 削除処理
result = db.item_delete(collection, args.objectid, doc_keys[int(selected_idx)],
                        query)
if result:
    print('削除成功')
else:
    print('削除失敗')
