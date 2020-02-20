import sys
import signal
import argparse
from edman import DB
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='ドキュメントを削除するスクリプト')
# parser.add_argument('-c', '--collection', help='collection name.')
parser.add_argument('objectid', help='objectid str.')
parser.add_argument('-i', '--inifile', help='DB connect file path.')
args = parser.parse_args()

# iniファイル読み込み
con = Action.reading_config_file(args.inifile)

db = DB(con)
# 対象oidの所属コレクションを自動的に取得 ※動作が遅い場合は使用しないこと
collection = db.find_collection_from_objectid(args.objectid)

# 指定のドキュメントの文書構造を取得
structure = db.get_structure(collection, args.objectid)

# 削除処理
result = db.delete(args.objectid, collection, structure)

if result:
    print('削除成功')
else:
    print('削除失敗')
