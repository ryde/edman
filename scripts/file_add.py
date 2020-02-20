import sys
import signal
import argparse
from edman import DB, File
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='ファイルを実験データに追加するスクリプト')
# parser.add_argument('-c', '--collection', help='collection name.')
parser.add_argument('objectid', help='objectid str.')
parser.add_argument('path', help='file or Dir path.')
# クエリは structureがembの時だけ
parser.add_argument('-q', '--query', default=None,
                    help='Ref is ObjectId or Emb is query list strings.')
parser.add_argument('-c', '--compress', action='store_true',
                    help='gzip compress.Default is not compressed.')
parser.add_argument('-i', '--inifile', help='DB connect file path.')
args = parser.parse_args()

# iniファイル読み込み
con = Action.reading_config_file(args.inifile)

db = DB(con)
file = File(db.get_db)

# 対象oidの所属コレクションを自動的に取得 ※動作が遅い場合は使用しないこと
collection = db.find_collection_from_objectid(args.objectid)

# ドキュメント構造の取得
structure = db.get_structure(collection, args.objectid)

# クエリの変換
query = Action.file_query_eval(args.query, structure)

if file.add_file_reference(collection, args.objectid,
                           Action.files_read(args.path), structure, query,
                           args.compress):
    print('更新しました')
else:
    print('更新に失敗しました')
