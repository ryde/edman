import sys
import signal
import configparser
import argparse
from pathlib import Path
from edman.db import DB
from edman.file import File
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='ファイルを実験データに追加するスクリプト')
# parser.add_argument('-c', '--collection', help='collection name.')
# parser.add_argument('-o', '--objectid', help='objectid str.')
parser.add_argument('objectid', help='objectid str.')
parser.add_argument('path', help='file or Dir path.')
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

db = DB(con)
file = File(db)

# 対象oidの所属コレクションを自動的に取得 ※動作が遅い場合は使用しないこと
collection = db.find_collection_from_objectid(args.objectid)

if file.add_file_reference(collection, args.objectid,
                           Action.files_read(args.path),
                           args.structure, query):
    print('更新しました')
else:
    print('更新に失敗しました')
