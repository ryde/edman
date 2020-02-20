import sys
import signal
import argparse
import json
from edman import DB
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='ドキュメントの項目を修正するスクリプト')
# parser.add_argument('-c', '--collection', help='collection name.')
parser.add_argument('objectid', help='objectid str.')
parser.add_argument('amend_file', type=open, help='JSON file.')
parser.add_argument('structure', help='Select ref or emb.')
parser.add_argument('-i', '--inifile', help='DB connect file path.')
args = parser.parse_args()
# 構造はrefかembのどちらか
if not (args.structure == 'ref' or args.structure == 'emb'):
    parser.error("structure requires 'ref' or 'emb'.")

# iniファイル読み込み
con = Action.reading_config_file(args.inifile)

# ファイル読み込み
try:
    amend_data = json.load(args.amend_file)
except json.JSONDecodeError:
    sys.exit(f'File is not json format.')
except IOError:
    sys.exit('file read error.')

#  DB接続
db = DB(con)

# 対象oidの所属コレクションを自動的に取得 ※動作が遅い場合は使用しないこと
collection = db.find_collection_from_objectid(args.objectid)

# アップデート処理
result = db.update(collection, args.objectid, amend_data, args.structure)
if result:
    print('アップデート成功')
else:
    print('アップデート失敗')
