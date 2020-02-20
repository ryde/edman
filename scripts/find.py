import sys
import signal
import argparse
from edman import DB, JsonManager, Search
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='DBから検索して結果をJSONファイルにするスクリプト')
parser.add_argument('collection')
# コマンドもしくはファイルでの検索文字列を選択
group = parser.add_mutually_exclusive_group()
group.add_argument('-q', '--query')
group.add_argument('-f', '--query_file', type=open)
parser.add_argument('-p', '--parent_depth', type=int, default=0)
parser.add_argument('-c', '--child_depth', type=int, default=0)
parser.add_argument('-o', '--out_file_name',
                    help='generate json, Name output file.',
                    default='search_result')
parser.add_argument('-d', '--dir', help='generate json file, dir path',
                    default='.')
parser.add_argument('-i', '--inifile', help='DB connect file path.')
args = parser.parse_args()
# クエリおよびクエリファイルはどちらかは必須
if not args.query and not args.query_file:
    parser.error("query or query_file is mandatory.")

# クエリ入力値変換
query = Action.query_eval(args.query if args.query else args.query_file.read())

# iniファイル読み込み
con = Action.reading_config_file(args.inifile)

db = DB(con)
search = Search(db)

# 検索
search_result = search.find(args.collection, query, args.parent_depth,
                            args.child_depth)

# 検索結果をjsonファイルとして保存
jm = JsonManager()
jm.save(search_result, args.dir, name=args.out_file_name, date=True)
