import sys
import signal
import argparse
from edman import DB
# from edman import DB, JsonManager
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(
    description='特定コレクション内のembデータの特定のキーに対してref化を行うスクリプト')
parser.add_argument('collection')
parser.add_argument('pullout_key')
parser.add_argument('-e', '--exclusion_keys', nargs='*')
parser.add_argument('-i', '--inifile', help='DB connect file path.')
# parser.add_argument('-d', '--dir',
#                     help='Dir of report files.',
#                     default=None)
args = parser.parse_args()

# 結果を記録する場合はパスの存在を調べる
# if args.dir is not None:
#     p = Path(args.dir)
#     if not p.exists() and not p.is_dir():
#         sys.exit('パスが不正です')

# iniファイル読み込み
con = Action.reading_config_file(args.inifile)

db = DB(con)
exclusion = tuple(args.exclusion_keys if args.exclusion_keys is not None else [])
result = db.loop_exclusion_key_and_ref(args.collection, args.pullout_key, exclusion)

# 結果を保存する
# if args.dir is not None:
#     jm = JsonManager()
#     jm.save(result, args.dir, 'pullout', date=True)

