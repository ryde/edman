import sys
import signal
import argparse
import configparser
from pathlib import Path
from edman2.db import DB
from edman2.json_manager import JsonManager
from edman2.convert import Convert
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# iniファイル読み込み
settings = configparser.ConfigParser()
settings.read(Path.cwd() / 'ini' / 'db.ini')
con = dict([i for i in settings['DB'].items()])

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='JSONからDBに投入するスクリプト')
parser.add_argument('path', help='file or Dir path.')
parser.add_argument('-rd', '--result_dir',
                    help='Dir of report files. default is current dir.',
                    default='.')
parser.add_argument('-s', '--structure', default='ref',
                    help='Select ref(Reference, default) or emb(embedded).')
args = parser.parse_args()
# 構造はrefかembのどちらか ※モジュール内でも判断できる
if not (args.structure == 'ref' or args.structure == 'emb'):
    parser.error("--structure requires 'ref' or 'emb'.")

db = DB()
db.connect(**con)  # DB接続
jm = JsonManager()
convert = Convert()
json_files = Action.files_read(args.path, 'json')

for file in Action.file_gen(json_files):
    # Ednman用にjsonをコンバート
    converted_edman = convert.dict_to_edman(file, mode=args.structure)
    # コンバート結果を保存する場合
    # jm.save({'converted_edman': converted_edman}, args.result_dir,
    #         name='edman_json_list', date=True)

    # DBへインサート
    inserted_report = db.insert(converted_edman)
    jm.save({'inserted_report': inserted_report}, args.result_dir,
            name='inserted', date=True)
