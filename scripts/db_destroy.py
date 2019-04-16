"""
DBと認証ユーザを削除する
"""
import sys
import signal
import getpass
import argparse
from collections import OrderedDict
# from edman.db import DB
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# コマンドライン引数処理
parser = argparse.ArgumentParser(description='DB及びDB管理ユーザ削除スクリプト')
parser.add_argument('-ru', '--remove_user', help='Remove DB user.',
                    action='store_true')
args = parser.parse_args()

# 管理者アカウント入力
if args.remove_user:
    del_user = True
    admin_account = {}
    admin_text = OrderedDict()
    admin_text["MongoDB's Admin DB >> "] = 'dbname'
    admin_text["MongoDB's Admin name >> "] = 'name'
    admin_text["MongoDB's Admin password >> "] = 'pwd'
    for key, value in admin_text.items():
        while True:
            # パスワードの時は入力値を非表示
            buff = getpass.getpass(key) if 'pwd' in value else input(key)
            if not buff:
                print('Required!')
            else:
                admin_account[value] = buff
                break
else:
    del_user = False
    admin_account = None

# ユーザアカウント入力
user_text = OrderedDict()
user_text["destroy target DB >> "] = 'dbname'
user_text["destroy DB user name >> "] = 'name'
user_text["destroy DB user password >> "] = 'pwd'
user_account = {}
for key, value in user_text.items():
    while True:
        # パスワードの時は入力値を非表示
        buff = getpass.getpass(key) if 'pwd' in value else input(key)
        if not buff:
            print('Required!')
        else:
            user_account[value] = buff
            break

# ホスト名入力(デフォルト設定あり)
host = input(
    "MongoDB's host (Enter to skip, set 127.0.0.1) >> ") or '127.0.0.1'

# ポート入力(デフォルト設定あり)
while True:
    port = input("MongoDB's port (Enter to skip, set 27017) >> ")
    if len(port) == 0:
        port = 27017
        break
    elif not port.isdigit():
        print('input port(number)')
    elif len(port) > 5:
        print('input port(Max 5 digits)')
    else:
        port = int(port)
        break

# 入力値出力
if args.remove_user and admin_account is not None:
    print('[admin]')
    for key, value in admin_account.items():
        print(key + ' : ' + '*' * len(
            value) if key == 'pwd' else key + ' : ' + value)

print('\n[user]')
for key, value in user_account.items():
    print(key + ' : ' + '*' * len(
        value) if key == 'pwd' else key + ' : ' + value)

print(f"""
host : {host}
port : {port}
""")

# 最終確認
print(user_account['dbname'], 'is Erase all data.')
while True:
    confirm = input('OK? y/n(exit) >>')
    if confirm == 'n':
        sys.exit('exit')
    elif confirm == 'y':
        break
    else:
        continue

# DBの削除
# db = DB()
# db.destroy(user_account, host, port, admin=admin_account, del_user=del_user)
Action.destroy(user_account, host, port, admin=admin_account, del_user=del_user)
