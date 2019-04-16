"""
新しいユーザ権限のDBを作成
"""
import sys
import signal
import getpass
from pathlib import Path
from collections import OrderedDict
from edman.db import DB
from action import Action

# Ctrl-Cを押下された時の対策
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit('\n'))

# 管理者アカウント入力
admin_text = OrderedDict()
admin_text["MongoDB's Admin DB >> "] = 'dbname'
admin_text["MongoDB's Admin name >> "] = 'name'
admin_text["MongoDB's Admin password >> "] = 'pwd'
admin_account = {}
for key, value in admin_text.items():
    while True:
        # パスワードの時は入力値を非表示
        buff = getpass.getpass(key) if 'pwd' in value else input(key)
        if not buff:
            print('Required!')
        else:
            admin_account[value] = buff
            break

# ユーザアカウント入力
user_text = OrderedDict()
user_text["MongoDB's create user DB >> "] = 'dbname'
user_text["MongoDB's create user name >> "] = 'name'
user_text["MongoDB's create user password >> "] = 'pwd'
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

# iniセーブパス入力
while True:
    ini_input = input("db.ini save path >> ")
    p = Path(ini_input)
    if not ini_input:
        print('Required!')
    elif not p.exists():
        print('path is invalid')
    else:
        ini_dir = p.resolve()
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
print('[admin]')
for key, value in admin_account.items():
    print(key + ' : ' + '*' * len(
        value) if key == 'pwd' else key + ' : ' + value)

print('\n[user]')
for key, value in user_account.items():
    print(key + ' : ' + '*' * len(
        value) if key == 'pwd' else key + ' : ' + value)

# 最終確認
print(f"""
host : {host}
port : {port}
ini dir : {ini_dir}
""")
while True:
    confirm = input('OK? y/n(exit) >>')
    if confirm == 'n':
        sys.exit('exit')
    elif confirm == 'y':
        break
    else:
        continue

# DB作成
db = DB()
db.create(admin_account, user_account, ini_dir, host, port)
# Action.create(admin_account, user_account, ini_dir, host, port)

# テスト用のためにここに置く
# db.destroy(user_account, host, port, admin=admin_account, del_user=True)
