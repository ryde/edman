import configparser
import random
import string
import copy
from pathlib import Path
from unittest import TestCase
from pymongo import errors, MongoClient
from edman import DB
from edman.exceptions import EdmanDbConnectError


class TestMultiUser(TestCase):
    db_server_connect = False
    test_ini = []
    client = None
    connections = []

    @classmethod
    def setUpClass(cls):
        # 設定読み込み
        settings = configparser.ConfigParser()
        settings.read(Path.cwd() / 'ini' / 'test_db.ini')
        cls.test_ini = dict(settings.items('DB'))
        port: int  = int(cls.test_ini['port'])
        cls.test_ini['port'] = port

        # DB作成のため、pymongoから接続
        cls.client = MongoClient(cls.test_ini['host'], cls.test_ini['port'])

        # 接続確認
        try:
            cls.client.admin.command('hello')
            cls.db_server_connect = True
            print('Use DB.')
        except errors.ConnectionFailure:
            print('Do not use DB.')

        # 作成するユーザ及びDBの数
        users_count = 4
        test_account = []

        # 人数分のユーザ名とパスワードをランダムで作成
        for i in range(users_count):
            tmp = string.digits + string.ascii_lowercase + string.ascii_uppercase
            tmp_name = ''.join([random.choice(tmp) for _ in range(8)])
            tmp_pass = ''.join([random.choice(tmp) for _ in range(8)])
            test_account.append(
                {
                    'username': tmp_name,
                    'password': tmp_pass,
                    'db_name': tmp_pass,
                    'db_auth': tmp_pass,
                }
            )

        if cls.db_server_connect:
            # adminで認証
            cls.client = MongoClient(
                username=cls.test_ini['admin_user'],
                password=cls.test_ini['admin_password'])

            connections = []
            for account in test_account:
                # DB作成
                cls.client[account['db_name']].command(
                    "createUser",
                    account['username'],
                    pwd=account['password'],
                    roles=[
                        {
                            'role': 'dbOwner',
                            'db': account['db_name'],
                        },
                    ],
                )

                # edmanのDB接続オブジェクト作成
                con = {
                    'host': cls.test_ini['host'],
                    'port': cls.test_ini['port'],
                    'user': account['username'],
                    'password': account['password'],
                    'database': account['db_name'],
                    'options': [f"authSource={account['db_auth']}"]
                }
                connections.append(con)
            cls.connections = connections

    @classmethod
    def tearDownClass(cls):
        # アカウント情報をもとにDBとユーザを削除
        if cls.db_server_connect:
            for connection in cls.connections:
                # cls.clientはpymongo経由でDB削除
                cls.client.drop_database(connection['database'])
                # cls.testdb.dbはedman側の接続オブジェクト経由でユーザ(自分自身)の削除
                db = DB(connection)
                testdb = db.get_db
                testdb.command("dropUser", connection['user'])

    def test_admin_insert(self):
        if not self.db_server_connect:
            return

        # 管理者権限で各ユーザのDBに入力
        connections = copy.deepcopy(self.connections)
        test_results = {}
        for connection in connections:
            connection['options'] = ['authSource=admin']
            connection['user'] = 'admin'
            connection['password'] = 'admin'
            db = DB(connection)
            insert_data = [
                {'test_admin_insert': {'database': connection['database']}}
            ]
            result = db.insert(insert_data)
            result_oid = result[0]['test_admin_insert'][0]
            # print(result_oid)  # debug
            test_results.update({connection['database']: result_oid})
        # print(test_results)  # debug

        # 各ユーザがDBからデータを取り出してテスト
        for connection in self.connections:
            db = DB(connection)
            result_doc = db.doc('test_admin_insert',
                                test_results[connection['database']],
                                None)
            # print(result_doc['database'], connection['database'])  # debug
            with self.subTest(connection=connection):
                self.assertEqual(result_doc['database'],
                                 connection['database'])

    def test_auth(self):
        if not self.db_server_connect:
            return

        # 自分のアカウントを利用して、他人のデータベースに接続する(失敗になる)

        dbnames = [connection['database'] for connection in self.connections]

        for connection in self.connections:
            # 自分以外のデータベース名のリストを作成
            tmp_dbnames = copy.deepcopy(dbnames)
            if connection['database'] in tmp_dbnames:
                tmp_dbnames.remove(connection['database'])

            # 他人のデータベースに接続を試みる
            for dbname in tmp_dbnames:
                tmp_connection = copy.deepcopy(connection)
                tmp_connection['database'] = dbname
                with self.assertRaises(EdmanDbConnectError):
                    _ = DB(tmp_connection)
