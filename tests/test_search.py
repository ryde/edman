import configparser
from pathlib import Path
from datetime import datetime
from unittest import TestCase
import dateutil.parser
from pymongo import errors as py_errors, MongoClient
from bson import ObjectId, DBRef, errors
from edman import Config, DB, Search


class TestSearch(TestCase):

    @classmethod
    def setUpClass(cls):
        # 設定読み込み
        settings = configparser.ConfigParser()
        settings.read(Path.cwd() / 'ini' / 'test_db.ini')
        cls.test_ini = dict([i for i in settings['DB'].items()])
        cls.test_ini['port'] = int(cls.test_ini['port'])

        # DB作成のため、pymongoから接続
        cls.client = MongoClient(cls.test_ini['host'], cls.test_ini['port'])

        # 接続確認
        try:
            cls.client.admin.command('ismaster')
            cls.db_server_connect = True
            print('Use DB.')
        except py_errors.ConnectionFailure:
            cls.db_server_connect = False
            print('Do not use DB.')

        if cls.db_server_connect:
            # adminで認証
            cls.client[cls.test_ini['admin_db']].authenticate(
                cls.test_ini['admin_user'],
                cls.test_ini['admin_password'])
            # DB作成
            cls.client[cls.test_ini['db']].command(
                "createUser",
                cls.test_ini['user'],
                pwd=cls.test_ini['password'],
                roles=[
                    {
                        'role': 'dbOwner',
                        'db': cls.test_ini['db'],
                    },
                ],
            )
            # ユーザ側認証
            cls.client[cls.test_ini['db']].authenticate(cls.test_ini['user'],
                                                        cls.test_ini[
                                                            'password'])

            # edmanのDB接続オブジェクト作成
            con = {
                'host': cls.test_ini['host'],
                'port': cls.test_ini['port'],
                'database': cls.test_ini['db'],
                'auth_database': cls.test_ini['db'],
                'user': cls.test_ini['user'],
                'password': cls.test_ini['password']
            }
            db = DB(con)
            cls.testdb = db.get_db
            cls.search = Search(db)
        else:
            cls.search = Search()

    @classmethod
    def tearDownClass(cls):
        if cls.db_server_connect:
            # cls.clientはpymongo経由でDB削除
            # cls.testdb.dbはedman側の接続オブジェクト経由でユーザ(自分自身)の削除
            cls.client.drop_database(cls.test_ini['db'])
            # cls.client[cls.admindb].authenticate(cls.adminid, cls.adminpasswd)
            cls.testdb.command("dropUser", cls.test_ini['user'])

    def setUp(self):
        self.config = Config()
        self.parent = self.config.parent
        self.child = self.config.child
        self.date = self.config.date
        self.file = self.config.file

    def tearDown(self):
        if self.db_server_connect:
            # システムログ以外のコレクションを削除
            collections_all = self.testdb.list_collection_names()
            log_coll = 'system.profile'
            if log_coll in collections_all:
                collections_all.remove(log_coll)
            for collection in collections_all:
                self.testdb.drop_collection(collection)

    def test__merge_parent(self):

        # データ構造のテスト
        # 本来は親子関係なのでchild項目も存在するが、テストに関係ないので省略
        dummy_id = ObjectId()
        dummy_id2 = ObjectId()
        parent_id = ObjectId()
        parent_data = {
            'parent_col': {
                '_id': dummy_id,
                'a': 'aa',
                'parent_col2': {
                    '_id': dummy_id2,
                    'b': 'bb',
                    self.parent: DBRef('parent_col', dummy_id),
                    'parent_col3': {
                        '_id': parent_id,
                        'c': 'cc',
                        self.parent: DBRef('parent_col2', dummy_id2)
                    }
                }
            }
        }
        self_data = {
            'self_col': {
                'car_name': 'F355',
                self.parent: DBRef('parent_col3', parent_id)
            }
        }
        actual = self.search._merge_parent(parent_data, self_data)
        expected = \
            actual['parent_col']['parent_col2']['parent_col3']['self_col'][
                'car_name']

        self.assertIsInstance(actual, dict)
        self.assertEqual('F355', expected)

    def test__objectid_replacement(self):
        # 正常系
        query = {'_id': '5bf4f3ce9663380fc50d6dbd'}
        actual = self.search._objectid_replacement(query)
        self.assertIsInstance(actual['_id'], ObjectId)

        # 異常系
        query = {'_id': 'dragon'}
        with self.assertRaises(errors.InvalidId) as cm:
            _ = self.search._objectid_replacement(query)

    def test__get_self(self):
        if not self.db_server_connect:
            return

        # テストデータをDBに挿入
        data = {'test_data': 'test'}
        test_collection = 'get_self_test'
        db = self.client[self.test_ini['db']]
        insert_result = db[test_collection].insert_one(data)

        # メソッドを実行してデータを取ってくる dictや構造
        # 作成したデータと取得したデータの差異
        query = {'_id': insert_result.inserted_id}
        actual = self.search._get_self(query, test_collection)
        self.assertIsInstance(actual, dict)
        self.assertEqual(sorted(list(data.keys())),
                         sorted(list(actual[test_collection].keys())))

    def test__get_parent(self):
        if not self.db_server_connect:
            return

        # テストデータをDBに挿入
        db = self.client[self.test_ini['db']]
        data1_id = ObjectId()
        data2_id = ObjectId()
        data3_id = ObjectId()
        parent_coll = 'parent_coll'
        parent2_coll = 'parent2_coll'
        self_coll = 'self_coll'
        data1 = {
            '_id': data1_id,
            'data1': 'test',
            self.child: [DBRef(parent2_coll, data2_id)]
        }
        _ = db[parent_coll].insert_one(data1)
        data2 = {
            '_id': data2_id,
            'data2': 'test',
            self.parent: DBRef(parent_coll, data1_id),
            self.child: [DBRef(self_coll, data3_id)]
        }
        _ = db[parent2_coll].insert_one(data2)
        data3 = {
            '_id': data3_id,
            'data3': 'test',
            self.parent: DBRef(parent2_coll, data2_id)
        }
        _ = db[self_coll].insert_one(data3)

        self_result = {self_coll: data3}
        actual = self.search._get_parent(self_result, depth=2)
        self.assertIsInstance(actual, dict)

        for k, v in actual.items():
            # 構造のチェック
            self.assertEqual(k, parent_coll)
            self.assertEqual(v['_id'], data1['_id'])
            self.assertIsInstance(v[parent2_coll], dict)
            self.assertEqual(v[parent2_coll]['_id'], data2_id)

            # 親データ内のネストチェック
            self.assertEqual(v[self.child][0].id, v[parent2_coll]['_id'])
            self.assertEqual(v['_id'], v[parent2_coll][self.parent].id)

            # selfとparentのネストチェック
            self.assertEqual(v[parent2_coll][self.child][0].id, data3['_id'])
            self.assertEqual(v[parent2_coll]['_id'], data3[self.parent].id)

    def test__build_to_doc_parent(self):
        # データ構造のテスト
        # parentに近い方から順番に並んでいる(一番最後がrootまたはrootに近い方)
        parent_data_list = [
            {
                'parent_2': {
                    'car_name': 'STORATOS'
                }
            },
            {
                'parent_1': {
                    'program': 'python'
                }
            }
        ]
        actual = self.search._build_to_doc_parent(parent_data_list)
        self.assertIsInstance(actual, dict)
        expected = actual['parent_1']['parent_2']['car_name']
        self.assertEqual('STORATOS', expected)

    def test__process_data_derived_from_mongodb(self):

        # 正常系
        data = {
            'coll1': {
                '_id': ObjectId(),
                'data1': 1,
                self.child: ['aa', 'bb'],
                'coll2': {
                    '_id': ObjectId(),
                    'data2': 2,
                    self.parent: 'cc',
                    self.child: ['dd', 'ee'],
                    'coll3': [
                        {
                            '_id': ObjectId(),
                            'data3': 3,
                            'list_data': [1, 3.24, 'string'],
                            self.parent: 'ff',
                            self.child: ['gg', 'hh'],
                            self.file: [ObjectId(), ObjectId()]
                        },
                        {
                            '_id': ObjectId(),
                            'data4': 4,
                            self.parent: 'ii',
                            self.child: ['jj', 'kk']
                        }
                    ]
                }
            }
        }

        def _item_literal_check(list_child):
            result = True
            if isinstance(list_child, dict):
                result = False
            if isinstance(list_child, list):
                for j in list_child:
                    if isinstance(j, dict) or isinstance(j, list):
                        result = False
                        break
            return result

        # リファレンス削除チェック用メソッド
        def detect_ref(d):
            if self.parent == d or self.child == d:
                raise ValueError('parent or child, was not deleted')

        # 日付変換チェック用メソッド
        def detect_date(d):
            if isinstance(d, datetime):
                raise ValueError('datetime was not deleted')

        # データを回すだけの簡単なお仕事
        def rec(data: dict):
            for key, val in data.items():
                if isinstance(data[key], list) and _item_literal_check(
                        data[key]):
                    for i in data[key]:
                        detect_date(i)
                elif isinstance(data[key], list):
                    for item in data[key]:
                        detect_ref(item)
                        rec(item)
                elif isinstance(data[key], dict):
                    detect_ref(data[key])
                    rec(data[key])

                # 型変換
                else:
                    detect_date(val)

        actual = self.search._process_data_derived_from_mongodb(data)
        self.assertIsInstance(actual, dict)
        self.assertIsNone(rec(actual))

    def test__format_datetime(self):
        # 正常系
        data = ['2018/11/22', '2018/11/22 11:45:23', '2019-01-01 00:00:00']
        for i in map(dateutil.parser.parse, data):
            # 入力値と出力値がdatetime形式として一致しているか確認
            with self.subTest(i=i):
                actual = self.search._format_datetime(i)
                self.assertEqual(i, dateutil.parser.parse(actual[self.date]))

            # 構造のテスト
            self.assertIsInstance(actual, dict)
            self.assertIsInstance(actual[self.date], str)
            self.assertEqual(self.date, list(actual.keys())[0])

    # def test_find(self):
    #     # 中身は別のメソッドが中心なのでテストは割愛
    #     pass
    #
    # def test__self_data_select(self):
    #     # 画面上の選択処理なので、テストは割愛
    #     pass
    #
