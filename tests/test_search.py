import configparser
import copy
from datetime import datetime
# from logging import getLogger,  FileHandler, ERROR
from logging import ERROR, StreamHandler, getLogger
from pathlib import Path
from unittest import TestCase

import dateutil.parser
from bson import DBRef, ObjectId, errors
from pymongo import MongoClient
from pymongo import errors as py_errors

from edman import DB, Config, Convert, Search


class TestSearch(TestCase):
    db_server_connect = False
    test_ini: dict = {}
    client = None
    testdb = None

    @classmethod
    def setUpClass(cls):
        # 設定読み込み
        settings = configparser.ConfigParser()
        settings.read(Path.cwd() / 'ini' / 'test_db.ini')
        cls.test_ini = dict(settings.items('DB'))
        port = int(cls.test_ini['port'])
        cls.test_ini['port'] = port

        # DB作成のため、pymongoから接続
        cls.client = MongoClient(cls.test_ini['host'], cls.test_ini['port'])

        # 接続確認
        try:
            cls.client.admin.command('hello')
            cls.db_server_connect = True
            print('Use DB.')
        except py_errors.ConnectionFailure:
            print('Do not use DB.')

        if cls.db_server_connect:
            # adminで認証
            cls.client = MongoClient(
                username=cls.test_ini['admin_user'],
                password=cls.test_ini['admin_password'])
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
            # edmanのDB接続オブジェクト作成
            con = {
                'host': cls.test_ini['host'],
                'port': cls.test_ini['port'],
                'user': cls.test_ini['user'],
                'password': cls.test_ini['password'],
                'database': cls.test_ini['db'],
                'options': [f"authSource={cls.test_ini['db']}"]
            }
            db = DB(con)
            cls.testdb = db.get_db
            cls.search = Search(db)
            cls.db = db
        else:
            cls.search = Search()

        cls.logger = getLogger()

        # ログを画面に出力
        ch = StreamHandler()
        ch.setLevel(ERROR)  # ハンドラーにもそれぞれログレベル、フォーマットの設定が可能
        cls.logger.addHandler(ch)  # StreamHandlerの追加

        # ログをファイルに出力
        # fh = FileHandler('./tests.log')  # 引数には出力ファイルのパスを指定
        # fh.setLevel(ERROR)  # ハンドラーには、logger以下のログレベルを設定することは出来ない(この場合、DEBUGは不可)
        # cls.logger.addHandler(fh)  # FileHandlerの追加


    @classmethod
    def tearDownClass(cls):
        if cls.db_server_connect:
            # cls.clientはpymongo経由でDB削除
            # cls.testdb.dbはedman側の接続オブジェクト経由でユーザ(自分自身)の削除
            cls.client.drop_database(cls.test_ini['db'])
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
        with self.assertRaises(errors.InvalidId):
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

        # 存在しないデータの時はNoneを返す
        query = {'_id': insert_result.inserted_id, 'test_data': 'foo'}
        actual = self.search._get_self(query, test_collection)
        self.assertIsNone(actual)

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


        # テスト
        # parent_col = 'Beamtime'
        # target_col = 'expInfo'
        # d = {
        #     parent_col:
        #         {
        #             "test_data": "test",
        #             target_col: [
        #                 {
        #                     "layer_test1b": "data",
        #                     "layer_test1a": [
        #                         {
        #                             "test2_data": "data",
        #                             "layer_test2": [
        #                                 {
        #                                     "layer_test3a": "data",
        #                                     "layer_test3b": [
        #                                         {
        #                                             "layer_test4a": "data",
        #                                             "layer_test4b": [
        #                                                 {
        #                                                     "layer_test5a": "data"
        #                                                 }
        #                                             ]
        #                                         }
        #                                     ]
        #                                 }
        #                             ]
        #                         },
        #                         {
        #                             "test3_data": "data",
        #                             "layer_test2": [
        #                                 {
        #                                     "layer_test3a_2": "data3_2",
        #                                     "layer_test3b": [
        #                                         {
        #                                             "layer_test4a_2": "data_2",
        #                                             "layer_test4b": [
        #                                                 {
        #                                                     "layer_test5a_2": "data_2"
        #                                                 },
        #                                                 {
        #                                                     "elen": "elen",
        #                                                     "layer_test5": [
        #                                                         {
        #                                                             "mario": "mario"
        #                                                         },
        #                                                         {
        #                                                             "mario2": "mario22"
        #                                                         },
        #                                                     ]
        #                                                 }
        #                                             ]
        #                                         }
        #                                     ]
        #                                 }
        #                             ]
        #                         }
        #                     ]
        #                 }
        #             ]
        #         }
        # }
        #
        # convert = Convert()
        # insert_result = self.db.insert(convert.dict_to_edman(d))
        #
        # # インサート結果からtopのoidを取得
        # b = []
        # for i in insert_result:
        #     for k, v in i.items():
        #         if k == parent_col:
        #             b.extend(v)
        # insert_root_oid = b[0]
        #
        # # rootのドキュメントを取得
        # docs = self.search.doc2(parent_col, insert_root_oid)
        #
        # # 中間子要素と最後の子要素のテスト用データ取得のために子要素を全部取得
        # o = self.db.get_child_all({parent_col: docs})
        # # x = self.search.generate_json_dict(o, include=['_id', Config.parent,
        # #                                                Config.child])
        # x = self.search.generate_json_dict(o, include=[Config.parent,
        #                                                Config.child])
        #
        # # 最下層の子要素を取得
        # # self_doc_id = (x['expInfo'][0]['layer_test1a'][1]['layer_test2'][0]
        # # ['layer_test3b'][0]['layer_test4b'][1]['layer_test5'][0]['_id'])
        #
        # lower_doc = (x['expInfo'][0]['layer_test1a'][1]['layer_test2'][0]
        # ['layer_test3b'][0]['layer_test4b'][1]['layer_test5'][0])
        # print('lower_doc', lower_doc)
        # a = self.search._get_parent({'layer_test5':lower_doc}, depth=4)
        # print('a', a)

    # def test_get_ref_depth_bfs(self):
    #     if not self.db_server_connect:
    #         return
    #
    #     parent_col = 'Beamtime'
    #     target_col = 'expInfo'
    #     d = {
    #         parent_col:
    #             {
    #                 "test_data": "test",
    #                 target_col: [
    #                     {
    #                         "layer_test1b": "data",
    #                         "layer_test1a": [
    #                             {
    #                                 "test2_data": "data",
    #                                 "layer_test2": [
    #                                     {
    #                                         "layer_test3a": "data",
    #                                         "layer_test3b": [
    #                                             {
    #                                                 "layer_test4a": "data",
    #                                                 "layer_test4b": [
    #                                                     {
    #                                                         "layer_test5a": "data"
    #                                                     }
    #                                                 ]
    #                                             }
    #                                         ]
    #                                     }
    #                                 ]
    #                             },
    #                             {
    #                                 "test3_data": "data",
    #                                 "layer_test2": [
    #                                     {
    #                                         "layer_test3a_2": "data3_2",
    #                                         "layer_test3b": [
    #                                             {
    #                                                 "layer_test4a_2": "data_2",
    #                                                 "layer_test4b": [
    #                                                     {
    #                                                         "layer_test5a_2": "data_2"
    #                                                     },
    #                                                     {
    #                                                         "elen": "elen",
    #                                                         "layer_test5": [
    #                                                             {
    #                                                                 "mario": "mario"
    #                                                             },
    #                                                             {
    #                                                                 "mario2": "mario22"
    #                                                             },
    #                                                         ]
    #                                                     }
    #                                                 ]
    #                                             }
    #                                         ]
    #                                     }
    #                                 ]
    #                             }
    #                         ]
    #                     }
    #                 ]
    #             }
    #     }
    #
    #     convert = Convert()
    #     insert_result = self.db.insert(convert.dict_to_edman(d))
    #
    #     # インサート結果からtopのoidを取得
    #     b = []
    #     for i in insert_result:
    #         for k, v in i.items():
    #             if k == parent_col:
    #                 b.extend(v)
    #     insert_root_oid = b[0]
    #
    #     # rootのドキュメントを取得
    #     docs = self.search.doc2(parent_col, insert_root_oid)
    #
    #     # 中間子要素と最後の子要素のテスト用データ取得のために子要素を全部取得
    #     o = self.db.get_child_all({parent_col: docs})
    #     # x = self.search.generate_json_dict(o, include=['_id', Config.parent,
    #     #                                                Config.child])
    #     x = self.search.generate_json_dict(o, include=['_id', Config.parent,
    #                                                    Config.child])
    #     target_doc = x['expInfo'][0]
    #     print('target_doc', target_doc)
    #
    #     result = self.search.get_ref_depth_bfs('expInfo',target_doc['_id'])
    #     print(result)


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

    def test_generate_json_dict(self):

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

        actual = self.search.generate_json_dict(data)
        self.assertIsInstance(actual, dict)
        self.assertIsNone(rec(actual))

        # 正常系 refsの指定 resultに_id, ed_parent, _ed_childを残す
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
        expected = copy.deepcopy(data)
        del expected['coll1']['coll2']['coll3'][0][self.file]
        refs = ['_id', self.parent, self.child]
        actual = self.search.generate_json_dict(data, include=refs)
        self.assertDictEqual(actual, expected)

        # 正常系 refsに設定するが空リストの場合
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
        expected = copy.deepcopy(data)
        del expected['coll1']['_id']
        del expected['coll1'][self.child]
        del expected['coll1']['coll2']['_id']
        del expected['coll1']['coll2'][self.parent]
        del expected['coll1']['coll2'][self.child]
        del expected['coll1']['coll2']['coll3'][0]['_id']
        del expected['coll1']['coll2']['coll3'][0][self.parent]
        del expected['coll1']['coll2']['coll3'][0][self.child]
        del expected['coll1']['coll2']['coll3'][0][self.file]
        del expected['coll1']['coll2']['coll3'][1]['_id']
        del expected['coll1']['coll2']['coll3'][1][self.parent]
        del expected['coll1']['coll2']['coll3'][1][self.child]
        actual = self.search.generate_json_dict(data, include=[])
        self.assertDictEqual(actual, expected)

        # 異常系 リファレンス系以外のキーを指定した場合
        data_e1 = {
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
        with self.assertRaises(ValueError):
            _ = self.search.generate_json_dict(data_e1, include=['test'])

        # 異常系 exclusionにリストとNone以外の値が入力された場合
        data_e2 = {
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
        with self.assertRaises(ValueError):
            _ = self.search.generate_json_dict(data_e2, include=(1, 2))

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

        # 正常系 階層指定 中身がdb.find()なので割愛
        # all_docs = self.search.get_documents(1, doc_col, doc_id,
        #                                      parent_depth=0, child_depth=1)
        # print('all_docs:', all_docs)
        # 正常系 単一のドキュメント 中身がdb.find()なので割愛
        # all_docs = self.search.get_documents(3, doc_col, doc_id,
        #                                      parent_depth=0, child_depth=0)
        # print('all_docs:', all_docs)

    def test_get_tree(self):
        if not self.db_server_connect:
            return

        # テストデータ
        parent_col = 'Beamtime'
        target_col = 'expInfo'
        d = {
            parent_col:
                {
                    "test_data": "test",
                    target_col: [
                        {
                            "layer_test1b": "data",
                            "layer_test1a": [
                                {
                                    "test2_data": "data",
                                    "layer_test2": [
                                        {
                                            "layer_test3a": "data",
                                            "layer_test3b": [
                                                {
                                                    "layer_test4a": "data",
                                                    "layer_test4b": [
                                                        {
                                                            "layer_test5a": "data"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                },
                                {
                                    "test3_data": "data",
                                    "layer_test2": [
                                        {
                                            "layer_test3a_2": "data3_2",
                                            "layer_test3b": [
                                                {
                                                    "layer_test4a_2": "data_2",
                                                    "layer_test4b": [
                                                        {
                                                            "layer_test5a_2": "data_2"
                                                        },
                                                        {
                                                            "elen": "elen",
                                                            "layer_test5": [
                                                                {
                                                                    "mario": "mario"
                                                                },
                                                                {
                                                                    "mario2": "mario22"
                                                                },
                                                            ]
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
        }

        convert = Convert()
        insert_result = self.db.insert(convert.dict_to_edman(d))

        # インサート結果からtopのoidを取得
        b = []
        for i in insert_result:
            for k, v in i.items():
                if k == parent_col:
                    b.extend(v)
        insert_root_oid = b[0]

        # rootのドキュメントを取得
        docs = self.search.doc2(parent_col, insert_root_oid)

        # tree取得でrootを指定した場合(Config.parentなし)
        top_all_tree = self.search.get_tree(parent_col, docs['_id'])

        # 中間子要素と最後の子要素のテスト用データ取得のために子要素を全部取得
        o = self.db.get_child_all({parent_col: docs})
        x = self.search.generate_json_dict(o, include=['_id', Config.parent,
                                                       Config.child])

        # tree取得で中間の場合
        self_doc_id = x['expInfo'][0]['_id']
        mid_all_tree = self.search.get_tree('expInfo', self_doc_id)

        # tree取得で最後の子要素を指定した場合(Config.childなし)
        self_doc_id = (x['expInfo'][0]['layer_test1a'][1]['layer_test2'][0]
        ['layer_test3b'][0]['layer_test4b'][1]['layer_test5'][0]['_id'])
        last_all_tree = self.search.get_tree('layer_test5', self_doc_id)

        self.assertDictEqual(top_all_tree, mid_all_tree)
        self.assertDictEqual(mid_all_tree, last_all_tree)
        self.assertDictEqual(last_all_tree, top_all_tree)

        # oidを含むツリーを取得する場合(例としてpx-appの詳細画面のtree取得を想定)
        # test_tree = self.search.get_tree('layer_test5', self_doc_id, include=['_id'])
        # print(test_tree)

    def test_doc2(self):
        if not self.db_server_connect:
            return

        # テストデータ
        doc = {
            'test': 'star',
            'val': 456,
            self.parent: ObjectId(),
            self.child: [ObjectId(), ObjectId()],
            self.file: [ObjectId(), ObjectId()]
        }
        collection = 'test_doc'
        insert_result = self.testdb[collection].insert_one(doc)
        oid = insert_result.inserted_id

        # 正常系 リファレンスデータを除く
        actual = self.search.doc2(
            collection, oid,
            exclude_keys=['_id', self.parent, self.child, self.file])
        expected = {'test': 'star', 'val': 456}
        self.assertDictEqual(actual, expected)

        # 正常系 リファレンスデータあり
        actual = self.search.doc2(collection, oid)
        expected = copy.deepcopy(doc)
        self.assertDictEqual(actual, expected)

    # def test_logger_test(self):
    #     self.search.logger_test()

    # def test_find(self):
    #     pass

    # d = {
    #     "Beamtime":
    #         [
    #             {
    #                 "date": {"#date": "2019-09-17"},
    #                 "expInfo": [
    #                     {
    #                         "time": {"#date": "2019/09/17 13:21:45"},
    #                         "int_value": 135,
    #                         "float_value": 24.98
    #                     },
    #                     {
    #                         "time": {"#date": "2019/09/17 13:29:12"},
    #                         "string_value": "hello world"
    #                     },
    #                     {"layer_test1a": {
    #                         "layer_test2": {
    #                             "layer_test3a": "data",
    #                             "layer_test3b": {
    #                                 "layer_test4a": "data",
    #                                 "layer_test4b": {
    #                                     "layer_test5a": "data"
    #                                 },
    #                                 "data": "data"
    #                             },
    #                         },
    #                         "test2_data": "data"
    #                     },
    #                         "layer_test1b": "data"}
    #                 ]
    #             },
    #             {
    #                 "date": {"#date": "2019-09-18"},
    #                 "expInfo": [
    #                     {
    #                         "array_value": ["string", 1234, 56.78, True,
    #                                         None],
    #                         "Bool": False,
    #                         "Null type": None
    #                     }
    #                 ]
    #             }
    #         ]
    # }

    # OKパターン
    # d = {
    #     "a_col": {
    #         "key1": "data"
    #     }
    # }

    # OKパターン
    # d = {
    #     "a_col": {
    #         "b_col": {"key":"data"}
    #     }
    # }

    # # NGパターン
    # d = {
    #     "a_col": {
    #         "b_col": {
    #             "c_col":{"key":"data"}
    #         }
    #     }
    # }

    # # OKパターン
    # d = {
    #     "a_col": {
    #         "key":"data",
    #         "b_col": {
    #             "key":"data",
    #             "c_col":{"key":"data"}
    #         }
    #     }
    # }
    #

    # convert = Convert()
    # converted_edman = convert.dict_to_edman(d)
    # print(f"{converted_edman=}")
    # insert_result = self.db.insert(converted_edman)
    # print(f"{insert_result=}")

    # b= []
    # for i in insert_result:
    #     for k ,v in i.items():
    #         if k == 'Beamtime':
    #             b.extend(v)
    #
    # oid = b[0]
    # r = self.search.find('Beamtime',{'_id':oid},parent_depth=1, child_depth=5)
    # print(f"{r=}")

    # d = DB({'port': '27017', 'host': 'localhost', 'user': 'admin',
    #         'password': 'admin', 'database': 'pxs_edman-user02_db',
    #         'options': ['authSource=admin']})
    # s = Search(d)
    # collection = 'plate'
    # query = {'_id': ObjectId('655c0b0854e5efe89ad26747')}
    # search_result = s.find(collection, query, parent_depth=0,
    #                        child_depth=1, exclusion=['_id'])
    # print('result', search_result)

    # r = d.get_child_all(search_result)
    # r = d.get_child(search_result, 0)
    # print('r', r)

    #
    # def test__self_data_select(self):
    #     # 画面上の選択処理なので、テストは割愛
    #     pass
    #
