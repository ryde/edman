import configparser
import copy
import tempfile
import gridfs
from unittest import TestCase
from pathlib import Path
from datetime import datetime
from pymongo import errors, MongoClient
from bson import ObjectId
from edman import Config, DB


class TestDB(TestCase):

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
        except errors.ConnectionFailure:
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
            cls.con = {
                'host': cls.test_ini['host'],
                'port': cls.test_ini['port'],
                'database': cls.test_ini['db'],
                'user': cls.test_ini['user'],
                'password': cls.test_ini['password']
            }
            cls.db = DB(cls.con)
            cls.testdb = cls.db.get_db
        else:
            cls.db = DB()

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

    # def tearDown(self):
    #     # DB破棄
    #     pass

    def test_insert(self):
        if not self.db_server_connect:
            return

        data = [
            {'collection1': [
                {'name': 'IBM 5100', 'value': 100},
                {'name': 'Apple II', 'value': 200}
            ]
            },
            {'collection2': [
                {'maker': 'HONDA', 'car': 'S600'},
                {'maker': 'SUZUKI', 'car': 'Cappuccino'},
            ]
            }
        ]
        insert_result = self.db.insert(data)
        self.assertIsInstance(insert_result, list)

        actual = []
        for i in insert_result:
            for collection, oids in i.items():
                buff = []
                for idx, oid in enumerate(oids):
                    query = {'_id': oid}
                    find_result = self.testdb[collection].find_one(query)
                    buff.append(find_result)
                actual.append({collection: buff})

        for idx, i in enumerate(data):
            with self.subTest(i=i, idx=idx):
                self.assertDictEqual(i, actual[idx])

    def test__reference_item_delete(self):
        # 正常系
        doc = {
            self.parent: ObjectId(),
            self.child: [ObjectId(), ObjectId()],
            self.file: [ObjectId(), ObjectId()],
            'param': 'OK'
        }
        actual = self.db._reference_item_delete(doc)
        expected = {'param': 'OK'}
        self.assertDictEqual(actual, expected)

    def test_doc(self):
        if not self.db_server_connect:
            return

        # 正常系(ref) reference_delete=True
        doc = {
            'test': 'star',
            'val': 456,
            self.parent: ObjectId(),
            self.child: [ObjectId(), ObjectId()],
            self.file: [ObjectId(), ObjectId()]
        }
        collection = 'test_doc'
        query = None
        # insert
        insert_result = self.testdb[collection].insert_one(doc)
        oid = insert_result.inserted_id
        actual = self.db.doc(collection, oid, query, reference_delete=True)
        expected = {'test': 'star', 'val': 456}
        self.assertDictEqual(actual, expected)

        # 正常系(ref) reference_delete=False
        oid = insert_result.inserted_id
        actual = self.db.doc(collection, oid, query, reference_delete=False)
        expected = copy.deepcopy(doc)
        self.assertDictEqual(actual, expected)

        # 正常系 (emb)
        doc = {
            'test': 'star',
            'test2': {
                'moon': [
                    {'session': 'off'},
                    {
                        'session': 'on',
                        self.file: [ObjectId(), ObjectId()]
                    }
                ]
            }
        }
        query = ['test2', 'moon', '1']
        # insert
        insert_result = self.testdb[collection].insert_one(doc)
        oid = insert_result.inserted_id
        actual = self.db.doc(collection, oid, query, reference_delete=True)
        expected = {'session': 'on'}
        self.assertDictEqual(actual, expected)

    def test__get_emb_doc(self):
        # 正常系
        doc = {
            'test': 'star',
            'test2': {
                'moon': [
                    {'session': 'off'},
                    {'session': 'on'}
                ]
            }
        }
        query = ['test2', 'moon', '1']
        actual = self.db._get_emb_doc(doc, query)
        expected = {'session': 'on'}
        self.assertDictEqual(actual, expected)

        # リストデータが存在する時の指定
        doc = {
            'test': 'star',
            'test2': {
                'moon': [
                    {'session': 'off'},
                    {'session': 'on'}
                ]
            },
            'test3': {
                'wind': 'storm',
                'fire': ['flame', 'heat', 'light'],
                'aqua': 'water'
            }
        }
        query = ['test3', 'fire', '2']
        actual = self.db._get_emb_doc(doc, query)
        expected = 'light'
        self.assertEqual(actual, expected)

    def test_item_delete(self):
        if not self.db_server_connect:
            return

        # 正常系(emb)
        doc = {
            'test': 'star',
            'test2': {
                'moon': [
                    {'session': 'off'},
                    {
                        'session': 'on',
                        'delete': 'on',
                        self.file: [ObjectId(), ObjectId()]
                    }
                ]
            }
        }
        collection = 'test_item_delete'
        insert_result = self.testdb[collection].insert_one(doc)
        oid = insert_result.inserted_id
        query = ['test2', 'moon', '1']
        delete_key = 'delete'
        actual = self.db.item_delete(collection, oid, delete_key, query)
        self.assertTrue(actual)

        # 正常系(ref)
        doc = {
            'session': 'on',
            'delete': 'on',
            self.file: [ObjectId(), ObjectId()]
        }
        insert_result = self.testdb[collection].insert_one(doc)
        oid = insert_result.inserted_id
        query = None
        actual = self.db.item_delete(collection, oid, delete_key, query)
        self.assertTrue(actual)

    def test_update(self):
        if not self.db_server_connect:
            return

        # 正常系 emb
        collection = 'test_update'
        file_ref = [ObjectId(), ObjectId(), ObjectId()]
        insert_data = {
            'test1': 'Kcar',
            'sports': [{'name': 'NSX',
                        '_ed_file': file_ref}, {'name': 'works'}]

        }
        insert_result = self.testdb[collection].insert_one(insert_data)
        oid = insert_result.inserted_id
        # _ = self.testdb[collection].find_one({'_id': oid})  # debug
        # print('db_data', _)  # debug
        amend_data = {'test1': 'Kcar',
                      'sports': [{'name': 'vivio'}, {'name': 'beat'}]}
        actual = self.db.update(collection, oid, amend_data, 'emb')
        self.assertTrue(actual)

        # 日付データのテスト emb
        collection = 'test_update2'
        file_ref = [ObjectId(), ObjectId(), ObjectId()]
        date_list = [datetime.now(), datetime.now()]
        date_data = datetime.now()
        insert_data = {
            'test1': 'Kcar',
            'sports': [{'name': 'NSX',
                        '_ed_file': file_ref}, {'name': 'works'}],
            'dd': [{'aa': [{'bb': 'cc'}, {
                'dd': 'ee',
                'date_list': date_list,
                'date_data': date_data
            }]}]

        }
        insert_result = self.testdb[collection].insert_one(insert_data)
        oid = insert_result.inserted_id
        # _ = self.testdb[collection].find_one({'_id': oid})  # debug
        # print('db_data', _)  # debug
        date_list = [
            datetime(2020, 1, 2, 1, 2, 3),
            datetime(2017, 12, 24, 0, 0, 0)
        ]
        date_data = datetime(2019, 3, 6, 12, 15, 30)
        amend_data = {
            'test1': 'Kcar',
            'sports': [{'name': 'NSX',
                        '_ed_file': file_ref}, {'name': 'works'}],
            'dd': [{'aa': [{'bb': 'cc'}, {
                'dd': 'ee',
                'date_list': date_list,
                'date_data': date_data
            }]}]

        }
        actual = self.db.update(collection, oid, amend_data, 'emb')
        self.assertTrue(actual)
        # _ = self.testdb[collection].find_one({'_id': oid})  # debug
        # print('amended_db_data', _)  # debug

        # 正常系 ref
        insert_data = {
            'test1': 'sport',
            'name': 'NSX',
            '_ed_parent': ObjectId(),
            '_ed_child': [ObjectId(), ObjectId()],
            '_ed_file': [ObjectId(), ObjectId()]
        }
        amend_data = {'test1': 'sport', 'name': 'storatos'}
        insert_result = self.testdb[collection].insert_one(insert_data)
        oid = insert_result.inserted_id
        actual = self.db.update(collection, oid, amend_data, 'ref')
        self.assertTrue(actual)

        # 日付データのテスト ref
        date_list = [
            datetime(2020, 1, 2, 1, 2, 3),
            datetime(2017, 12, 24, 0, 0, 0)
        ]
        date_data = datetime(2019, 3, 6, 12, 15, 30)

        insert_data = {
            'test1': 'sport',
            'name': 'NSX',
            'date_list': date_list,
            'date_data': date_data,
            '_ed_parent': ObjectId(),
            '_ed_child': [ObjectId(), ObjectId()],
            '_ed_file': [ObjectId(), ObjectId()]
        }

        insert_result = self.testdb[collection].insert_one(insert_data)
        oid = insert_result.inserted_id

        date_list = [
            datetime(2020, 1, 2, 1, 2, 3),
            datetime(2017, 12, 24, 0, 0, 0)
        ]
        date_data = datetime(2019, 3, 6, 12, 15, 30)

        amend_data = {
            'test1': 'sport',
            'name': 'NSX',
            'date_list': date_list,
            'date_data': date_data,
            '_ed_parent': ObjectId(),
            '_ed_child': [ObjectId(), ObjectId()],
            '_ed_file': [ObjectId(), ObjectId()]
        }

        actual = self.db.update(collection, oid, amend_data, 'ref')
        db_result = self.testdb[collection].find_one({'_id': oid})
        # print('db_result', db_result)  # debug
        self.assertTrue(actual)
        # datetime型として取得できているか
        self.assertIsInstance(db_result['date_list'][0], datetime)
        self.assertIsInstance(db_result['date_list'][1], datetime)
        self.assertIsInstance(db_result['date_data'], datetime)

    def test__merge(self):
        # 正常系
        orig = {'beamtime': [{'name': 'NSX'}, {'spec': {'power': 280}}]}
        amend = {'beamtime': [{'name': 'NSX-R'}, {'spec': {'power': 300}}]}
        expected = {'beamtime': [{'name': 'NSX-R'}, {'spec': {'power': 300}}]}
        actual = self.db._merge(orig, amend)
        self.assertDictEqual(expected, actual)

        # 正常系2 より複雑な場合
        oid = ObjectId()
        file_ref = [ObjectId(), ObjectId(), ObjectId()]
        orig = {
            '_id': oid,
            'beamtime': [
                {
                    'expInfo': [
                        {'data1': {'experiment': 'process', 'flag': True,
                                   'dd': 'ff'}},
                        {'data2': {'experiment': 'process', 'flag': False,
                                   'dd': 'ff'}},
                        {
                            'data3': {
                                'experiment': 'runs',
                                'flag': True,
                                'dd': 'ff',
                                'files': {'filename': 'ss.jpg'},
                                '_ed_file': file_ref
                            }
                        }
                    ]
                }
            ]
        }
        amend = {
            'beamtime': [
                {
                    'expInfo': [
                        {'data1': {'experiment': 'process', 'flag': True,
                                   'dd': 'ff'}},
                        {'data2': {'experiment': 'start', 'flag': True,
                                   'dd': 'ff'}},
                        {
                            'data3': {
                                'experiment': 'runs',
                                'flag': True,
                                'dd': 'ee',
                                'files': {'filename': 'position.jpg'}
                            }
                        }
                    ]
                }
            ]
        }
        expected = {
            '_id': oid,
            'beamtime': [
                {
                    'expInfo': [
                        {
                            'data1': {
                                'experiment': 'process',
                                'flag': True,
                                'dd': 'ff'
                            }
                        },
                        {
                            'data2': {
                                'experiment': 'start',
                                'flag': True,
                                'dd': 'ff'
                            }
                        },
                        {
                            'data3': {
                                'experiment': 'runs',
                                'flag': True,
                                'dd': 'ee',
                                'files': {
                                    'filename': 'position.jpg'
                                },
                                '_ed_file': file_ref
                            }
                        }
                    ]
                }
            ]
        }
        actual = self.db._merge(orig, amend)
        self.assertDictEqual(expected, actual)

        # リストデータがあった場合
        oid = ObjectId()
        orig = {
            "_id": oid,
            "position": "top",
            "username": "ryde",
            "structure_list_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 380,
                    "float_val": 4453.456
                },
                {
                    "maker": "HONDA",
                    "carname": "NSX",
                    "power": 280,
                    "float_val": 321.56,
                    "list_data": [
                        "Mario",
                        "Sonic",
                        "Ryu",
                        "Link"
                    ],
                    "structure_list_3_1": [
                        {
                            "filename": "test1.txt",
                            "name": "添付ファイル1"
                        },
                        {
                            "filename": "test2.txt",
                            "name": "添付ファイル2"
                        }
                    ],
                    "structure_list_3_2": [
                        {
                            "filename": "test3.txt",
                            "name": "添付ファイル3",
                            "structure_list_4": {
                                "filename": "test4.txt",
                                "name": "添付ファイル4",
                                "structure_list_5": [
                                    {
                                        "url": "example2.com",
                                        "name": "テストURL2"
                                    },
                                    {
                                        "url": "example3.com",
                                        "name": "テストURL3"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        amend = {
            "position": "top",
            "username": "ryde",
            "structure_list_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 512,
                    "float_val": 4453.456
                },
                {
                    "maker": "HONDA",
                    "carname": "NSX-R",
                    "power": 380,
                    "float_val": 321.56,
                    "list_data": [
                        "Mario",
                        "Link"
                    ],
                    "structure_list_3_1": [
                        {
                            "filename": "test1.txt",
                            "name": "添付ファイル1"
                        },
                        {
                            "filename": "test2.txt",
                            "name": "添付ファイル2"
                        }
                    ],
                    "structure_list_3_2": [
                        {
                            "filename": "test3.txt",
                            "name": "添付ファイル3",
                            "structure_list_4": {
                                "filename": "test4.txt",
                                "name": "添付ファイル4",
                                "structure_list_5": [
                                    {
                                        "url": "example2.com",
                                        "name": "テストURL2"
                                    },
                                    {
                                        "url": "example3.com",
                                        "name": "テストURL3"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        expected = {
            "_id": oid,
            "position": "top",
            "username": "ryde",
            "structure_list_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 512,
                    "float_val": 4453.456
                },
                {
                    "maker": "HONDA",
                    "carname": "NSX-R",
                    "power": 380,
                    "float_val": 321.56,
                    "list_data": [
                        "Mario",
                        "Link"
                    ],
                    "structure_list_3_1": [
                        {
                            "filename": "test1.txt",
                            "name": "添付ファイル1"
                        },
                        {
                            "filename": "test2.txt",
                            "name": "添付ファイル2"
                        }
                    ],
                    "structure_list_3_2": [
                        {
                            "filename": "test3.txt",
                            "name": "添付ファイル3",
                            "structure_list_4": {
                                "filename": "test4.txt",
                                "name": "添付ファイル4",
                                "structure_list_5": [
                                    {
                                        "url": "example2.com",
                                        "name": "テストURL2"
                                    },
                                    {
                                        "url": "example3.com",
                                        "name": "テストURL3"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        actual = self.db._merge(orig, amend)
        self.assertDictEqual(expected, actual)

    def test__merge_list(self):

        # 正常系
        orig = [{'A': '1'}, {'B': '2'}]
        amend = [{'A': '3'}, {'B': '2'}]
        actual = self.db._merge_list(orig, amend)
        self.assertListEqual(amend, actual)

        orig = ['1', '2']
        amend = ['3', '4']
        expected = orig + amend
        actual = self.db._merge_list(orig, amend)
        self.assertListEqual(expected, actual)

        orig = ['1', '2', {'A': '3'}]
        amend = ['3', '4', {'A': '4'}]
        actual = self.db._merge_list(orig, amend)
        expected = ['1', '2', {'A': '4'}, '3', '4']
        self.assertListEqual(expected, actual)

    def test_collections(self):
        if not self.db_server_connect:
            return

        # 正常系
        collections = ['test_collections_A', 'test_collections_B',
                       'test_collections_C', 'test_collections_D',
                       'test_collections_E']
        insert_data = {'test_data': 'test'}

        for collection in collections:
            self.testdb[collection].insert_one(insert_data)

        result = self.db.collections()
        self.assertIsInstance(result, tuple)

        actual = sorted(list(result))
        expected = sorted(collections)
        self.assertListEqual(actual, expected)

    def test_find_collection_from_objectid(self):
        if not self.db_server_connect:
            return

        # 正常系
        collection = 'find_collection_from_objectid'
        insert_data = {'test_data': 'test'}
        insert_result = self.testdb[collection].insert_one(insert_data)
        actual = self.db.find_collection_from_objectid(
            insert_result.inserted_id)
        self.assertEqual(actual, collection)

        # 正常系 oidが文字列の場合、自動的にobjectIdに変換される
        collection = 'find_collection_from_objectid2'
        insert_data = {'test_data2': 'test2'}
        insert_result = self.testdb[collection].insert_one(insert_data)
        str_oid = str(insert_result.inserted_id)
        actual = self.db.find_collection_from_objectid(str_oid)
        self.assertEqual(actual, collection)

    def test__delete_execute(self):

        # 正常系
        doc = {'A': '1', 'B': '2', 'C': '3', 'D': '4'}
        keys = ['A', 'C']
        expected = {'B': '2', 'D': '4'}
        self.db._delete_execute(doc, keys)
        self.assertDictEqual(doc, expected)

        # 正常系 キーが存在しなかった場合
        doc = {'A': '1', 'B': '2', 'C': '3', 'D': '4'}
        keys = ['E']
        expected = {'A': '1', 'B': '2', 'C': '3', 'D': '4'}
        self.db._delete_execute(doc, keys)
        self.assertDictEqual(doc, expected)

    def test__convert_datetime(self):

        # 正常系
        test_date = '1997-04-01'
        data = {'name': 'KEK', 'value': 20, 'since': {self.date: test_date}}
        actual = self.db._convert_datetime(data)
        self.assertIsInstance(actual['since'], datetime)
        self.assertIsInstance(actual, dict)

        test_date = '1997/04/01'
        data = {'since': {self.date: test_date}}
        actual = self.db._convert_datetime(data)
        self.assertIsInstance(actual['since'], datetime)

        # 正常系 日付をテキストで入力された場合
        data = {'since': test_date}
        actual = self.db._convert_datetime(data)
        self.assertIsInstance(actual['since'], str)

        # 正常系 #dateを利用しているが、変換対象でない場合
        data = {'since': {self.date: '1997年4月1日'}}
        actual = self.db._convert_datetime(data)
        self.assertIsInstance(actual['since'], str)

        # 正常系 リストデータを含む
        test_date = ['1997-04-01', '2004/4/1']
        data = {
            'date_list':
                [
                    {self.date: test_date[0]},
                    {self.date: test_date[1]}
                ]
        }
        actual = self.db._convert_datetime(data)
        self.assertIsInstance(actual['date_list'], list)
        for i in actual['date_list']:
            with self.subTest(i=i):
                self.assertIsInstance(i, datetime)

    def test_delete(self):
        if not self.db_server_connect:
            return

        # emb削除テスト
        collection = 'test_delete_test1'
        emb_data = [{collection: {
            "position": "top",
            "username": "ryde",
            "structure_list_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 380,
                    "float_val": 4453.456
                },
                {
                    "maker": "HONDA",
                    "carname": "NSX",
                    "power": 280,
                    "float_val": 321.56,
                    "list_data": [
                        "Mario",
                        "Sonic",
                        "Ryu",
                        "Link"
                    ]
                }
            ]
        }}]
        result = self.db.insert(emb_data)
        actual = self.db.delete(result[0][collection][0], collection, 'emb')
        self.assertTrue(actual)
        f_result = self.testdb[collection].find_one(
            {'_id': result[0][collection][0]})
        self.assertIsNone(f_result)

        # emb file添付されていた場合

        # ファイル作成
        # dbにインサート
        fs = gridfs.GridFS(self.testdb)
        fs_inserted_oid = fs.put(b'hello, world', filename='sample.txt')

        # テストデータ作成
        collection = 'delete_emb_fs_sample'
        data = {
            'name': 'NSX',
            'st2': [
                {'name': 'Gt-R', 'power': '280'},
                {'name': '180SX', 'power': '220', '_ed_file': [fs_inserted_oid]}
            ],
            'type': 'RX'
        }
        # dbにインサート
        inserted = self.testdb[collection].insert_one(data)
        # print(inserted.inserted_id)

        # 削除テスト
        actual = self.db.delete(inserted.inserted_id, collection, 'emb')
        self.assertTrue(actual)

        # ref 親と子の間削除テスト
        # TODO refテストデータ入力
        # TODO refデータ削除
        # TODO 削除されたか確認、assert

        # ref 親削除テスト
        # TODO refテストデータ入力
        # TODO refデータ削除
        # TODO 削除されたか確認、assert

        # ref 子削除テスト
        # TODO refテストデータ入力
        # TODO refデータ削除
        # TODO 削除されたか確認、assert

        # ref file添付されていた場合のテスト
        # TODO refテストデータ入力
        # TODO refデータ削除
        # TODO 削除されたか確認、assert

    # def test_connect(self):
    #     # setUpClassで使用。割愛
    #     pass
    #
