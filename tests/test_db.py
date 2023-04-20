import configparser
import copy
import tempfile
import gridfs
from unittest import TestCase
from pathlib import Path
from datetime import datetime
import dateutil.parser
from pymongo import errors, MongoClient
from bson import ObjectId, DBRef
from edman import Config, DB, Convert, File
from edman.exceptions import EdmanDbProcessError


class TestDB(TestCase):
    db_server_connect = False
    test_ini = []
    client = None

    @classmethod
    def setUpClass(cls):

        # 設定読み込み
        settings = configparser.ConfigParser()
        settings.read(Path.cwd() / 'ini' / 'test_db.ini')
        cls.test_ini = dict(settings.items('DB'))
        cls.test_ini['port'] = int(cls.test_ini['port'])

        # DB作成のため、pymongoから接続
        cls.client = MongoClient(cls.test_ini['host'], cls.test_ini['port'])

        # 接続確認
        try:
            cls.client.admin.command('ping')
            cls.db_server_connect = True
            print('Use DB.')
        except errors.ConnectionFailure:
            print('Do not use DB.')

        if cls.db_server_connect:
            # adminで認証
            cls.client = MongoClient(
                username=cls.test_ini['admin_user'],
                password=cls.test_ini['admin_password'])

            admin_conn = {
                'host': cls.test_ini['host'],
                'port': cls.test_ini['port'],
                'user': cls.test_ini['admin_user'],
                'password': cls.test_ini['admin_password'],
                'database': cls.test_ini['admin_db'],
                'options': [f"authSource={cls.test_ini['admin_db']}"]
            }
            cls.admin_db = DB(admin_conn)

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
            cls.db = DB(con)
            cls.testdb = cls.db.get_db
        else:
            cls.db = DB()

    @classmethod
    def tearDownClass(cls):
        if cls.db_server_connect:
            # cls.clientはpymongo経由でDB削除
            # cls.testdb.dbはedman側の接続オブジェクト経由でユーザ(自分自身)の削除
            cls.client.drop_database(cls.test_ini['db'])
            cls.testdb.command("dropUser", cls.test_ini['user'])

    def tearDown(self) -> None:
        if self.db_server_connect:
            # システムログ以外のコレクションを削除
            collections_all = self.testdb.list_collection_names()
            log_coll = 'system.profile'
            if log_coll in collections_all:
                collections_all.remove(log_coll)
            for collection in collections_all:
                self.testdb.drop_collection(collection)

    def setUp(self):
        self.config = Config()
        self.parent = self.config.parent
        self.child = self.config.child
        self.date = self.config.date
        self.file = self.config.file

    # def tearDown(self):
    #     # DB破棄
    #     pass

    @staticmethod
    def make_txt_files(dir_path, name='file_dl_list', suffix='.txt',
                       text='test', qty=1):
        # 添付ファイル用テキストファイル作成
        p = Path(dir_path)
        if qty == 1:
            filename = name + suffix
            save_path = p / filename
            with save_path.open('w') as f:
                f.write(text)
        else:
            for i in range(qty):
                filename = name + str(i) + suffix
                save_path = p / filename
                with save_path.open('w') as f:
                    f.write(text + str(i))
        return sorted(p.glob(name + '*' + suffix))

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

        data2 = [
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

    def test__convert_datetime_dict(self):

        # 正常系
        test_date = '1997-04-01'
        data = {'name': 'KEK', 'value': 20, 'since': {self.date: test_date}}
        actual = self.db._convert_datetime_dict(data)
        self.assertIsInstance(actual['since'], datetime)
        self.assertIsInstance(actual, dict)

        test_date = '1997/04/01'
        data = {'since': {self.date: test_date}}
        actual = self.db._convert_datetime_dict(data)
        self.assertIsInstance(actual['since'], datetime)

        # 正常系 日付をテキストで入力された場合
        data = {'since': test_date}
        actual = self.db._convert_datetime_dict(data)
        self.assertIsInstance(actual['since'], str)

        # 正常系 #dateを利用しているが、変換対象でない場合
        data = {'since': {self.date: '1997年4月1日'}}
        actual = self.db._convert_datetime_dict(data)
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
        actual = self.db._convert_datetime_dict(data)
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
                {'name': '180SX', 'power': '220',
                 '_ed_file': [fs_inserted_oid]}
            ],
            'type': 'RX'
        }
        # dbにインサート
        inserted = self.testdb[collection].insert_one(data)

        # 削除テスト
        actual = self.db.delete(inserted.inserted_id, collection, 'emb')
        self.assertTrue(actual)

        # ref 親と子の間削除テスト
        data = {
            'delete_ref_fs_sample': {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo'},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        del_collection = 'st2'
        children = []
        for collections in inserted_report:
            for collection, oids in collections.items():
                if collection == del_collection:
                    children.extend(oids)
        del_oid = children[1]
        self.db.delete(del_oid, del_collection, 'ref')
        doc = self.testdb[del_collection].find_one({'_id': del_oid})
        self.assertIsNone(doc)

        # ref 親(指定がトップ)削除テスト
        data = {
            'delete_ref_fs_sample': {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo'},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        del_collection = 'delete_ref_fs_sample'
        children = []
        for collections in inserted_report:
            for collection, oids in collections.items():
                if collection == del_collection:
                    children.extend(oids)
        del_oid = children[0]
        self.db.delete(del_oid, del_collection, 'ref')
        doc = self.testdb[del_collection].find_one({'_id': del_oid})
        self.assertIsNone(doc)

        # ref 子(指定が一番下のドキュメント)削除テスト
        data = {
            'delete_ref_fs_sample': {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo'},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        del_collection = 'engine'
        children = []
        for collections in inserted_report:
            for collection, oids in collections.items():
                if collection == del_collection:
                    children.extend(oids)
        del_oid = children[0]
        self.db.delete(del_oid, del_collection, 'ref')
        doc = self.testdb[del_collection].find_one({'_id': del_oid})
        self.assertIsNone(doc)

        # ref file添付されていた場合のテスト
        fs_inserted_oid = fs.put(b'hello, world', filename='sample.txt')
        fs_inserted_oid2 = fs.put(b'hello, world2', filename='sample2.txt')
        fs_inserted_oid3 = fs.put(b'hello, world3', filename='sample3.txt')

        data = {
            'delete_ref_fs_sample': {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280',
                     '_ed_file': [fs_inserted_oid]},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo',
                             '_ed_file': [fs_inserted_oid2, fs_inserted_oid3]},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        del_collection = 'st2'
        children = []
        for collections in inserted_report:
            for collection, oids in collections.items():
                if collection == del_collection:
                    children.extend(oids)
        del_oid = children[1]
        self.db.delete(del_oid, del_collection, 'ref')
        doc = self.testdb[del_collection].find_one({'_id': del_oid})
        self.assertIsNone(doc)

        # gridfsのファイルが消えているか確認
        expected = sorted([fs_inserted_oid2, fs_inserted_oid3])
        for oid in expected:
            with self.subTest(oid=oid):
                self.assertFalse(fs.exists(oid))

    def test__delete_documents(self):
        if not self.db_server_connect:
            return

        fs = gridfs.GridFS(self.testdb)
        fs_inserted_oid = fs.put(b'hello, world', filename='sample.txt')
        fs_inserted_oid2 = fs.put(b'hello, world2', filename='sample2.txt')
        collection = 'delete_ref_fs_sample'
        data = {
            collection: {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo', '_ed_file': [fs_inserted_oid2]},
                            {'type': 'NA'}
                        ],
                     '_ed_file': [fs_inserted_oid]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        doc = self.testdb[collection].find_one(
            {'_id': inserted_report[2][collection][0]})
        elements = self.db._recursive_extract_elements_from_doc(doc,
                                                                collection)

        delete_doc_id_dict = {}
        for element in elements:
            doc_collection = list(element.keys())[0]
            id_and_refs = list(element.values())[0]

            for oid, refs in id_and_refs.items():
                if delete_doc_id_dict.get(doc_collection):
                    delete_doc_id_dict[doc_collection].append(oid)
                else:
                    delete_doc_id_dict.update({doc_collection: [oid]})

        self.db._delete_documents(delete_doc_id_dict)
        doc = self.testdb[collection].find_one({'_id': doc['_id']})
        self.assertIsNone(doc)

    def test__delete_reference_from_parent(self):
        if not self.db_server_connect:
            return
        pass
        collection = 'delete_ref_fs_sample'
        data = {
            collection: {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo'},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)

        doc = self.testdb['st2'].find_one(
            {'_id': inserted_report[1]['st2'][0]})
        self.db._delete_reference_from_parent(doc[self.parent], doc['_id'])

        parent_delete_after = self.testdb[collection].find_one(
            {'_id': doc[self.parent].id})

        expected = [i.id for i in parent_delete_after[self.child]]
        self.assertFalse(True if doc['_id'] in expected else False)

    def test__extract_elements_from_doc(self):
        if not self.db_server_connect:
            return

        fs = gridfs.GridFS(self.testdb)
        fs_inserted_oid = fs.put(b'hello, world', filename='sample.txt')
        fs_inserted_oid2 = fs.put(b'hello, world2', filename='sample2.txt')

        collection = 'delete_ref_fs_sample'
        data = {
            collection: {
                'name': 'NSX',
                '_ed_file': [fs_inserted_oid, fs_inserted_oid2],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        doc = self.testdb[collection].find_one(
            {'_id': inserted_report[0][collection][0]})
        actual = [self.db._extract_elements_from_doc(doc, collection)]

        self.assertIsInstance(actual, list)
        self.assertEqual(collection, list(actual[0].keys())[0])
        self.assertEqual(doc['_id'], list(actual[0][collection].keys())[0])
        self.assertListEqual(doc['_ed_file'],
                             list(actual[0][collection].values())[0][
                                 '_ed_file'])

    def test__recursive_extract_elements_from_doc(self):
        if not self.db_server_connect:
            return

        fs = gridfs.GridFS(self.testdb)
        fs_inserted_oid = fs.put(b'hello, world', filename='sample.txt')
        fs_inserted_oid2 = fs.put(b'hello, world2', filename='sample2.txt')
        collection = 'delete_ref_fs_sample'
        data = {
            collection: {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo', '_ed_file': [fs_inserted_oid2]},
                            {'type': 'NA'}
                        ],
                     '_ed_file': [fs_inserted_oid]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        # print(inserted_report)
        doc = self.testdb[collection].find_one(
            {'_id': inserted_report[2][collection][0]})
        actual = [i for i in self.db._recursive_extract_elements_from_doc(
            doc, collection)]

        expected_oid_list = []
        for i in inserted_report:
            for c, oids in i.items():
                expected_oid_list.extend(oids)

        actual_oid_list = []
        actual_file_list = []
        for i in actual:
            for collection, ids in i.items():
                for oid in ids:
                    actual_oid_list.append(oid)
                    if ids[oid].get('_ed_file'):
                        actual_file_list.extend(ids[oid]['_ed_file'])

        self.assertIsInstance(actual, list)
        self.assertListEqual(sorted([fs_inserted_oid, fs_inserted_oid2]),
                             sorted(actual_file_list))
        self.assertListEqual(sorted(expected_oid_list),
                             sorted(actual_oid_list))

    def test__collect_emb_file_ref(self):

        # 正常系
        l1 = [ObjectId(), ObjectId(), ObjectId()]
        l2 = [ObjectId(), ObjectId()]
        l3 = [ObjectId()]
        expected = l1 + l2 + l3
        data = {
            '_id': ObjectId(),
            'name': 'NSX',
            'st2': [
                {'name': 'Gt-R', 'power': '280',
                 '_ed_file': l1},
                {'name': '180SX', 'power': '220',
                 '_ed_file': l2}
            ],
            '_ed_file': l3
        }
        actual = sum(
            [i for i in self.db._collect_emb_file_ref(data, '_ed_file')], [])
        self.assertListEqual(actual, expected)

        # ファイルリファレンスが含まれていなかった場合
        data = {
            '_id': ObjectId(),
            'name': 'NSX',
            'st2': [
                {'name': 'Gt-R', 'power': '280'},
                {'name': '180SX', 'power': '220'}
            ],
            'type': 'RX'
        }
        actual = sum(
            [i for i in self.db._collect_emb_file_ref(data, '_ed_file')], [])
        self.assertEqual(len(actual), 0)

    def test_get_reference_point(self):
        # データ構造及び、値のテスト
        collection = 'collection_name'
        data = {
            collection: {
                '_id': 'aa', self.parent: 'bb'
            }
        }
        actual = self.db.get_reference_point(data[collection])

        self.assertIsInstance(actual, dict)
        self.assertTrue(actual[self.parent])
        self.assertFalse(actual[self.child])

        # データ構造及び、値のテスト　その2
        data = {
            collection: {
                '_id': 'aa',
                self.parent: 'bb',
                self.child: 'cc'
            }
        }
        actual = self.db.get_reference_point(data[collection])
        self.assertTrue(actual[self.child])

    def test_get_structure(self):
        if not self.db_server_connect:
            return

        # emb モードのテスト
        collection = 'test_get_structure_emb'
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
        oid = result[0][collection][0]
        actual = self.db.get_structure(collection, oid)
        # print('actual', actual)
        self.assertEqual(actual, 'emb')

        # ref モードのテスト
        data = {
            'sample': {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo'},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        target_collection = 'st2'
        oid = inserted_report[1][target_collection][1]
        actual = self.db.get_structure(target_collection, oid)
        self.assertEqual(actual, 'ref')

    def test_structure(self):
        if not self.db_server_connect:
            return

        # refからembへコンバート
        data = {
            'sample': {
                'name': 'NSX',
                'st2': [
                    {'name': 'GT-R', 'power': '280'},
                    {'name': '180SX', 'power': '220', 'engine':
                        [
                            {'type': 'turbo'},
                            {'type': 'NA'}
                        ]
                     }
                ],
                'type': 'R'
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)

        target_collection = 'st2'
        file = File(self.testdb)
        attached_file_oid = inserted_report[0]['engine'][0]

        with tempfile.TemporaryDirectory() as tmp_dir:
            p_files = self.make_txt_files(tmp_dir, name='file_ref',text='test ref')
            for p_file in p_files:
                file.upload('engine', attached_file_oid,((p_file,False),),'ref')

        oid = inserted_report[1][target_collection][1]
        new_collection = 'new_collection'
        actual = self.db.structure(target_collection, oid,
                                   structure_mode='emb',
                                   new_collection=new_collection)
        find_result = self.testdb[new_collection].find_one(
            {'_id': actual[0][new_collection][0]})
        self.assertTrue(
            True if self.file in find_result['engine'][0] else False)

        # _ed_fileとidを除いたデータが入力値と一致するか
        del find_result['engine'][0][self.file]
        del find_result['_id']
        self.assertEqual(data['sample']['st2'][1], find_result)

        # ドキュメントが一つの場合
        data = {
            'sample': {
                'name': 'NSX',
                'power': 280
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='ref')
        inserted_report = self.db.insert(converted_edman)
        oid = inserted_report[0]['sample'][0]
        new_collection = 'new_collection'
        actual = self.db.structure('sample', oid,
                                   structure_mode='emb',
                                   new_collection=new_collection)
        # print(actual)
        find_result = self.testdb[new_collection].find_one(
            {'_id': actual[0][new_collection][0]})
        del find_result['_id']
        self.assertDictEqual(data['sample'], find_result)

        # embからrefへの変換
        data = {
            'sample2': {
                'game list': [
                    {
                        'product': 'super mario land'
                    },
                    {
                        'product:': 'metal gear solid'
                    },
                    {
                        'data': 'value',
                        'Machine product': [
                            {
                                'hard': 'SNES',
                                'Developer': 'Nintendo'
                            }
                        ]
                    }
                ]
            }
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(data, mode='emb')
        inserted_report = self.db.insert(converted_edman)
        oid = inserted_report[0]['sample2'][0]
        new_collection = 'new_collection'
        actual = self.db.structure('sample2', oid,
                                   structure_mode='ref',
                                   new_collection=new_collection)
        actual2 = self.db.structure('new_collection',
                                    actual[0]['new_collection'][0],
                                    structure_mode='emb',
                                    new_collection='new_collection2')

        result = self.testdb['new_collection2'].find_one(
            {'_id': actual2[0]['new_collection2'][0]})
        del result['_id']
        self.assertDictEqual(result, data['sample2'])

    def test_get_child_all(self):
        if not self.db_server_connect:
            return

        db = self.client[self.test_ini['db']]
        parent_id = ObjectId()
        child1_id = ObjectId()
        child2_id = ObjectId()
        child3_id = ObjectId()
        child4_id = ObjectId()
        parent_col = 'parent_col'
        child1_col = 'child1'
        child2_col = 'child2'
        child3_col = 'child3'
        child4_col = 'child4'
        parent_dbref = DBRef(parent_col, parent_id)
        child1_dbref = DBRef(child1_col, child1_id)
        child2_dbref = DBRef(child2_col, child2_id)
        child3_dbref = DBRef(child3_col, child3_id)
        child4_dbref = DBRef(child4_col, child4_id)
        parent_data = {
            '_id': parent_id,
            'data': 'test',
            self.parent: DBRef('storaged_test_parent', ObjectId()),
            self.child: [child1_dbref, child2_dbref]
        }
        _ = db[parent_col].insert_one(parent_data)
        child1_data = {
            '_id': child1_id,
            'data2': 'test2',
            self.parent: parent_dbref
        }
        _ = db[child1_col].insert_one(child1_data)
        child2_data = {
            '_id': child2_id,
            'data3': 'test3',
            self.parent: parent_dbref,
            self.child: [child3_dbref]
        }
        _ = db[child2_col].insert_one(child2_data)
        child3_data = {
            '_id': child3_id,
            'data4': 'test4',
            self.parent: child2_dbref,
            self.child: [child4_dbref]
        }
        _ = db[child3_col].insert_one(child3_data)
        child4_data = {
            '_id': child4_id,
            'data5': 'test5',
            self.parent: child3_dbref
        }
        _ = db[child4_col].insert_one(child4_data)

        expected = {
            child1_col: [
                {
                    '_id': child1_id, 'data2': 'test2',
                    '_ed_parent': parent_dbref
                }
            ],
            child2_col: [
                {
                    '_id': child2_id, 'data3': 'test3',
                    '_ed_parent': parent_dbref,
                    '_ed_child': [child3_dbref],
                    child3_col: [
                        {
                            '_id': child3_id, 'data4': 'test4',
                            '_ed_parent': child2_dbref,
                            '_ed_child': [child4_dbref],
                            child4_col: [
                                {
                                    '_id': child4_id,
                                    'data5': 'test5',
                                    '_ed_parent': child3_dbref
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        actual = self.db.get_child_all({parent_col: parent_data})
        # print(actual)
        self.assertDictEqual(expected, actual)

    def test_get_child(self):
        if not self.db_server_connect:
            return

        db = self.client[self.test_ini['db']]
        parent_id = ObjectId()
        child1_id = ObjectId()
        child2_id = ObjectId()
        parent_col = 'parent_col'
        child1_col = 'child1'
        child2_col = 'child2'
        parent_data = {
            '_id': parent_id,
            'data': 'test',
            self.parent: DBRef('storaged_test_parent', ObjectId()),
            self.child: [DBRef(child1_col, child1_id),
                         DBRef(child2_col, child2_id)]
        }
        _ = db[parent_col].insert_one(parent_data)
        child1_data = {
            '_id': child1_id,
            'data2': 'test2',
            self.parent: DBRef(parent_col, parent_id)
        }
        _ = db[child1_col].insert_one(child1_data)
        child2_data = {
            '_id': child2_id,
            'data3': 'test3',
            self.parent: DBRef(parent_col, parent_id)
        }
        _ = db[child2_col].insert_one(child2_data)

        # depth関連テストのみ 他のテストは内部で実行されるメソッドにおまかせ
        # 通常取得
        actual = self.db.get_child({parent_col: parent_data}, 2)
        self.assertEqual(2, len(actual))
        # 境界 childデータより多い指定
        actual = self.db.get_child({parent_col: parent_data}, 3)
        self.assertEqual(2, len(actual))
        # 0は子供データは取得できない
        actual = self.db.get_child({parent_col: parent_data}, 0)
        self.assertEqual(0, len(actual))
        # -1は指定不可能
        actual = self.db.get_child({parent_col: parent_data}, -1)
        self.assertEqual(0, len(actual))

    def test__child_storaged(self):
        if not self.db_server_connect:
            return

        # テストデータ入力
        db = self.client[self.test_ini['db']]
        parent_id = ObjectId()
        child1_id = ObjectId()
        child2_id = ObjectId()
        parent_col = 'parent_col'
        child1_col = 'child1'
        child2_col = 'child2'
        parent_dbref = DBRef(parent_col, parent_id)
        parent_data = {
            '_id': parent_id,
            'data': 'test',
            self.parent: DBRef('storaged_test_parent', ObjectId()),
            self.child: [DBRef(child1_col, child1_id),
                         DBRef(child2_col, child2_id)]
        }
        _ = db[parent_col].insert_one(parent_data)
        child1_data = {
            '_id': child1_id,
            'data2': 'test2',
            self.parent: parent_dbref,
            self.child: [DBRef('aaa', ObjectId())]
        }
        _ = db[child1_col].insert_one(child1_data)
        child2_data = {
            '_id': child2_id,
            'data3': 'test3',
            self.parent: parent_dbref,
            self.child: [DBRef('aaa', ObjectId())]
        }
        _ = db[child2_col].insert_one(child2_data)

        actual = self.db._child_storaged({parent_col: parent_data})
        # print('storaged', actual)
        self.assertIsInstance(actual, list)

        # テストデータと出力が同一かテスト
        test_cols = [{child1_col: child1_data}, {child2_col: child2_data}]
        for a, t in zip(actual, test_cols):
            with self.subTest(a=a, t=t):
                self.assertEqual(a, t)

    def test__build_to_doc_child(self):
        # データ構造のテスト
        parent_id = ObjectId()
        parent_collection = 'parent_col'
        parent_obj = DBRef(parent_collection, parent_id)
        fam_id = ObjectId()
        child3_id = ObjectId()
        data = [
            [
                {
                    'child1': {
                        '_id': ObjectId('5bfca6709663380f2c35012f'),
                        'data2': 'test2',
                        self.parent: parent_obj,
                        self.child: [
                            DBRef('aaa', ObjectId('5bfca6709663380f2c350132'))]
                    }
                },
                {
                    'child2': {
                        '_id': fam_id,
                        'data3': 'test3',
                        self.parent: parent_obj,
                        self.child: [DBRef('child3', child3_id)]
                    }
                }
            ],
            [
                {
                    'child3': {
                        '_id': child3_id,
                        'data2': 'test4',
                        self.parent: DBRef('child2', fam_id)
                    }

                }
            ]
        ]
        actual = self.db._build_to_doc_child(data)
        self.assertIsInstance(actual, dict)
        # 親子構造になっているか(child2の中のchild3が入力値と同じか)
        self.assertEqual(actual['child2'][0]['child3'][0],
                         data[1][0]['child3'])

    def test__get_uni_parent(self):
        # 正常系 構造と値のテスト
        parent_id = ObjectId()
        data = {
            'collection': [
                {
                    'name': 'Abarth 124 spider',
                    self.parent: DBRef('parent_collection', parent_id)
                },
                {
                    'name': 'MR2',
                    self.parent: DBRef('parent_collection', parent_id)
                },
            ]
        }
        actual = self.db._get_uni_parent(data)
        self.assertIsInstance(actual, ObjectId)
        self.assertEqual(parent_id, actual)

        # 異常系 兄弟間で親が違う場合(構造上ありえないが、念の為、例外のテスト)
        data = {
            'collection': [
                {
                    'name': 'Abarth 124 spider',
                    self.parent: DBRef('parent_collection', ObjectId())
                },
                {
                    'name': 'MR2',
                    self.parent: DBRef('parent_collection', ObjectId())
                },
            ]
        }
        with self.assertRaises(ValueError) as e:
            _ = self.db._get_uni_parent(data)

    def test_delete_reference(self):

        parent_coll = 'parent'
        parent_id = ObjectId()
        parent_dbref = DBRef(parent_coll, parent_id)
        child1_coll = 'child1'
        child1_id = ObjectId()
        child1_dbref = DBRef(child1_coll, child1_id)
        child2_id = ObjectId()
        child2_coll = 'child2'
        child2_dbref = DBRef(child2_coll, child2_id)
        child3_id = ObjectId()
        child3_coll = 'child3'
        child3_dbref = DBRef(child3_coll, child3_id)
        child4_id = ObjectId()
        child4_coll = 'child4'
        child4_dbref = DBRef(child4_coll, child4_id)
        child5_id = ObjectId()
        child5_coll = 'child5'
        child5a_dbref = DBRef(child5_coll, child5_id)
        child6_id = ObjectId()
        child6_coll = 'child6'
        child6_dbref = DBRef(child6_coll, child6_id)
        child7_id = ObjectId()
        child5b_dbref = DBRef(child5_coll, child7_id)
        file_ref1 = ObjectId()
        file_ref2 = ObjectId()
        file_ref3 = ObjectId()

        data = {
            '_id': parent_id,
            'name': 'Ryu',
            self.parent: DBRef('aaa', ObjectId()),
            self.child: [child1_dbref, child2_dbref],
            child1_coll:
                {
                    '_id': child1_coll,
                    'name': 'Ken',
                    self.parent: parent_dbref,
                    self.child: [child3_dbref, child4_dbref],
                    child3_coll: {
                        '_id': child3_id,
                        'name': 'E.Honda',
                        self.parent: child1_dbref
                    },
                    child4_coll: {
                        '_id': child4_id,
                        'name': 'Chun-Li',
                        self.parent: child1_dbref,
                        self.child: [child6_dbref],
                        child6_coll: {
                            '_id': child6_id,
                            'name': 'Dhalshim',
                            self.parent: child4_dbref,
                            self.file: [file_ref1, file_ref2]
                        }
                    }
                },
            child2_coll:
                {
                    '_id': child2_coll,
                    'name': 'Guile',
                    self.parent: parent_dbref,
                    self.child: [child5a_dbref, child5b_dbref],
                    child5_coll: [
                        {
                            '_id': child5_id,
                            'name': 'Blanka',
                            self.parent: child2_dbref,
                            self.file: [file_ref3]
                        },
                        {
                            '_id': child7_id,
                            'name': 'Zangief',
                            self.parent: child2_dbref
                        },
                    ]
                }

        }

        actual = self.db.delete_reference(data,
                                          (self.parent, self.child, '_id'))
        # print('delete_reference actual', actual)

        expect = {
            'name': 'Ryu',
            child1_coll:
                {
                    'name': 'Ken',
                    child3_coll: {
                        'name': 'E.Honda',
                    },
                    child4_coll: {
                        'name': 'Chun-Li',
                        child6_coll: {
                            'name': 'Dhalshim',
                            self.file: [file_ref1, file_ref2]
                        }
                    }
                },
            child2_coll:
                {
                    'name': 'Guile',
                    child5_coll: [
                        {
                            'name': 'Blanka',
                            self.file: [file_ref3]
                        },
                        {
                            'name': 'Zangief',
                        },
                    ]
                }
        }
        self.assertDictEqual(expect, actual)

    def test_get_collections(self):
        if not self.db_server_connect:
            return

        # テストデータ入力
        test_data = {
            'test_get_collections1': {
                'str_data': 'test',
                'int_data': 12,
                'float_data': 25.1,
                'bool_data': True,
                'datetime_data': datetime.now(),
            },
            'test_get_collections2': {
                'str_data': 'test',
                'int_data': 12,
                'float_data': 25.1,
                'bool_data': True,
                'datetime_data': datetime.now(),
            }
        }
        expected = []
        for collection, doc in test_data.items():
            insert_result = self.testdb[collection].insert_one(doc)
            expected.append(collection)
            # print(self.testdb[collection].find_one(
            #     {'_id': insert_result.inserted_id}))
        expected.sort()

        # 作成したデータとテストDB内のコレクションが一致するかどうかテスト
        coll_filter = {"name": {"$regex": r"^(?!system\.)"}}
        actual = self.db.get_collections(coll_filter=coll_filter)
        self.assertEqual(expected, actual)
        # print(expected, actual)

    def test_pack_list(self):

        # 正常系 (変換設定、変換対象の個数が同じ)
        input_types = ['int', 'str', 'int']
        test_list = ['1', '2', '3']
        actual = self.db.pack_list(input_types, test_list)
        expected = input_types
        self.assertListEqual(expected, actual)

        # 正常系 型の設定側が多い場合
        input_types = ['int', 'str', 'int']
        test_list = ['1', '2']
        actual = self.db.pack_list(input_types, test_list)
        expected = ['int', 'str', 'int']
        self.assertListEqual(expected, actual)

        # 正常系 変換対象のリスト側が多い場合
        input_types = ['int', 'str', 'int']
        test_list = ['1', '2', '3', '4', '5']
        actual = self.db.pack_list(input_types, test_list)
        expected = ['int', 'str', 'int', 'int', 'int']
        self.assertListEqual(expected, actual)

        # 正常系 型の設定側が一つ
        input_types = ['int']
        test_list = ['1', '2', '3', '4', '5']
        actual = self.db.pack_list(input_types, test_list)
        expected = ['int', 'int', 'int', 'int', 'int']
        self.assertListEqual(expected, actual)

    def test_bson_type(self):
        if not self.db_server_connect:
            return

        # 共通データ
        type_table = {
            'bool': bool,
            'int': int,
            'float': float,
            'str': str,
            'datetime': dateutil.parser.parse
        }

        # 正常系 すべての値を変換のテスト
        collection = 'test_get_bson_type'
        test_data = {
            collection: {
                'str_data': 'test',
                'int_data': '12',
                'float_data': '25.1',
                'bool_data': 'True',
                'datetime_data': '2018-02-21 21:46:39',
                '_ed_child': [ObjectId()]
            }
        }
        input_items = list(test_data.values())[0]
        insert_result = self.testdb[list(test_data.keys())[0]].insert_one(
            input_items)
        before_result = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})
        input_json = {
            collection: {
                'str_data': 'str',
                'int_data': 'int',
                'float_data': 'float',
                'bool_data': 'bool',
                'datetime_data': 'datetime',
            }
        }
        _ = self.db.bson_type(input_json)
        after_result = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})
        # print(before_result)
        # print(after_result)
        input_values = list(input_json.values())[0]
        for (before_k, before_v), (after_k, after_v) in zip(
                before_result.items(), after_result.items()):
            # before_vにinput_valuesのvalueにtype_tableから取ってきた型をキャスト
            # specify_type = input_values.get(before_k)
            type_cast = type_table.get(input_values.get(before_k), str)
            expected = type(type_cast(before_v))

            # after_vの型
            actual = type(after_v)
            # 1と2をassertEqualでテスト
            with self.subTest(after=expected, before=actual):
                self.assertEqual(expected, actual)

        # 正常系 指定していないデータは無変換のテスト
        collection = 'test_bson_type2'
        test_data2 = {
            collection: {
                'data1': 'test',
                'data2': '12',
                'data3': 23.4,
                '_ed_child': [ObjectId()]
            }
        }
        input_items = list(test_data2.values())[0]
        insert_result = self.testdb[list(test_data2.keys())[0]].insert_one(
            input_items)
        input_json = {
            collection: {
                'data2': 'int'
            }
        }
        _ = self.db.bson_type(input_json)
        actual = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})
        input_values = list(input_json.values())[0]
        expected = list(copy.deepcopy(test_data2).values())[0]
        del expected['_ed_child']
        del expected['_id']
        for k, v in input_values.items():
            type_cast = type_table.get(v)
            buff = type_cast(input_items.get(k))
            expected[k] = buff
        self.assertDictEqual(expected, actual)

        # 正常系 存在しないパラメータは無視
        collection = 'test_bson_type3'
        test_data3 = {
            collection: {
                'data1': 'test',
                'data2': '13',
                '_ed_child': [ObjectId()]
            }
        }
        input_items = list(test_data3.values())[0]
        insert_result = self.testdb[list(test_data3.keys())[0]].insert_one(
            input_items)
        input_json = {
            collection: {
                'data2': 'int',
                'pass_data': 'str'
            }
        }
        _ = self.db.bson_type(input_json)
        actual = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})

        # expected作成
        input_values = list(input_json.values())[0]
        expected = list(copy.deepcopy(test_data3).values())[0]
        del expected['_ed_child']
        del expected['_id']
        # input_valuesのキーがexpectedに存在しない時はexpectedからキーを消す
        for i in [k for k in input_values if expected.get(k) is None]:
            input_values.pop(i, None)

        # expectedにjsonを適応
        for k, v in input_values.items():
            type_cast = type_table.get(v)
            expected[k] = type_cast(input_items.get(k))

        # print("expected", expected, "actual", actual)
        self.assertDictEqual(expected, actual)

        # 正常系 存在しないコレクションは無視
        collection = 'test_bson_type4'
        test_data4 = {
            collection: {
                'data': '13',
                '_ed_child': [ObjectId()]
            }
        }
        input_items = list(test_data4.values())[0]
        insert_result = self.testdb[list(test_data4.keys())[0]].insert_one(
            input_items)
        test_non_collection = 'test_non_collection'
        input_json = {
            collection: {'data': 'int'},
            test_non_collection: {'data2': 'test'}
        }
        _ = self.db.bson_type(input_json)

        actual = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})

        # expected作成
        input_values = list(input_json.values())[0]
        expected = list(copy.deepcopy(test_data4).values())[0]
        del expected['_ed_child']
        del expected['_id']
        for k, v in input_values.items():
            type_cast = type_table.get(v)
            expected[k] = type_cast(input_items.get(k))

        self.assertDictEqual(expected, actual)

        # DBデータとjsonデータのリスト(正常、個数違い境界)
        # 正常系 すべての値を変換のテスト
        collection = 'test_get_bson_type5'
        test_data5 = {
            collection: {
                'str_data': 'test',
                'list_data1': ['125', 'UK', 'True'],
                'list_data2': ['1', '2', '3', '4'],
                'list_data3': ['1', '2', '3', '4', '5', '6'],
                'list_data4': ['1', '2', '3', '4', '5', '6'],
                '_ed_child': [ObjectId()]
            }
        }
        input_items = list(test_data5.values())[0]
        insert_result = self.testdb[list(test_data5.keys())[0]].insert_one(
            input_items)
        before_result = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})
        # print('before_result', before_result)
        input_json = {
            collection: {
                'str_data': 'str',
                'list_data1': ['int', 'str', 'bool'],
                'list_data2': ['str', 'int', 'str', 'int', 'int'],
                'list_data3': ['str', 'int', 'str', 'int', 'int'],
                'list_data4': ['int'],
            }
        }
        _ = self.db.bson_type(input_json)
        actual = self.testdb[collection].find_one(
            {'_id': insert_result.inserted_id},
            projection={'_id': 0, '_ed_child': 0})
        # print('actual', actual)

        # expected作成
        # データ作成が複雑になるのでホワイトテストであることを利用し、ベタに
        expected = {'str_data': 'test', 'list_data1': [125, 'UK', True],
                    'list_data2': ['1', 2, '3', 4],
                    'list_data3': ['1', 2, '3', 4, 5, 6],
                    'list_data4': [1, 2, 3, 4, 5, 6]}
        self.assertDictEqual(actual, expected)

    def test_create_user_and_role(self):
        if not self.db_server_connect:
            return

        # DB内部ユーザを作成するメソッドなので
        # 各ユーザのDBにロールとユーザ情報を作成する
        test_db_name = 'test_create_user_and_role'
        test_user_name = 'test_create_user_and_role_user'
        test_role_name = 'test_create_user_and_role_role'

        # 正常系
        self.assertIsNone(
            self.admin_db.create_user_and_role(user_name=test_user_name,
                                               db_name=test_db_name,
                                               pwd='pwd',
                                               role_name=test_role_name))
        # 後始末
        c = MongoClient(host=self.test_ini['host'],
                        port=self.test_ini['port'],
                        username=self.test_ini['admin_user'],
                        password=self.test_ini['admin_password'],
                        authSource=self.test_ini['admin_db'])
        c[test_db_name].command("dropRole", test_role_name)
        c[test_db_name].command("dropUser", test_user_name)
        c.drop_database(test_db_name)

        # 異常系
        # 接続していない
        a = DB()
        with self.assertRaises(EdmanDbProcessError):
            a.create_user_and_role(test_db_name, test_user_name, 'pwd')
        # adminじゃない
        with self.assertRaises(EdmanDbProcessError):
            self.db.create_user_and_role(test_db_name, test_user_name, 'pwd')
        # パラメータが足りない
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.create_user_and_role('', test_user_name, 'pwd')
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.create_user_and_role(test_db_name, '', 'pwd')
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.create_user_and_role(test_db_name, test_user_name,
                                               '')
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.create_user_and_role(test_db_name, test_user_name,
                                               'pwd', role_name='')

    def test_create_role(self):
        if not self.db_server_connect:
            return
        # ldap用なのでadminのDBにロールを作成する
        test_db_name = 'test_create_user_and_db'
        test_role_name = 'test_create_user_and_db_role'

        # 正常系
        self.assertIsNone(
            self.admin_db.create_role(test_db_name, test_role_name))
        # 後始末
        c = MongoClient(host=self.test_ini['host'],
                        port=self.test_ini['port'],
                        username=self.test_ini['admin_user'],
                        password=self.test_ini['admin_password'],
                        authSource=self.test_ini['admin_db'])
        c[self.test_ini['admin_db']].command("dropRole", test_role_name)
        c.drop_database(test_db_name)

        # 異常系
        # 接続していない
        a = DB()
        with self.assertRaises(EdmanDbProcessError):
            a.create_role(test_db_name, test_role_name)
        # adminじゃない
        with self.assertRaises(EdmanDbProcessError):
            self.db.create_role(test_db_name, test_role_name)
        # パラメータが足りない
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.create_role('', test_role_name)
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.create_role(test_db_name, '')

    def test_delete_user_and_role(self):
        if not self.db_server_connect:
            return
        c = MongoClient(host=self.test_ini['host'],
                        port=self.test_ini['port'],
                        username=self.test_ini['admin_user'],
                        password=self.test_ini['admin_password'],
                        authSource=self.test_ini['admin_db'])
        test_user_name = 'test_delete_user_name'
        test_db_name = 'test_delete_user_db'
        test_role_name = 'test_delete_user_role'
        c[test_db_name].command(
            "createRole",
            test_role_name,
            privileges=[
                {
                    "resource": {"db": test_db_name, "collection": ""},
                    "actions": ["changeOwnPassword"]
                }
            ],
            roles=[
                {
                    'role': 'readWrite',
                    'db': test_db_name,
                },
            ],
        )
        c[test_db_name].command(
            "createUser",
            test_user_name,
            pwd=test_user_name,
            roles=[test_role_name],
        )
        # 正常
        self.assertIsNone(
            self.admin_db.delete_user_and_role(test_user_name, test_db_name,
                                               test_role_name))
        # 異常系
        # 接続していない
        a = DB()
        with self.assertRaises(EdmanDbProcessError):
            a.delete_user_and_role(test_user_name, test_db_name,
                                   test_role_name)
        # adminじゃない
        with self.assertRaises(EdmanDbProcessError):
            self.db.delete_user_and_role(test_user_name, test_db_name,
                                         test_role_name)
        # パラメータが足りない
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.delete_user_and_role('', test_db_name,
                                               test_role_name)
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.delete_user_and_role(test_user_name, '',
                                               test_role_name)

    def test_delete_db(self):
        if not self.db_server_connect:
            return

        # テスト用ユーザ＆DB作成 テストのために手動で作成
        c = MongoClient(host=self.test_ini['host'],
                        port=self.test_ini['port'],
                        username=self.test_ini['admin_user'],
                        password=self.test_ini['admin_password'],
                        authSource=self.test_ini['admin_db'])
        test_db_name = 'delete_db_testdb'
        tdb = c[test_db_name]
        tdb.test_col.insert_one({"test_delete_db": "param1"})

        # 正常
        self.assertIsNone(
            self.admin_db.delete_db(test_db_name))
        # 正常 DB名がない=DBが存在しない時は削除せずに正常終了
        self.assertIsNone(
            self.admin_db.delete_db(""))
        # 異常
        # 接続していない
        a = DB()
        with self.assertRaises(EdmanDbProcessError):
            a.delete_db(test_db_name)

    def test_delete_role(self):
        if not self.db_server_connect:
            return

        test_db_name = 'delete_role_testdb'
        test_role_name = 'delete_role_testrole'
        # テスト用ユーザ＆DB作成 テストのために手動で作成
        c = MongoClient(host=self.test_ini['host'],
                        port=self.test_ini['port'],
                        username=self.test_ini['admin_user'],
                        password=self.test_ini['admin_password'],
                        authSource=self.test_ini['admin_db'])
        c[test_db_name].command(
            "createRole",
            test_role_name,
            privileges=[
                {
                    "resource": {"db": test_db_name, "collection": ""},
                    "actions": ["changeOwnPassword"]
                }
            ],
            roles=[
                {
                    'role': 'readWrite',
                    'db': test_db_name,
                },
            ],
        )
        # 正常
        self.assertIsNone(
            self.admin_db.delete_role(test_role_name, test_db_name))
        # 異常
        # 接続していない
        a = DB()
        with self.assertRaises(EdmanDbProcessError):
            a.delete_role(test_role_name, test_db_name)
        # adminじゃない
        with self.assertRaises(EdmanDbProcessError):
            self.db.delete_role(test_role_name, test_db_name)
        # パラメータが足りない
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.delete_role('', test_db_name)
        with self.assertRaises(EdmanDbProcessError):
            self.admin_db.delete_role(test_role_name, '')

    def test_get_ref_depth(self):
        if not self.db_server_connect:
            return

        # 正常系
        test_parent_doc_id = ObjectId()
        test_doc_id = ObjectId()
        test_parent_col = 'col1'
        test_current_col = 'col2'
        self.testdb[test_parent_col].insert_one({
            '_id': test_parent_doc_id,
            'data': 'test1',
            self.child: [DBRef(test_current_col, test_doc_id)]
        })
        self.testdb[test_current_col].insert_one({
            '_id': test_doc_id,
            'data': 'test2',
            self.parent: DBRef(test_parent_col, test_parent_doc_id)
        })

        # 上に登る
        test_get_doc = self.testdb[test_current_col].find_one(
            {'_id': test_doc_id})
        actual = self.db.get_ref_depth(test_get_doc, "_ed_parent")
        expected = 1
        self.assertEqual(expected, actual)

        # 下に下る
        test_get_doc = self.testdb[test_parent_col].find_one(
            {'_id': test_parent_doc_id})
        actual = self.db.get_ref_depth(test_get_doc, "_ed_child")
        expected = 1
        self.assertEqual(expected, actual)

        # 正常系 ドキュメント 上なし
        test_doc_id = ObjectId()
        test_doc_id_child = ObjectId()
        test_parent_col = 'col3'
        test_current_col_2 = 'col4'
        self.testdb[test_parent_col].insert_one({
            '_id': test_doc_id,
            'data': 'test3',
            self.child: [DBRef(test_current_col_2, test_doc_id_child)]
        })
        test_get_doc = self.testdb[test_parent_col].find_one(
            {'_id': test_doc_id})
        actual = self.db.get_ref_depth(test_get_doc, "_ed_parent")
        expected = 0
        self.assertEqual(expected, actual)

        # 正常系 ドキュメント 下なし
        test_doc_id = ObjectId()
        test_doc_id_parent = ObjectId()
        test_parent_col = 'col5'
        test_current_col_2 = 'col6'
        self.testdb[test_current_col_2].insert_one({
            '_id': test_doc_id,
            'data': 'test4',
            self.parent: DBRef(test_parent_col, test_doc_id_parent)
        })
        test_get_doc = self.testdb[test_current_col_2].find_one(
            {'_id': test_doc_id})
        actual = self.db.get_ref_depth(test_get_doc, "_ed_child")
        expected = 0
        self.assertEqual(expected, actual)

        # 単発のドキュメント 動作はするが本来は存在しないデータ
        test_doc_id = ObjectId()
        test_current_col = 'col8'
        self.testdb[test_current_col].insert_one({
            '_id': test_doc_id,
            'data': 'test5',
        })
        test_get_doc = self.testdb[test_current_col].find_one(
            {'_id': test_doc_id})
        actual = self.db.get_ref_depth(test_get_doc, "_ed_child")
        expected = 0
        self.assertEqual(expected, actual)

    def test__get_root_dbref(self):
        if not self.db_server_connect:
            return

        # docをDBに入れる
        parent_id = ObjectId()
        doc_id = ObjectId()
        child_id = ObjectId()
        parent_col = 'parent_col'
        doc_col = 'doc_col'
        child_col = 'child_col'
        insert_docs = [
            {
                'col': parent_col,
                'doc': {
                    '_id': parent_id,
                    'name': 'parent',
                    Config.child: [DBRef(doc_col, doc_id)]
                },
            },
            {
                'col': doc_col,
                'doc': {
                    '_id': doc_id,
                    'name': 'doc',
                    Config.parent: DBRef(parent_col, parent_id),
                    Config.child: [DBRef(child_col, child_id)]
                }
            },
            {
                'col': child_col,
                'doc': {
                    '_id': child_id,
                    'name': 'child',
                    Config.parent: DBRef(doc_col, doc_id),
                }
            }]

        result = {}
        for i in insert_docs:
            insert_result = self.testdb[i['col']].insert_one(i['doc'])
            result.update({i['col']: insert_result.inserted_id})

        # 正常系
        docs = self.db.doc(child_col, result[child_col], query=None,
                           reference_delete=False)
        # print('docs:', docs)
        actual = self.db.get_root_dbref(docs)
        expected = DBRef(parent_col, parent_id)
        self.assertEqual(expected, actual)

        # 正常系 doc要素がそもそもrootだった場合
        docs = self.db.doc(parent_col, result[parent_col], query=None,
                           reference_delete=False)
        # print('docs:', docs)
        actual = self.db.get_root_dbref(docs)
        expected = None
        self.assertEqual(expected, actual)
