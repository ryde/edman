import configparser
import os
import tempfile
import gzip
from unittest import TestCase
from pathlib import Path
import gridfs
import datetime
import json
import zipfile
import shutil
from pymongo import errors, MongoClient
from bson import ObjectId, DBRef
from bson.json_util import dumps
from edman import Config, Convert, DB, File, Search
from edman.exceptions import EdmanDbProcessError


class TestFile(TestCase):
    db = None
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
            cls.client.admin.command('hello')
            cls.db_server_connect = True
            print('Use DB.')
        except errors.ConnectionFailure:
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
            cls.db = DB(con)
            cls.testdb = cls.db.get_db

    @classmethod
    def tearDownClass(cls):
        if cls.db_server_connect:
            # cls.clientはpymongo経由でDB削除
            # cls.testdb.dbはedman側の接続オブジェクト経由でユーザ(自分自身)の削除
            cls.client.drop_database(cls.test_ini['db'])
            cls.testdb.command("dropUser", cls.test_ini['user'])

    def setUp(self):

        if self.db_server_connect:
            self.file = File(self.db.get_db)
        else:
            self.file = File()
        self.config = Config()

    # def tearDown(self):
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

    def test_file_gen(self):

        # 正常系
        with tempfile.TemporaryDirectory() as tmp_dir:
            files = self.make_txt_files(tmp_dir, name='insert_file_ref_test',
                                        qty=2)
            expected = []
            for file_path in files:
                with file_path.open() as f:
                    expected.append((file_path.name, f.read()))

            # ファイル読み込み、テスト
            actual = []
            # files = tuple(sorted(p.glob('gen_test*.txt')))
            for idx, f in enumerate(self.file.file_gen(tuple(files))):
                filename, filedata = f
                actual.append((filename, filedata.decode()))
            self.assertListEqual(sorted(actual), sorted(expected))

    def test_file_list_attachment(self):

        # _ed_fileがなかった場合
        doc = {'name': 'NSX', 'ddd': 'aaa'}
        files_oid = [ObjectId(), ObjectId()]
        actual = self.file.file_list_attachment(doc, files_oid)
        expected = {'name': 'NSX', 'ddd': 'aaa', self.config.file: files_oid}
        self.assertDictEqual(expected, actual)

        # _ed_fileがすでに存在する場合
        oid1 = ObjectId()
        oid2 = ObjectId()

        files_oid = [oid1, oid2]
        doc = {'abc': '123', self.config.file: files_oid}
        at_files_oid = [ObjectId(), ObjectId()]
        actual = self.file.file_list_attachment(doc, at_files_oid)

        files_oid2 = [oid1, oid2]
        expected = {'abc': '123', self.config.file: files_oid2 + at_files_oid}

        self.assertDictEqual(actual, expected)

    def test_file_list_replace(self):
        # _ed_fileがある場合
        files_oid = [ObjectId(), ObjectId()]
        doc = {'name': 'NSX', self.config.file: files_oid}
        rep_files_oid = [ObjectId(), ObjectId()]
        actual = self.file.file_list_replace(doc, rep_files_oid)
        expected = {'name': 'NSX', self.config.file: rep_files_oid}
        self.assertDictEqual(expected, actual)

        # 空リストの場合
        doc = {'name': 'NSX', self.config.file: [ObjectId(), ObjectId()]}
        rep_files_oid = []
        actual = self.file.file_list_replace(doc, rep_files_oid)
        expected = {'name': 'NSX'}
        self.assertDictEqual(expected, actual)

        # _ed_fileがない場合(例外になる)
        doc = {'name': 'NSX'}
        rep_files_oid = [ObjectId(), ObjectId()]
        with self.assertRaises(ValueError):
            _ = self.file.file_list_replace(doc, rep_files_oid)

    def test_fs_delete(self):
        if not self.db_server_connect:
            return

        # 正常系
        with tempfile.TemporaryDirectory() as tmp_dir:
            fs_oids = []
            for i in tuple(
                    self.make_txt_files(tmp_dir, name='delete_test', qty=2)):
                with i.open('rb') as f:
                    self.fs = gridfs.GridFS(self.testdb)
                    fs_oids.append(
                        self.fs.put(f.read(), filename=str(i.name)))

            self.file.fs_delete(fs_oids)
            for i in fs_oids:
                with self.subTest(i=i):
                    self.assertFalse(self.fs.exists(i))

    def test_get_file_names(self):
        if not self.db_server_connect:
            return

        with tempfile.TemporaryDirectory() as tmp_dir:
            files = self.make_txt_files(tmp_dir, name='file_names', qty=2)

            # ファイル読み込み、ファイルをgridfsに入れる
            files_oid = []
            self.fs = gridfs.GridFS(self.testdb)
            for filename in files:
                with filename.open('rb') as f:
                    files_oid.append(
                        self.fs.put(f.read(), filename=filename.name))

            # DocをDBに入れる
            doc = {
                'test1': {
                    'test2': 'name',
                    'test3': {
                        'test4': [
                            {
                                'test5': 'moon',
                                self.config.file: files_oid,
                            },
                            {'test6': 'star'}
                        ]
                    }
                }
            }
            insert_result = self.testdb['structure_emb'].insert_one(doc)

            # 正常系テスト(embのみ.refとembの判断はget_file_ref()で行っているので割愛)
            oid = insert_result.inserted_id
            query = ['test1', 'test3', 'test4', '0']
            actual = self.file.get_file_names('structure_emb', oid, 'emb',
                                              query)
            expected = dict(zip(files_oid, [i.name for i in files]))
            self.assertDictEqual(actual, expected)

            # 正常系 ファイルがなかった場合 →空のリストを出力
            doc = {
                'test1': {
                    'test2': 'name2',
                    'test3': {
                        'test4': [
                            {'test5': 'moon2'},
                            {'test6': 'star2'}
                        ]
                    }
                }
            }
            insert_result = self.testdb['structure_ref'].insert_one(doc)
            oid = insert_result.inserted_id
            actual = self.file.get_file_names('structure_ref', oid, 'ref')
            self.assertDictEqual(actual, {})

            # 異常系
            # ドキュメントがなかった場合 →EdmanDbProcessErrorを補足
            oid = ObjectId()
            with self.assertRaises(EdmanDbProcessError):
                _ = self.file.get_file_names('structure_ref', oid, 'ref')

    def test__get_file_ref(self):
        # 正常系(ref)
        expected = [ObjectId(), ObjectId(), ObjectId()]
        doc = {
            'name': 'test1',
            self.config.file: expected,
            self.config.parent: DBRef('test0', ObjectId()),
            self.config.child: [DBRef('test2', ObjectId()),
                                DBRef('test3', ObjectId())]
        }
        structure = 'ref'
        actual = self.file.get_file_ref(doc, structure)
        self.assertListEqual(expected, actual)

        # 正常系(emb)
        expected = [ObjectId(), ObjectId(), ObjectId()]
        doc = {
            'test1': {
                'test2': 'name',
                'test3': {
                    'test4': [
                        {
                            'test5': 'moon',
                            self.config.file: expected,
                        },
                        {'test6': 'star'}
                    ]
                }
            }
        }
        structure = 'emb'
        query = ['test1', 'test3', 'test4', '0']
        actual = self.file.get_file_ref(doc, structure, query)
        self.assertListEqual(expected, actual)

    def test_delete(self):
        if not self.db_server_connect:
            return

        with tempfile.TemporaryDirectory() as tmp_dl_dir:

            # embの添付ファイル削除テスト
            # ファイル読み込み、ファイルをgridfsに入れる
            files_oid = []
            self.fs = gridfs.GridFS(self.testdb)
            dl_files = self.make_txt_files(tmp_dl_dir, name='file_delete')
            for filename in dl_files:
                with filename.open('rb') as f:
                    files_oid.append(
                        self.fs.put(f.read(), filename=filename.name))

            # DocをDBに入れる
            doc = {
                'test1': {
                    'test2': 'name',
                    'test3': {
                        'test4': [
                            {
                                'test5': 'moon',
                                self.config.file: files_oid,
                            },
                            {'test6': 'star'}
                        ]
                    }
                }
            }
            insert_result = self.testdb['structure_emb'].insert_one(doc)
            oid = str(insert_result.inserted_id)
            query = ['test1', 'test3', 'test4', '0']

            result = self.file.delete(files_oid[0], 'structure_emb', oid,
                                      'emb', query)
            self.assertTrue(result)
            # ファイルが消えたか検証
            self.assertFalse(self.fs.exists(files_oid[0]))

            # refの添付ファイル削除テスト
            # ファイル読み込み、ファイルをgridfsに入れる
            files_oid = []
            self.fs = gridfs.GridFS(self.testdb)
            for filename in dl_files:
                with filename.open('rb') as f:
                    files_oid.append(
                        self.fs.put(f.read(), filename=filename.name))

            # docをDBに入れる
            doc = {'name': 'test1',
                   self.config.file: files_oid,
                   self.config.parent: ObjectId(),
                   self.config.child: DBRef('child_col', ObjectId())}

            insert_result = self.testdb['structure_ref'].insert_one(doc)
            oid = str(insert_result.inserted_id)

            result = self.file.delete(files_oid[0], 'structure_ref', oid,
                                      'ref')
            self.assertTrue(result)
            # ファイルが消えたか検証
            self.assertFalse(self.fs.exists(files_oid[0]))

    def test__get_emb_files_list(self):

        # 正常系
        file_list = [ObjectId(), ObjectId()]

        doc = {
            "_id": ObjectId(),
            "position": "top",
            "username": "ryde",
            "bool": True,
            "structure_2": [
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
                    "structure_3_1": [
                        {
                            "filename": "test1.txt",
                            "name": "添付ファイル1"
                        },
                        {
                            "filename": "test2.txt",
                            "name": "添付ファイル2"
                        }
                    ],
                    "structure_3_2": [
                        {
                            "filename": "test3.txt",
                            "name": "添付ファイル3",
                            "structure_4": {
                                "filename": "test4.txt",
                                "name": "添付ファイル4",
                                "structure_5": [
                                    {
                                        "url": "example2.com",
                                        "name": "テストURL2"
                                    },
                                    {
                                        "url": "example3.com",
                                        "name": "テストURL3"
                                    }
                                ]
                            },
                            "structure_6": [
                                {
                                    "url": "example_x.com",
                                    "name": "テストURL_x"
                                },
                                {
                                    "url": "example_y.com",
                                    "name": "テストURL_y",
                                    self.config.file: file_list,
                                }
                            ],
                            "structure_5": {
                                "url": "example.com",
                                "name": "テストURL1",
                                "structure_5": {
                                    "url": "example4.com",
                                    "name": "テストURL4"
                                }
                            }
                        }
                    ]
                }
            ]
        }

        query = ['structure_2', '1', 'structure_3_2', '0', 'structure_6', '1']
        actual = self.file._get_emb_files_list(doc, query)

        expected = file_list
        self.assertEqual(expected, actual)

    def test_zipped_json(self):

        # # 正常系
        s = {"document": [{"test1": "ABC"}, {"test2": 12345}]}
        raw_json = dumps(s, ensure_ascii=False, indent=4)
        encoded_json = raw_json.encode('utf-8')
        filename = 'jsonfile'

        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            ac_z_path = self.file.zipped_json(encoded_json, filename, p)

            # 圧縮前のjsonファイルを削除する
            first_file = filename + '.json'
            first = p / first_file
            first.unlink()

            # ac_z_pathを解凍して中身のファイル名がzipped_filepathと同じか調べる
            with zipfile.ZipFile(ac_z_path, 'r') as inputFile:
                inputFile.extractall(tmpdir)

            # 解凍されたディレクトリ名を取得する
            ps = list(p.glob('**/*.json'))[0]
            actual = ps.stem

        expected = filename
        self.assertEqual(expected, actual)

    def test_zipped_contents(self):
        if not self.db_server_connect:
            return

        with tempfile.TemporaryDirectory() as tmp_dl_dir:
            # 添付ファイル用テキストファイル作成

            test_var = 'test'
            name = 'file_dl_list.txt'
            input_filename = os.path.splitext(os.path.basename(name))[0]
            file_dl_list = self.make_txt_files(tmp_dl_dir, name=input_filename,
                                               text=test_var)

            # ファイル読み込み、ファイルをgridfsに入れる
            files_oid = []
            self.fs = gridfs.GridFS(self.testdb)
            for filename in file_dl_list:
                metadata = {'filename': filename.name, 'compress': None}
                with filename.open('rb') as f:
                    files_oid.append(self.fs.put(f.read(), **metadata))

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
                        Config.file: files_oid,
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

            # # ドキュメントを取得する
            search = Search(self.db)
            exclusion = ['_id', Config.file]
            docs = search.find(
                doc_col, {'_id': result[doc_col]},
                1, 1, exclusion)
            result_with_filepath, downloads = (
                self.file.get_fileref_and_generate_dl_list(
                    docs, "_ed_attachment"))
            res = search.process_data_derived_from_mongodb(
                result_with_filepath)

        # 実行
        raw_json = dumps(res, ensure_ascii=False, indent=4)
        encoded_json = raw_json.encode('utf-8')
        json_tree_file_name = 'json_tree'

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            zip_filepath = self.file.zipped_contents(downloads,
                                                     json_tree_file_name,
                                                     encoded_json, p)
            # 解凍
            # print('unpack before',list(p.iterdir()))
            shutil.unpack_archive(zip_filepath, tmp)
            # print('unpack after', list(p.iterdir()))

            # 添付ファイルを読み込んでテキストの中身を抽出
            path_lists = [tmp, str(list(downloads.keys())[0]), name]
            with open(os.path.join(*path_lists)) as f:
                attached_s = f.read()
            path_lists = [tmp, json_tree_file_name + '.json']
            json_file_path = os.path.join(*path_lists)
            with open(json_file_path) as jf:
                j_data = json.load(jf)
            # 外に出すデータを組み立て(zip内部のjsonデータ、上記の添付ファイルデータ)
            actual_data = {'json_data': j_data, 'attached_data': attached_s}
        # テスト
        self.assertDictEqual(res, actual_data['json_data'])
        self.assertEqual(test_var, actual_data['attached_data'])

    def test_get_fileref_and_generate_dl_list(self):

        if not self.db_server_connect:
            return

        # DBにdocsと添付ファイルを挿入する
        with tempfile.TemporaryDirectory() as tmp_dl_dir:
            name = 'file_dl_list.txt'
            # ファイル読み込み、ファイルをgridfsに入れる
            files_oid = []
            self.fs = gridfs.GridFS(self.testdb)
            input_filename = os.path.splitext(os.path.basename(name))[0]
            for filename in self.make_txt_files(tmp_dl_dir, input_filename):
                with filename.open('rb') as f:
                    files_oid.append(
                        self.fs.put(f.read(), filename=filename.name))
            # docをDBに入れる
            child_oid = ObjectId()
            doc = {'name': 'test1',
                   self.config.file: files_oid,
                   self.config.child: DBRef('child_col', child_oid)}
            insert_result = self.testdb[
                'test__get_fileref_and_generate_dl_list'].insert_one(doc)
            # ドキュメントを取得する
            res = self.testdb[
                'test__get_fileref_and_generate_dl_list'].find_one(
                {'_id': insert_result.inserted_id})
            # 実行
            new_docs, dl_list = self.file.get_fileref_and_generate_dl_list(
                res, '_ed_attachment')
            # 正常系 docsを比較
            expected = {'_id': insert_result.inserted_id, 'name': 'test1',
                        '_ed_attachment': [
                            str(insert_result.inserted_id) + '/' + name],
                        '_ed_child': DBRef('child_col', child_oid)}
            actual = new_docs
            self.assertDictEqual(expected, actual)

            # 正常系 DL処理用の辞書を比較
            expected = {insert_result.inserted_id: files_oid}
            actual = dl_list
            self.assertDictEqual(expected, actual)

    def test_generate_zip_filename(self):

        # 正常系
        # ファイル名を指定
        filename = 'testFileName'
        now = datetime.datetime.now()
        name = now.strftime('%Y%m%d%H%M%S')
        expected = name + filename + '.zip'
        actual = self.file.generate_zip_filename(filename)
        self.assertEqual(expected, actual)

        # ファイル名指定なし
        now = datetime.datetime.now()
        name = now.strftime('%Y%m%d%H%M%S')
        expected = name + '.zip'
        actual = self.file.generate_zip_filename()
        self.assertEqual(expected, actual)

        # 正常系
        # 文字列以外
        filename = 2545
        now = datetime.datetime.now()
        name = now.strftime('%Y%m%d%H%M%S')
        filename_str = str(filename)
        expected = name + filename_str + '.zip'
        actual = self.file.generate_zip_filename(filename)
        self.assertEqual(expected, actual)

    def test_grid_out(self):
        if not self.db_server_connect:
            return

        with tempfile.TemporaryDirectory() as tmp_dir:
            files = self.make_txt_files(tmp_dir, name='file_dl', qty=2)

            # ファイル読み込み、ファイルをgridfsに入れる
            files_oid = []
            self.fs = gridfs.GridFS(self.testdb)
            test_vars = {}
            for filename in files:
                with filename.open('rb') as f:
                    content = f.read()
                    # file_obj = gzip.compress(content, compresslevel=6)
                    # files_oid.append(
                    #     self.fs.put(file_obj, filename=filename.name,
                    #                 compress='gzip'))
                    files_oid.append(
                        self.fs.put(content, filename=filename.name))
                    test_vars.update({filename.name: content.decode()})

        with tempfile.TemporaryDirectory() as tmp_dl_dir:
            path = Path(tmp_dl_dir)

            grid_out_result = self.file._grid_out(files_oid, tmp_dl_dir)
            # 実行結果
            self.assertTrue(grid_out_result)

            # DB送信前のファイル名＆ファイルの中身と、ダウンロード後のファイル名&ファイルの中身を比較する
            expected = {}
            for dl_file in path.glob('*.txt'):
                with dl_file.open() as f:
                    expected.update({dl_file.name: f.read()})
            self.assertDictEqual(test_vars, expected)

    def test_upload(self):
        if not self.db_server_connect:
            return

        # refの場合
        # refデータ入力
        ref_json = {
            "structure_1": [
                {
                    "position": "top",
                    "username": "ryde",
                    "structure_2": [
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
                            "start_date": {"#date": "1981-04-23"},
                            "structure_3_1": [
                                {
                                    "filename": "test1.txt",
                                    "name": "添付ファイル1"
                                },
                                {
                                    "filename": "test2.txt",
                                    "name": "添付ファイル2"
                                }
                            ],
                            "structure_3_2": [
                                {
                                    "filename": "test3.txt",
                                    "name": "添付ファイル3",
                                    "structure_4": {
                                        "filename": "test4.txt",
                                        "name": "添付ファイル4",
                                        "structure_5": [
                                            {
                                                "url": "example2.com",
                                                "name": "テストURL2"
                                            },
                                            {
                                                "url": "example3.com",
                                                "name": "テストURL3"
                                            }
                                        ]
                                    },
                                    "structure_6": [
                                        {
                                            "url": "example_x.com",
                                            "name": "テストURL_x"
                                        },
                                        {
                                            "url": "example_y.com",
                                            "name": "テストURL_y"
                                        }
                                    ],
                                    "structure_5": {
                                        "url": "example.com",
                                        "name": "テストURL1",
                                        "structure_5": {
                                            "url": "example4.com",
                                            "name": "テストURL4"
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        convert = Convert()
        converted_edman = convert.dict_to_edman(ref_json, mode='ref')
        insert_result = self.db.insert(converted_edman)

        with tempfile.TemporaryDirectory() as tmp_dir:
            files = self.make_txt_files(tmp_dir, name='insert_file_ref_test',
                                        qty=2)
            expected = []
            for file_path in files:
                with file_path.open() as f:
                    expected.append((file_path.name, f.read()))

            # メソッド実行
            collection = 'structure_5'
            oid = [i[collection][0] for i in insert_result if collection in i][
                0]
            p_files = tuple([(i, True) for i in files])
            insert_file_result = self.file.upload(collection, oid,
                                                  p_files,
                                                  'ref')

            # DBからデータ取得
            query = {'_id': oid}
            result = self.testdb[collection].find_one(query)

            # 取得したデータからgridfsのファイルを取得
            actual = []
            self.fs = gridfs.GridFS(self.testdb)
            for file_oid in result[self.config.file]:
                data = self.fs.get(file_oid)
                d = data.read()
                if hasattr(data, 'compress') and data.compress == 'gzip':
                    d = gzip.decompress(d)
                actual.append((data.filename, d.decode()))

            # テスト
            self.assertTrue(insert_file_result)
            self.assertListEqual(sorted(actual), sorted(expected))

        # embの場合
        data = {
            "position": "top",
            "structure_2": [
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
                    "structure_3_1": [
                        {
                            "filename": "test1.txt",
                            "name": "添付ファイル1"
                        },
                        {
                            "filename": "test2.txt",
                            "name": "添付ファイル2"
                        }
                    ],
                    "structure_3_2": [
                        {
                            "filename": "test3.txt",
                            "name": "添付ファイル3",
                            "structure_4": {
                                "filename": "test4.txt",
                                "name": "添付ファイル4",
                                "structure_5": [
                                    {
                                        "url": "example2.com",
                                        "name": "テストURL2"
                                    },
                                    {
                                        "url": "example3.com",
                                        "name": "テストURL3"
                                    }
                                ]
                            },
                            "structure_6": [
                                {
                                    "url": "example_x.com",
                                    "name": "テストURL_x"
                                },
                                {
                                    "url": "example_y.com",
                                    "name": "テストURL_y"
                                }
                            ],
                            "structure_5": {
                                "url": "example.com",
                                "name": "テストURL1",
                                "structure_5": {
                                    "url": "example4.com",
                                    "name": "テストURL4"
                                }
                            }
                        }
                    ]
                }
            ]
        }
        insert_result = self.testdb['structure_emb'].insert_one(data)

        with tempfile.TemporaryDirectory() as tmp_dir:
            files_obj = self.make_txt_files(tmp_dir, name='emb_test', qty=2)
            expected = []
            for file_path in files_obj:
                with file_path.open() as f:
                    expected.append(f.read())

            collection = 'structure_emb'
            oid = insert_result.inserted_id
            query = ['structure_2', '1']

            files_obj2 = tuple([(i, True) for i in files_obj])
            # メソッド実行
            insert_file_emb_result = self.file.upload(
                collection, oid, files_obj2, 'emb', query)

            # ドキュメントをfindして出す
            result = self.testdb[collection].find_one({'_id': oid})

            # gridfsからデータ取得
            out_data = []
            self.fs = gridfs.GridFS(self.testdb)
            for oid in result['structure_2'][1][self.config.file]:
                data = self.fs.get(oid)
                f_data = data.read()
                if hasattr(data, 'compress') and data.compress == 'gzip':
                    f_data = gzip.decompress(f_data)

                out_data.append(f_data.decode())

            # DBデータとファイルのデータに差異はないか
            self.assertListEqual(sorted(expected), sorted(out_data))

            # メソッド成功時のフラグ
            self.assertTrue(insert_file_emb_result)

    def test_grid_in(self):
        if not self.db_server_connect:
            return

        # 正常系
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_files = self.make_txt_files(tmp_dir, name='grid_in_test',
                                               qty=2)
            compress_settings = [False, False]
            td = tuple(
                [(p, b) for (p, b) in zip(sample_files, compress_settings)])

            self.fs = gridfs.GridFS(self.testdb)
            actual = []
            for oid in self.file.grid_in(td):
                data = self.fs.get(oid)
                f_data = data.read().decode()
                b_data = False
                actual.append([data.filename, f_data, b_data])

            expected = []
            for file_path, compress in zip(sample_files, compress_settings):
                with file_path.open() as f:
                    expected.append([file_path.name, f.read(), compress])

            self.assertListEqual(sorted(actual), sorted(expected))

    def test_generate_file_path_dict(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_p = Path(tmp)
            p = tmp_p / 'sub'
            p.mkdir()
            sample_files = self.make_txt_files(
                p,
                name='generate_file_path_dict_test',
                qty=2)
            file_list = list(map(str, sample_files))
            expected = {k: v for k, v in zip(file_list, sample_files)}
            actual = self.file.generate_file_path_dict(file_list, p)

            # paths = [(v, False) for k, v in actual.items()]
            # data = tuple(paths)
            # print('data', data)
            # grid_in_results = self.file.grid_in(data)
            # gf_inserted_dict = {}
            # for i, j in zip(actual, grid_in_results):
            #     gf_inserted_dict.update({i: j})
            # print('gf_inserted_dict', gf_inserted_dict)

            self.assertDictEqual(actual, expected)

    def test_generate_upload_list(self):
        json_dict = {
            "position": "top",
            "structure_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 380,
                    "_ed_attachment": ['aaa/01.jpg']
                },
                {
                    "maker": "Ferrari",
                    "carname": "458 Italia",
                    "_ed_attachment": ['bbb/01.jpg', 'bbb/02.jpg'],

                }],
            "structure_3": {
                "structure_4": {
                    "data1": 45,
                    "_ed_attachment": ['ccc/01.jpg']}
            },
            "structure_5": {
                "structure_6": {
                    "data1": ["123", "456"]
                },
            }

        }
        expected = ['aaa/01.jpg', 'bbb/01.jpg', 'bbb/02.jpg', 'ccc/01.jpg']
        actual = self.file.generate_upload_list(json_dict)
        # print(actual)
        self.assertListEqual(actual, expected)

    def test_json_rewrite(self):
        if not self.db_server_connect:
            return

        json_dict = {
            "position": "top",
            "structure_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 380,
                    "_ed_attachment": ['aaa/01.jpg']
                },
                {
                    "maker": "Ferrari",
                    "carname": "458 Italia",
                    "list_test": ["123", "456"],
                    "_ed_attachment": ['bbb/01.jpg', 'bbb/02.jpg'],

                }],
            "structure_3": {
                "structure_4": {
                    "data1": 45,
                    "_ed_attachment": ['ccc/01.jpg']}
            }
        }
        a01_file = ObjectId()
        b01_file = ObjectId()
        b02_file = ObjectId()
        c01_file = ObjectId()
        files_dict = {'aaa/01.jpg': a01_file,
                      'bbb/01.jpg': b01_file,
                      'bbb/02.jpg': b02_file,
                      'ccc/01.jpg': c01_file}
        expected = {
            "position": "top",
            "structure_2": [
                {
                    "maker": "Ferrari",
                    "carname": "F355",
                    "power": 380,
                    "_ed_file": [a01_file]
                },
                {
                    "maker": "Ferrari",
                    "carname": "458 Italia",
                    "list_test": ["123", "456"],
                    "_ed_file": [b01_file, b02_file]
                }],
            "structure_3": {
                "structure_4": {
                    "data1": 45,
                    "_ed_file": [c01_file]}
            }
        }
        actual = self.file.json_rewrite(json_dict, files_dict)
        # print(actual)
        self.assertDictEqual(actual, expected)
