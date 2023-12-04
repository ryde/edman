from datetime import datetime
# from logging import getLogger,  FileHandler, ERROR
from logging import ERROR, StreamHandler, getLogger
from unittest import TestCase

import dateutil.parser
from bson import ObjectId, errors

from edman import Config
from edman.utils import Utils


class TestUtils(TestCase):

    def setUp(self):
        self.config = Config()
        self.parent = self.config.parent
        self.child = self.config.child
        self.date = self.config.date
        self.file = self.config.file

    @classmethod
    def setUpClass(cls):
        cls.logger = getLogger()

        # ログを画面に出力
        ch = StreamHandler()
        ch.setLevel(ERROR)  # ハンドラーにもそれぞれログレベル、フォーマットの設定が可能
        cls.logger.addHandler(ch)  # StreamHandlerの追加

        # ログをファイルに出力
        # fh = FileHandler('./tests.log')  # 引数には出力ファイルのパスを指定
        # fh.setLevel(ERROR)  # ハンドラーには、logger以下のログレベルを設定することは出来ない(この場合、DEBUGは不可)
        # cls.logger.addHandler(fh)  # FileHandlerの追加


    def test__item_literal_check(self):
        # 正常系 リスト内が全てリテラルデータ
        data = [1, 2, 3]
        self.assertTrue(Utils.item_literal_check(data))

        # 正常系 リスト内にオブジェクトを含むリテラルデータ
        data = [1, 2, ObjectId()]
        self.assertTrue(Utils.item_literal_check(data))

        # 正常系 リスト内に辞書
        data = [1, 2, {'d': 'bb'}]
        self.assertFalse(Utils.item_literal_check(data))

        # 正常系 リスト内にリスト
        data = [1, 2, ['1', 2]]
        self.assertFalse(Utils.item_literal_check(data))

        # 正常系 入力が辞書
        data = {'d': '34'}
        self.assertFalse(Utils.item_literal_check(data))

    def test_doc_traverse(self):
        # 正常系1 対象のキーを削除する
        doc = {
            'a': '1',
            'b': [
                {
                    'd': {
                        'e': '4',
                        'f': [ObjectId(), ObjectId(), ObjectId()],
                        'g': '5'
                    }
                },
                {
                    'c': '3'
                }
            ]
        }
        del_keys = ['f']
        query = ['b', '0', 'd']

        def delete(d, keys):
            for key in keys:
                if key in d:
                    del d[key]

        actual = Utils.doc_traverse(doc, del_keys, query, delete)
        expected = {
            'a': '1',
            'b': [
                {
                    'd': {
                        'e': '4',
                        'g': '5'
                    }
                },
                {
                    'c': '3'
                }
            ]
        }
        self.assertDictEqual(actual, expected)

        # 例外のテスト1 クエリのインデックスの不備
        query = ['bbb', '2', 'eee', '2']
        doc = {
            'aaa': '123',
            'bbb': [
                {'ccc': '456'}, {'ddd': '789'},
                {'eee': [
                    {'fff': {'ans': 'OK'}}, {'ggg': '1', 'hhh': 'iii'}
                ]}
            ]
        }
        oids = [ObjectId(), ObjectId(), ObjectId()]
        with self.assertRaises(IndexError):
            _ = Utils.doc_traverse(doc, oids, query, delete)

        # 例外のテスト2 クエリの指定ミス
        query = ['bbb', '2', 'eee', 'xxx']
        doc = {
            'aaa': '123',
            'bbb': [
                {'ccc': '456'}, {'ddd': '789'},
                {'eee': [
                    {'fff': {'ans': 'OK'}}, {'ggg': '1', 'hhh': 'iii'}
                ]}
            ]
        }
        oids = [ObjectId(), ObjectId(), ObjectId()]
        with self.assertRaises(ValueError):
            _ = Utils.doc_traverse(doc, oids, query, delete)

    def test_conv_objectid(self):

        # 正常系 oidの場合
        oid = ObjectId()
        actual = Utils.conv_objectid(oid)
        self.assertIsInstance(actual, ObjectId)
        self.assertEqual(oid, actual)

        # 正常系 文字列の場合
        oid = ObjectId()
        actual = Utils.conv_objectid(str(oid))
        self.assertIsInstance(actual, ObjectId)
        self.assertEqual(oid, actual)

        # 異常系 oidにならない文字列
        oid = str(ObjectId())
        oid = oid[:-1]
        with self.assertRaises(errors.InvalidId):
            _ = Utils.conv_objectid(oid)

    def test__to_datetime(self):
        # datetime正常
        input_list = ['2018/11/20', '2018/11/20 13:48', '2018/01/01 00:00:00']
        for s in input_list:
            with self.subTest(s=s):
                actual = Utils.to_datetime(s)
                self.assertIsInstance(actual, datetime)

        # 入力値が文字列だがdatetime変換できない場合、または入力値が文字列以外
        input_list = [20181120, 201811201348, 20200101000000, '8月12日', None]
        for s in input_list:
            with self.subTest(s=s):
                actual = Utils.to_datetime(s)
                self.assertIsInstance(actual, str)

    def test__query_check(self):

        # 正常系
        query = ['bbb', '2', 'eee', '0', 'fff']
        doc = {
            'aaa': '123',
            'bbb': [
                {'ccc': '456'}, {'ddd': '789'},
                {'eee': [
                    {'fff': {'ans': 'OK'}}, {'ggg': '1'}
                ]}
            ]
        }
        actual = Utils.query_check(query, doc)
        self.assertIsInstance(actual, bool)
        self.assertTrue(actual)

        # 異常系 間違ったクエリ
        query = ['bbb', '2', 'eee', '1', 'fff']  # インデックスの指定ミスを想定
        doc = {
            'aaa': '123',
            'bbb': [
                {'ccc': '456'}, {'ddd': '789'},
                {'eee': [
                    {'fff': {'ans': 'OK'}}, {'ggg': '1'}
                ]}
            ]
        }
        actual = Utils.query_check(query, doc)
        self.assertIsInstance(actual, bool)
        self.assertFalse(actual)

    def test_item_delete(self):
        # 正常系
        doc = {
            self.parent: ObjectId(),
            self.child: [ObjectId(), ObjectId()],
            self.file: [ObjectId(), ObjectId()],
            'param': 'OK'
        }
        actual = Utils.item_delete(
            doc, ('_id', self.parent, self.child, self.file))
        expected = {'param': 'OK'}
        self.assertDictEqual(actual, expected)

    def test_child_combine(self):
        # データ構造のテスト
        test_data = [
            [
                {'collection_A': {'name': 'NSX'}},
                {'collection_A': {'name': 'F355'}},
                {'collection_B': {'power': 280}}
            ]
        ]
        actual = [i for i in Utils.child_combine(test_data)]
        self.assertIsInstance(actual, list)
        self.assertEqual(2, len(actual[0]['collection_A']))

    def test_field_name_check(self):

        illegals = [None, '', '$aa', '.aa']
        for i in illegals:
            with self.subTest(i=i):
                actual = Utils.field_name_check(i)
                self.assertFalse(actual)

        # 文字列以外の方は文字列に変換される
        actual = Utils.field_name_check(455)
        self.assertTrue(actual)

    def test_collection_name_check(self):

        illegals = [None, '', '$aaa', 'aaa$b', 'system.aaa', '#aaa', '@aaa']
        for i in illegals:
            with self.subTest(i=i):
                actual = Utils.collection_name_check(i)
                self.assertFalse(actual)

        # 文字列以外の方は文字列に変換される
        actual = Utils.collection_name_check(345)
        self.assertTrue(actual)

    def test_type_cast_conv(self):
        # 正常系 変換テスト
        input_l = ['str', 'int', 'float', 'bool', 'datetime']
        expected = [str, int, float, bool, dateutil.parser.parse]
        actual = [Utils.type_cast_conv(i) for i in input_l]
        self.assertEqual(expected, actual)

        # 正常系 str変更テスト
        input_l = ['str', '12']
        expected = [str, str]
        actual = [Utils.type_cast_conv(i) for i in input_l]
        self.assertEqual(expected, actual)
