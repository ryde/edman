from unittest import TestCase
from datetime import datetime
from bson import ObjectId, errors
from edman.utils import Utils


class TestConvert(TestCase):
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

        def delete(doc, keys):
            for key in keys:
                if key in doc:
                    del doc[key]

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
        with self.assertRaises((SystemExit, errors.InvalidId)) as cm:
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
