from unittest import TestCase
from datetime import datetime
from bson import ObjectId, DBRef
from edman.convert import Convert
from edman.config import Config


class TestConvert(TestCase):

    def setUp(self):
        self.convert = Convert()
        self.config = Config()
        self.parent = self.config.parent
        self.child = self.config.child
        self.date = self.config.date

    def test__get_child_reference(self):

        # すでにparentが作られていた場合
        # (この関数後の処理にDBRefオブジェクトが0個の時の処理が書かれているため0個でもOK)
        test_data = {self.parent: {'_id': ObjectId()}}
        actual = self.convert._get_child_reference(test_data)
        self.assertEqual(0, len(list(actual.values())[0]))

        # テスト辞書データの中身が辞書の場合
        test_data = {'test_collection': {'_id': ObjectId()}}
        actual = self.convert._get_child_reference(test_data)
        self.assertIsInstance(actual, dict)
        self.assertEqual(self.config.child, list(actual.keys())[0])
        self.assertEqual(1, len(list(actual.values())[0]))

        # テスト辞書データの中身がリストの場合
        test_data = {
            'test_collection': [
                {'_id': ObjectId()},
                {'_id': ObjectId()}
            ]
        }
        actual = self.convert._get_child_reference(test_data)
        self.assertEqual(2, len(list(actual.values())[0]))

    def test__convert_datetime(self):
        # データ構造のテスト
        test_data = {'start_date': {'#date': '1981-04-23'}}
        actual = self.convert._convert_datetime(test_data)
        self.assertIsInstance(actual, dict)
        self.assertIsInstance(list(actual.values())[0], datetime)

    def test__list_organize(self):
        # データ構造のテスト
        test_data = [
            {
                'honda': [
                    {'name': 'NSX', 'power': '280'}
                ]
            },
            {
                'honda': [
                    {'name': 'S800', 'first model year': '1966'}
                ]
            }
        ]
        actual = self.convert._list_organize(test_data)
        self.assertIsInstance(actual, list)
        self.assertEqual('honda', list(actual[0].keys())[0])
        self.assertEqual(2, len(list(actual[0].values())[0]))
        for i in list(actual[0].values())[0]:
            with self.subTest(i=i):
                self.assertIsInstance(i, dict)

    def test__list_intercept_hook(self):

        data = {
            'parent_data': 'data1',
            'delete_coll': {'data': '1', 'data2': '2'},
            'child': [
                DBRef('test_collection', ObjectId()),
                DBRef('test_collection', ObjectId())]}
        actual = self.convert._list_intercept_hook('test_coll', data)

        # 子要素のデータ(この場合はdata['delete_coll'])が削除されているか
        self.assertIsInstance(actual, dict)
        for k, v in actual.items():
            with self.subTest(v=v):
                self.assertNotIn('delete_coll', v[0])

    def test__attached_oid(self):
        # 辞書内辞書の場合
        test_data = {
            'collection': {
                'car_name': 'NSX',
                'Purchase year': '1993'
            }
        }
        actual = self.convert._attached_oid(test_data)
        self.assertIsInstance(actual, dict)
        self.assertTrue(True if '_id' in list(actual.values())[0] else False)
        self.assertIsInstance(list(actual.values())[0]['_id'], ObjectId)

        # 辞書内リストの場合
        test_data = {
            'collection': [
                {
                    'car_name': 'NSX',
                    'Purchase year': '1993'
                }
            ]
        }
        actual = self.convert._attached_oid(test_data)
        self.assertTrue(
            True if '_id' in list(actual.values())[0][0] else False)
        self.assertIsInstance(list(actual.values())[0][0]['_id'], ObjectId)

    def test__collection_name_check(self):

        illegals = [None, '', '$aaa', 'aaa$b', 'system.aaa', '#aaa', '@aaa']
        for i in illegals:
            with self.subTest(i=i):
                actual = self.convert._collection_name_check(i)
                self.assertFalse(actual)

        # 文字列以外の方は文字列に変換される
        actual = self.convert._collection_name_check(345)
        self.assertTrue(actual)

    def test__field_name_check(self):

        illegals = [None, '', '$aa', '.aa']
        for i in illegals:
            with self.subTest(i=i):
                actual = self.convert._field_name_check(i)
                self.assertFalse(actual)

        # 文字列以外の方は文字列に変換される
        actual = self.convert._field_name_check(455)
        self.assertTrue(actual)

    def test__date_replace(self):
        list_data = [{self.date: '2019-02-28'},
                     {self.date: '2019-03-01 13:56:28'},
                     1,
                     'text']
        expected = [datetime(2019, 2, 28),
                    datetime(2019, 3, 1, 13, 56, 28),
                    1,
                    'text']
        actual = self.convert._date_replace(list_data)
        self.assertListEqual(expected, actual)

    def test__ref(self):
        pass
        # テスト用jsonの読み込み
        # with open('./test_json_files/ref_test_premo.json') as f:
        #     jsondict = json.load(f)
        #
        # actual = self.convert._ref(jsondict)
        #
        # with open('./test_json_files/ref_test_premo_result.json', 'w') as f:
        #     f.write(dumps(actual, ensure_ascii=False, indent=4))
        # TODO あとでテストデータを追加し、assertできるようにする

    def test_emb(self):
        pass
        # with open('./test_json_files/ref_test_premo.json') as f:
        #     jsondict = json.load(f)
        #
        # actual = self.convert._emb(jsondict)
        # with open('./test_json_files/emb_test_premo_result.json', 'w') as f:
        #     f.write(dumps(actual, ensure_ascii=False, indent=4))
        # TODO あとでテストデータを追加し、assertできるようにする

    #
    # def test_dict_to_edman(self):
    #     # 中身は他のメソッドなのでテストはパス
    #     pass
