import re
import sys
import copy
from typing import Union
from collections import defaultdict
from bson import ObjectId, DBRef
from edman.utils import Utils
from edman import Config


class Convert:
    """
    研究データをEdman用にコンバートするクラス
    """

    def __init__(self) -> None:
        config = Config()  # システム環境用の設定を読み込む
        self.parent = config.parent
        self.child = config.child
        self.date = config.date

    @staticmethod
    def _collection_name_check(collection_name: str) -> bool:
        """
        MongoDBの命名規則チェック(コレクション名)
                # $ None(null) '' system.
        最初がアンスコか文字
        mongoの制約の他に頭文字に#もNG

        コレクション名空間の最大長は、データベース名、ドット（.）区切り文字
        およびコレクション名（つまり <database>.<collection>）を合わせて
        120バイトを超えないこと
        ただし、子のメソッド利用時はDB名を取得するタイミングではないため、
        文字数のチェックは行えないことを留意すること

        https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names

        :param str collection_name:
        :return: bool
        """
        if collection_name is None:
            return False

        if not isinstance(collection_name, str):
            collection_name = str(collection_name)

        collection_name_length = len(collection_name)
        if collection_name_length == 0:
            return False

        if '$' in collection_name:
            return False

        if collection_name.startswith(
                'system.') or collection_name.startswith('#'):
            return False

        # 先頭に記号があるとマッチする
        if re.match(r'(\W)', collection_name) is not None:
            return False

        return True

    @staticmethod
    def _field_name_check(field_name: str) -> bool:
        """
        MongoDBの命名規則チェック(フィールド名)
        void, None(Null), 文字列先頭に($ .)は使用不可

        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names

        :param str field_name:
        :return: bool
        """
        if field_name is None:
            return False

        if not isinstance(field_name, str):
            field_name = str(field_name)

        if len(field_name) == 0:
            return False

        if field_name[0] in ('$', '.'):
            return False

        return True

    def _get_child_reference(self, child_data: dict) -> dict:
        """
        子データのリファレンス情報を作成して取得

        :param dict child_data:
        :return: dict
        """
        children = []
        for collection, child_value in child_data.items():

            # すでにparentが作られている場合は飛ばす
            if self.parent == collection:
                continue
            if isinstance(child_value, dict):
                children.append(DBRef(collection, child_value['_id']))
            elif isinstance(child_value, list) and (
                    not Utils.item_literal_check(child_value)):
                child_list = [DBRef(collection, j['_id']) for j in child_value]
                children.extend(child_list)
            else:
                continue

        return {self.child: children}

    def _convert_datetime(self, child_dict: dict) -> dict:
        """
        辞書内辞書になっている文字列日付時間データを、辞書内日付時間に変換

        (例)
        {'start_date': {'#date': '1981-04-23'}}
        から
        {'start_date': 1981-04-23T00:00:00}

        :param dict child_dict:
        :return: dict result
        """
        result = copy.deepcopy(child_dict)
        if isinstance(child_dict, dict):
            try:
                for key, value in child_dict.items():

                    if isinstance(value, dict) and self.date in value:
                        result.update(
                            {
                                key: Utils.to_datetime(
                                    child_dict[key][self.date])
                            })
            except AttributeError:
                sys.exit(f'日付変換に失敗しました.構造に問題があります. {child_dict}')
        return result

    @staticmethod
    def _list_organize(extracted_data: list) -> list:
        """
        リスト内辞書のデータから、同じコレクションの場合、
        値(リストになっている)を取り出し、マージさせる
        バルクインサートを利用するために、
        同じコレクションでまとめたほうが効率が良いため
        (例)
        [
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
        から
        [
            {
                'honda': [
                    {'name': 'NSX', 'power': '280'},
                    {'name': 'S800', 'first model year': '1966'}
                ]
            }
        ]

        :param list extracted_data:
        :return: list result
        """
        result = defaultdict(list)
        for insert_unit in extracted_data:
            collection = list(insert_unit.keys())[0]

            for doc in insert_unit[collection]:
                result[collection].append(doc)

        return [dict(result)]

    def _list_intercept_hook(self, collection: str,
                             doc_with_child: Union[dict, list]) -> dict:
        """
        対象ドキュメントの子要素のみを削除し、
        出力の対象コレクション内のリストに対して要素の追加もしくは書き換えを行う

        :param str collection:
        :param dict doc_with_child:
        :return: dict output
        """

        def child_delete(doc_with_child: dict) -> None:
            """
            子要素を削除する

            :param dict doc_with_child:
            :return: None
            """
            tmp = copy.deepcopy(doc_with_child)
            tmp_list = []

            # 子要素のデータを抽出
            for k, v in doc_with_child.items():
                if self.parent != k and self.child != k and (
                        isinstance(v, dict) or (
                        isinstance(v, list) and (
                        not Utils.item_literal_check(v)))):
                    tmp_list.append(k)

            # 該当データがtmpにあれば削除
            for j in tmp_list:
                if j in tmp:
                    del tmp[j]

            # outputのデータを入れ替える
            if collection in output:
                output[collection].append(tmp)
            else:
                output[collection] = [tmp]

        output = {}
        if isinstance(doc_with_child, list):
            for i in doc_with_child:
                child_delete(i)
        else:
            child_delete(doc_with_child)

        return output

    def _ref(self, raw_data: dict) -> list:
        """
        リファレンスモードでedman用に変換を行う

        :param dict raw_data:
        :return: list
        """
        parent_collections = []
        oid_list = []
        list_output = []

        def _parent_ref_add(parent_collections: list, parent: int,
                            tmp: dict) -> dict:
            """
            # 親のリファレンス(コレクション)を追加
            # rootは追加されない(rootは1にあたる)

            :param list parent_collections:
            :param dict tmp:
            :param int parent:
            :return: dict tmp
            """
            if len(parent_collections) > 1:
                if self.parent in tmp:
                    tmp[self.parent] = DBRef(parent_collections[parent],
                                             tmp[self.parent]['_id'])
            return tmp

        def recursive(reading_dict_data: dict) -> dict:
            """
            edman用に変換を行う
            再帰
            要リファクタリング

            :param dict reading_dict_data:
            :return: dict output
            """
            output = {}
            parent = -2  # 説明変数
            my = -1  # 説明変数

            for key, value in reading_dict_data.items():

                if isinstance(value, dict):

                    if not self._collection_name_check(key):
                        sys.exit(f'この名前はコレクション名にできません {key}')

                    oid_list.append(ObjectId())

                    # コレクションを親リストに登録
                    parent_collections.append(key)

                    converted_value = self._convert_datetime(value)

                    # tmpから子データが返ってくる
                    tmp = recursive(converted_value)

                    # 親のリファレンス(コレクション)を追加
                    # rootの場合は追加されない
                    tmp = _parent_ref_add(parent_collections, parent, tmp)

                    # 子データのリファレンスを取得して親のデータに入れる
                    child_ref = self._get_child_reference(tmp)
                    if list(child_ref.values())[0]:  # 子データがない場合もある
                        tmp.update(child_ref)

                    # rootにoidを追加する
                    if self.parent not in tmp:
                        tmp.update({'_id': oid_list[0]})

                    del oid_list[my]

                    # バルクインサート用のリストを作成
                    list_output.append(self._list_intercept_hook(key, tmp))

                    output.update({key: tmp})
                    del parent_collections[my]

                elif isinstance(value, list):

                    # 日付データが含まれていたらdatetimeオブジェクトに変換
                    value = self._date_replace(value)

                    # 通常のリストデータの場合
                    if Utils.item_literal_check(value):
                        if not self._field_name_check(key):
                            sys.exit(f'フィールド名に不備があります {key}')

                            # oidを取得して追加
                        if '_id' not in output:
                            output.update({'_id': oid_list[my]})

                            # 親のリファレンス(oid)を追加
                            # この時点ではまだDBRefオブジェクトにはしていない
                        if len(oid_list) > 1:
                            if self.parent not in output:
                                output.update(
                                    {
                                        self.parent: {'_id': oid_list[parent]}
                                    }
                                )
                        output.update({key: value})

                    # 子要素としてのリストデータの場合
                    else:
                        parent_collections.append(key)
                        tmp_list = []

                        if not self._collection_name_check(key):
                            sys.exit(f'この名前はコレクション名にできません {key}')

                        for i in value:
                            oid_list.append(ObjectId())
                            converted_value = self._convert_datetime(i)

                            # tmpから子データが返ってくる
                            tmp = recursive(converted_value)

                            # 親のリファレンス(コレクション)を追加
                            # rootの場合は追加されない
                            tmp = _parent_ref_add(parent_collections, parent,
                                                  tmp)

                            # 子データのリファレンスを取得して親のデータに入れる
                            child_ref = self._get_child_reference(tmp)
                            if list(child_ref.values())[0]:  # 子データがない場合もある
                                tmp.update(child_ref)

                            del oid_list[my]
                            tmp_list.append(tmp)

                        # バルクインサート用のリストを作成
                        list_output.append(
                            self._list_intercept_hook(key, tmp_list))

                        output.update({key: tmp_list})
                        del parent_collections[my]

                else:
                    if not self._field_name_check(key):
                        sys.exit(f'フィールド名に不備があります {key}')

                    tmp = {key: value}

                    # oidを取得して追加
                    if '_id' not in output:
                        output.update({'_id': oid_list[my]})

                    # 親のリファレンス(oid)を追加
                    # この時点ではまだDBRefオブジェクトにはしていない
                    if len(oid_list) > 1:
                        if self.parent not in output:
                            output.update(
                                {
                                    self.parent: {'_id': oid_list[parent]}
                                }
                            )
                    output.update(tmp)

            return output

        """
        list_outputを書き換えているため、extract()の返り値(output)は利用していない
        """
        _ = recursive(raw_data)
        return self._list_organize(list_output)

    @staticmethod
    def _attached_oid(data: dict) -> dict:
        """
        辞書の一番上の階層にoidを付与する
        辞書の中身がリストの場合はリスト内の辞書すべてにoidを付与

        :param dict data:
        :return: dict data
        """
        for k, v in data.items():
            try:
                if isinstance(v, dict):
                    v.update({'_id': ObjectId()})
                else:
                    for doc in v:
                        doc.update({'_id': ObjectId()})

            except ValueError as e:
                sys.exit(e)
        return data

    def _date_replace(self, list_data: list) -> list:
        """
        リスト内の要素に{'#date':日付時間}のデータが含まれていたら
        datetimeオブジェクトに変換する
        例
        [{'#date':2019-02-28 11:43:22}, ' test_date']
        ↓
        [datetime.datetime(2019, 2, 28, 11, 43, 22), 'test_date']

        :param list list_data:
        :return list:
        """
        return [Utils.to_datetime(i[self.date])
                if isinstance(i, dict) and self.date in i
                else i
                for i in list_data]

    def emb(self, raw_data: dict) -> dict:
        """
        エンベデッドモードでedman用の変換を行う
        主に日付の変換
        update処理でも使用している

        :param dict raw_data:
        :return: dict
        """

        def recursive(data: dict) -> dict:
            """
            再帰で辞書を走査して、日付データの変換などを行う
            要リファクタリング

            :param dict data:
            :return: dict
            """
            output = {}

            for key, value in data.items():

                if isinstance(value, dict):
                    if not self._collection_name_check(key):
                        sys.exit(f'この名前は使用できません {key}')

                    converted_value = self._convert_datetime(value)
                    output.update({key: recursive(converted_value)})

                elif isinstance(value, list):
                    # 日付データが含まれていたらdatetimeオブジェクトに変換
                    value = self._date_replace(value)

                    # 通常のリストデータの場合
                    if Utils.item_literal_check(value):
                        if not self._field_name_check(key):
                            sys.exit(f'フィールド名に不備があります {key}')
                        list_tmp_data = value
                    # 子要素としてのリストデータの場合
                    else:
                        if not self._collection_name_check(key):
                            sys.exit(f'この名前は使用できません {key}')
                        list_tmp_data = [recursive(self._convert_datetime(i))
                                         for i in value]
                    output.update({key: list_tmp_data})

                else:
                    if not self._field_name_check(key):
                        sys.exit(f'フィールド名に不備があります {key}')
                    output.update({key: value})
            return output

        result = recursive(raw_data)
        return result

    def dict_to_edman(self, raw_data: dict, mode='ref') -> list:
        """
        json辞書からedman用に変換する
        embはobjectIdを付与したり、辞書からリストに変換している

        :param dict raw_data: JSONを辞書にしたデータ
        :param str mode: ref(reference) or emb(embedded) データ構造の選択肢
        :return: list インサート用のリストデータ
        """
        if mode == 'ref':
            return self._ref(raw_data)
        elif mode == 'emb':
            return [self._attached_oid(self.emb(raw_data))]
        else:
            sys.exit("投入モードは'ref'または'emb'です")
