import sys
from datetime import datetime
from typing import Union
from bson import errors, ObjectId
from edman.utils import Utils
from edman.exceptions import EdmanDbProcessError
from edman import Config


class Search:
    """
    検索関連クラス
    """

    def __init__(self, db=None) -> None:
        config = Config()  # システム環境用の設定を読み込む
        self.parent = config.parent
        self.child = config.child
        self.date = config.date
        self.file = config.file
        self.db = db

        if db is not None:
            self.connected_db = db.get_db

    def find(self, collection: str, query: dict, parent_depth: int,
             child_depth: int) -> dict:
        """
        検索用メソッド

        :param str collection: 対象コレクション
        :param dict query: 検索クエリ
        :param int parent_depth: 親の指定深度
        :param int child_depth: 子の指定深度
        :return: result 親 + 自分 + 子の階層構造となった辞書データ
        :rtype: dict
        """

        coll_filter = {"name": {"$regex": r"^(?!system\.)"}}
        if collection not in self.connected_db.list_collection_names(
                filter=coll_filter):
            raise EdmanDbProcessError('コレクションが存在しません')

        query = self._objectid_replacement(query)
        self_result = self._get_self(query, collection)
        reference_point_result = self.db.get_reference_point(
            self_result[collection])

        parent_result = None
        if reference_point_result[self.parent]:
            parent_result = self._get_parent(self_result, parent_depth)

        children_result = None
        if reference_point_result[self.child]:
            children_result = self.db.get_child(self_result, child_depth)

        # 親も子も存在しない時はselfのみ
        result = self_result

        # 子データが存在する時だけselfとマージ
        if children_result:
            self_key = list(self_result.keys())[0]
            self_result[self_key].update(children_result)

        # 親データが存在する時だけselfとマージ
        if parent_result:
            result = self._merge_parent(parent_result, result)

        # JSONデータ用に変換
        result = self.process_data_derived_from_mongodb(result)

        return result

    def _merge_parent(self, parent_result: dict, self_result: dict) -> dict:
        """
        親データに家族データをマージする

        :param dict parent_result:マージ前の親データ
        :param dict self_result:マージ済み家族データ
        :return: parent_result 親と自分と子をマージしたデータ
        :rtype: dict
        """

        def recursive(data: dict):
            """
            親データ内で一番深いドキュメントに家族データを挿入する

            :param dict data:
            :return:
            """
            for key in data.keys():
                # 親が複数存在することはありえないので辞書のみで可
                if isinstance(data[key], dict):
                    if '_id' in data[key]:
                        if parent_id == data[key]['_id']:
                            data[key].update(self_result)
                    recursive(data[key])

        self_doc = list(self_result.values())[0]
        parent_id = self_doc[self.parent].id
        recursive(parent_result)  # parent_resultにself_resultを入れる
        return parent_result

    @staticmethod
    def _objectid_replacement(query: dict) -> dict:
        """
        ObjectIdのチェックと変換

        :param dict query:
        :return: query
        :rtype: dict
        """
        if '_id' in query:
            try:
                query['_id'] = ObjectId(query['_id'])
            except errors.InvalidId:
                raise
        return query

    def _get_self(self, query: dict, collection: str) -> dict:
        """
        自分自身のドキュメント取得

        :param dict query:
        :param str collection:
        :return:
        :rtype: dict
        """
        docs = list(self.connected_db[collection].find(query))

        if not len(docs):
            # TODO resultはNoneのまま返却するように設計変更する予定
            sys.exit('ドキュメントが見つかりませんでした')
        else:
            # 複数ドキュメントの場合は選択処理へ
            doc = docs[0] if len(docs) == 1 else self._self_data_select(docs)

        return {collection: doc}

    @staticmethod
    def _self_data_select(docs: list) -> dict:
        """
        ドキュメント選択

        TODO このメソッドはcli専用モジュールへ移動予定
        :param list docs:
        :return:
        :rtype: dict
        """
        print('この条件は複数のドキュメントが存在します')
        for idx, doc in enumerate(docs):
            print(' ', idx, ' : ', doc)

        # ユーザからのドキュメント選択
        while True:
            doc_idx = int(input(f'選択してください [0-{len(docs) - 1}] >>'))
            if not (0 <= doc_idx < len(docs)):
                print('Out of range.')
            else:
                break
        return docs[doc_idx]

    def _get_parent(self, self_doc: dict, depth: int) -> Union[dict, None]:
        """
        | 親となるドキュメントを取得
        | depthで深度を設定し、階層分取得する

        :param dict self_doc:
        :param int depth:
        :return: result
        :rtype: dict or None
        """

        def recursive(doc):
            """
            再帰でドキュメントを取得
            DBReferenceを利用し、設定されている深度を減らしながら再帰
            """
            if self.parent in doc:
                parent = self.connected_db.dereference(doc[self.parent])
                parent_collection = doc[self.parent].collection
                result.append({parent_collection: parent})
                nonlocal depth
                depth -= 1
                if depth > 0:
                    recursive(parent)

        if depth > 0:
            result = []  # recによって書き換えられる
            recursive(list(self_doc.values())[0])
            result = self._build_to_doc_parent(result)
        else:
            result = None
        return result

    @staticmethod
    def _build_to_doc_parent(parent_data_list: list) -> dict:
        """
        | 親の検索結果（リスト）を入れ子辞書に組み立てる
        |
        | 入力値であるparent_data_listについて、
        | parentに近い方から順番に並んでいる(一番最後がrootまたはrootに近い方)

        :param list parent_data_list:
        :return: result
        :rtype: dict
        """
        result = None
        for read_data in parent_data_list:
            if result is not None:
                buff = read_data
                collection = list(buff.keys())[0]
                doc = list(buff.values())[0]
                doc.update(result)
                result = {collection: doc}
            else:
                result = read_data
        return result

    def process_data_derived_from_mongodb(self, result_dict: dict) -> dict:
        """
        MongoDB依存の項目を処理する::
          _idとrefの削除
          型をJSONに合わせる

        :param dict result_dict:
        :return: result_dict
        :rtype: dict
        """
        refs = ('_id', self.parent, self.child, self.file)

        def recursive(data: dict):
            # idとrefの削除
            for key, val in data.items():
                if isinstance(data[key], dict):
                    recursive(Utils.reference_item_delete(data[key], refs))
                # リストデータは中身を型変換する
                elif isinstance(data[key], list) and Utils.item_literal_check(
                        data[key]):
                    data[key] = [self._format_datetime(i) for i in data[key]]
                elif isinstance(data[key], list):
                    for item in data[key]:
                        recursive(Utils.reference_item_delete(item, refs))
                else:
                    try:  # 型変換
                        data[key] = self._format_datetime(data[key])
                    except Exception as e:
                        raise

        recursive(result_dict)
        return result_dict

    def _format_datetime(self, item: Union[str, int, float, bool, datetime]
                         ) -> Union[dict, str, int, float, bool, datetime]:
        """
        datetime型なら書式変更して辞書に入れる
        その場合は辞書を返し、それ以外の時は入ってきた値をそのまま返す

        :param item:
        :type item: str or int or float or bool or datetime
        :return: result
        :rtype: dict or str or int or float or bool or datetime
        """
        # datetimeそのままだと%Y-%m-%dと%H:%M:%Sの間に"T"が入るため書式変更
        result = item
        if isinstance(item, datetime):
            result = {self.date: item.strftime("%Y-%m-%d %H:%M:%S")}
        return result
