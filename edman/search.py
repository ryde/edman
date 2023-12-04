from datetime import datetime
from logging import INFO, getLogger

from bson import DBRef, ObjectId
from bson import errors as bson_errors
from pymongo import errors

from edman import Config
from edman.exceptions import (EdmanDbProcessError, EdmanFormatError,
                              EdmanInternalError)
from edman.utils import Utils


# from collections import deque

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

        # ログ設定(トップに伝搬し、利用側でログとして取得してもらう)
        self.logger = getLogger(__name__)
        self.logger.setLevel(INFO)
        self.logger.propagate = True

        if self.db is not None:
            self.connected_db = db.get_db

    def find(self, collection: str, query: dict, parent_depth=0,
             child_depth=0, exclusion=None) -> dict:
        """
        検索用メソッド

        :param str collection: 対象コレクション
        :param dict query: 検索クエリ
        :param int parent_depth: 親の指定深度
        :param int child_depth: 子の指定深度
        :param None or list exclusion:除外するリファレンスキー 例 ['_ed_file']
        :return: result 親 + 自分 + 子の階層構造となった辞書データ
        :rtype: dict
        """

        coll_filter = {"name": {"$regex": r"^(?!system\.)"}}
        if collection not in self.connected_db.list_collection_names(
                filter=coll_filter):
            raise EdmanDbProcessError('コレクションが存在しません')

        query = self._objectid_replacement(query)
        self_result = self._get_self(query, collection)
        if self_result is None:
            raise EdmanDbProcessError('データを取得できませんでした')
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
        result = self.generate_json_dict(result, include=exclusion)

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
            except bson_errors.InvalidId:
                raise
        return query

    def _get_self(self, query: dict, collection: str) -> dict | None:
        """
        自分自身のドキュメント取得

        :param dict query:
        :param str collection:
        :return:
        :rtype: dict or None
        """
        result = None
        try:
            docs = list(self.connected_db[collection].find(query))
        except errors.OperationFailure:
            raise EdmanDbProcessError('ドキュメントが取得できませんでした')
        else:
            if len(docs):
                result = {collection: docs[0]}
        return result

    def _get_parent(self, self_doc: dict, depth: int) -> dict | None:
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
                data.append({parent_collection: parent})
                nonlocal depth
                depth -= 1
                if depth > 0:
                    recursive(parent)

        if depth > 0:
            data: list = []  # recによって書き換えられる
            recursive(list(self_doc.values())[0])
            result = self._build_to_doc_parent(data)
        else:
            result = None
        return result

    @staticmethod
    def _build_to_doc_parent(parent_data_list: list) -> dict | None:
        """
        | 親の検索結果（リスト）を入れ子辞書に組み立てる
        |
        | 入力値であるparent_data_listについて、
        | parentに近い方から順番に並んでいる(一番最後がrootまたはrootに近い方)

        :param list parent_data_list:
        :return: result
        :rtype: dict or None
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

    def process_data_derived_from_mongodb(self, result_dict: dict,
                                          exclusion=None) -> dict:
        """
        MongoDB依存の項目を処理する::
          _idやrefの削除
          型をJSONに合わせる

          廃止予定 代替 generate_json_dict()

        :param dict result_dict:
        :param List or None exclusion:
            default ['_id', self.parent, self.child, self.file]
        :return: result_dict
        :rtype: dict
        """
        result_dict = self.generate_json_dict(result_dict, include=exclusion)

        return result_dict

    def _format_datetime(self, item: datetime) -> dict[str, str]:
        """
        datetime型なら書式変更して辞書に入れる

        :param item:
        :type item: datetime
        :return: result
        :rtype: dict
        """
        return {self.date: item.strftime("%Y-%m-%d %H:%M:%S")}

    def doc2(self, collection: str, oid: ObjectId | str,
             exclude_keys=None) -> dict:
        """
        指定するドキュメントを取得する
        doc()の置き換え版 リファクタリング完了後にdoc()を削除する
        embは対象外

        :param str collection:
        :param ObjectId oid:
        :param None or list exclude_keys: e.g.
            ['_id', 'parent', 'child', 'file']
        :return: result
        :rtype: dict
        """

        if isinstance(exclude_keys, list):
            exclude_keys = tuple(exclude_keys)
        elif exclude_keys is not None:
            raise EdmanFormatError('exclude_keysはlistで指定してください')

        doc = self.connected_db[collection].find_one(
            {'_id': Utils.conv_objectid(oid)})
        if doc is None:
            result = {}
        else:
            result = Utils.item_delete(dict(doc),
                                       exclude_keys) if exclude_keys else doc

        return result

    def get_tree(self, collection: str, oid: ObjectId, include=None) -> dict:
        """
        oidで指定するドキュメントが所属するツリーを全て取得する

        :param str collection:
        :param ObjectId oid:
        :param None or list include: e.g. ['_id', 'parent', 'child', 'file']
        :return: result
        :rtype: dict
        """

        self_doc = self.doc2(collection, oid)
        root_ref = self.db.get_root_dbref(self_doc)

        # root_refがNoneの場合は親ドキュメント
        if root_ref is None:
            root_doc = self_doc
            root_ref = DBRef(collection, oid)
        else:
            root_doc = self.doc2(root_ref.collection, root_ref.id)

        children = self.db.get_child_all({root_ref.collection: root_doc})

        parents = []
        for d in list(children.values()):
            for i in d:
                parents.append(i[self.parent])

        if all([i for i in parents if i == root_ref]):
            result_docs = dict(**root_doc, **children)
            tree = {root_ref.collection: result_docs}
            result = self.generate_json_dict(tree, include=include)

        else:
            raise EdmanInternalError(
                'ルートのドキュメントと子要素が一致しません'
                + root_ref.collection + ':' + str(root_ref.id))

        return result

    def generate_json_dict(self, result_dict: dict, include=None) -> dict:
        """
        edman依存の項目を処理する::
          _idとrefの削除
          型をJSONに合わせる

        process_data_derived_from_mongodb()の置き換え版
        process_data_derived_from_mongodb()は廃止予定

        :param dict result_dict:
        :param List or None include:
            e.g. ['_id', self.parent, self.child, self.file]
        :return: result_dict
        :rtype: dict
        """
        default_refs = ['_id', self.parent, self.child, self.file]
        if include is None:
            refs = tuple(default_refs)
        else:
            if not isinstance(include, list):
                raise ValueError('listもしくはNoneが必要です')
            else:
                for i in include:
                    if i not in default_refs:
                        raise ValueError(
                            f"{default_refs}の中から選択する必要があります")
            # デフォルトの値からexclusionを差し引く
            refs = tuple(set(default_refs) - set(include))

        def recursive(data: dict):
            # idとrefの削除
            for key, val in data.items():
                if isinstance(data[key], dict):
                    recursive(Utils.item_delete(data[key], refs))
                # リストデータは中身を型変換する
                elif isinstance(data[key], list) and Utils.item_literal_check(
                        data[key]):
                    data[key] = [self._format_datetime(j)
                                 if isinstance(j, datetime) else j
                                 for j in data[key]]
                elif isinstance(data[key], list):
                    for item in data[key]:
                        recursive(Utils.item_delete(item, refs))
                else:
                    try:  # 型変換
                        if isinstance(data[key], datetime):
                            data[key] = self._format_datetime(data[key])
                    except Exception:
                        raise

        recursive(result_dict)
        return result_dict

    # def logger_test(self):
    #     self.logger.error('logger test メソッド内 エラー')
    #     raise EdmanInternalError('logger test メソッド内 例外')

    # def get_ref_depth_bfs(self, collection, oid):
    #     # 子要素の最大の深さを取得する
    #     max_depth = 0
    #     doc = self.doc2(collection, oid)
    #     if self.child not in doc:
    #         raise EdmanDbProcessError('子要素が存在しません')  # これではなく0を返すべき?
    #
    #     q = deque([])  # キュー
    #     q.append(doc[self.child])
    #
    #     while len(q) > 0:
    #
    #         children = q.popleft()  # キュー取りだし（先頭）
    #         # このへんに別の配列を用意?
    #         for ref in children: # whileにする?
    #             d = self.connected_db.dereference(ref)
    #             if self.child in d:
    #                 q.append(d[self.child]) #ここでキューに言えるのは変?
    #
    #         max_depth += 1  # これを増やすタイミングは配列が空になったとき?
    #
    #     return max_depth
