import sys
import copy
from datetime import datetime
from collections import defaultdict
from typing import Union
from bson import errors, ObjectId
from edman.utils import Utils
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
        if db is not None:
            self.db = db.get_db

    def _search_necessity_judge(self, self_result: dict) -> dict:
        """
        データに親や子のリファレンス項目名が含まれているか判断

        :param dict self_result:
        :return: dict result
        """
        result = {}
        if self_result:
            for k, v in self_result.items():
                keys = list(v.keys())
                parent_exist = True if self.parent in keys else False
                result.update({self.parent: parent_exist})
                child_exist = True if self.child in keys else False
                result.update({self.child: child_exist})

        return result

    def find(self, collection: str, query: dict, parent_depth: int,
             child_depth: int) -> dict:
        """
        検索用メソッド

        :param str collection: 対象コレクション
        :param dict query: 検索クエリ
        :param int parent_depth: 親の指定深度
        :param int child_depth: 子の指定深度
        :return: dict result 親 + 自分 + 子の階層構造となった辞書データ
        """

        coll_filter = {"name": {"$regex": r"^(?!system\.)"}}
        if collection not in self.db.list_collection_names(filter=coll_filter):
            sys.exit('コレクションが存在しません')

        query = self._objectid_replacement(query)
        self_result = self._get_self(query, collection)
        search_necessity = self._search_necessity_judge(self_result)

        parent_result = None
        if search_necessity[self.parent]:
            parent_result = self._get_parent(self_result, parent_depth)

        children_result = None
        if search_necessity[self.child]:
            children_result = self._get_child(self_result, child_depth)

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
        result = self._process_data_derived_from_mongodb(result)

        return result

    def _merge_parent(self, parent_result: dict, self_result: dict) -> dict:
        """
        親データに家族データをマージする

        :param dict parent_result:マージ前の親データ
        :param dict self_result:マージ済み家族データ
        :return: dict parent_result 親と自分と子をマージしたデータ
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
        :return: dict query
        """
        if '_id' in query:
            try:
                query['_id'] = ObjectId(query['_id'])
            except errors.InvalidId:
                sys.exit('ObjectIdが正しくありません')
        return query

    def _get_self(self, query: dict, collection: str) -> dict:
        """
        自分自身のドキュメント取得

        :param dict query:
        :param str collection:
        :return: dict
        """
        docs = list(self.db[collection].find(query))

        if not len(docs):
            sys.exit('ドキュメントが見つかりませんでした')
        else:
            if len(docs) == 1:
                doc = docs[0]
            else:  # 複数ドキュメントの場合は選択処理へ
                doc = self._self_data_select(docs)
            return {collection: doc}

    @staticmethod
    def _self_data_select(docs: list) -> dict:
        """
        ドキュメント選択

        :param list docs:
        :return: dict
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
        :return: dict result
        """

        def recursive(doc):
            """
            再帰でドキュメントを取得
            DBReferenceを利用し、設定されている深度を減らしながら再帰
            """
            if self.parent in doc:
                parent = self.db.dereference(doc[self.parent])
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
        :return: dict result
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

    def _child_storaged(self, doc: dict) -> list:
        """
        | リファレンスで子データを取得する
        |
        | 同じコレクションの場合は子データをリストで囲む

        :param dict doc:
        :return: list children
        """
        doc = list(doc.values())[0]
        children = []
        # 単純にリスト内に辞書データを入れたい場合
        if self.child in doc:
            children = [
                {child_ref.collection: self.db.dereference(child_ref)}
                for child_ref in doc[self.child]]

        return children

    def _get_child(self, self_doc: dict, depth: int) -> dict:
        """
        | 子のドキュメントを取得
        |
        | depthで深度を設定し、階層分取得する

        :param dict self_doc:
        :param int depth:
        :return: dict
        """

        def recursive(doc_list: list, depth: int):
            """
            再帰で結果リスト組み立て
            """
            if depth > 0:
                tmp = []
                # ここでデータを取得する
                for doc in doc_list:
                    tmp = self._child_storaged(doc)
                    if tmp:
                        result.append(tmp)
                depth -= 1

                # 子データがある時は繰り返す
                if tmp:
                    recursive(tmp, depth)

        result = []  # recによって書き換えられる

        if depth >= 1:  # depthが効くのは必ず1以上
            recursive([self_doc], depth)  # 再帰関数をシンプルにするため、初期データをリストで囲む
            # TODO result.appendではなくextendにしてみるとlistのカッコがとれるかもしれない
            result = self._build_to_doc_child(result)  # 親子構造に組み立て
        return result

    def _get_uni_parent(self, bros: dict) -> ObjectId:
        """
        | 兄弟データ内の親のIDを取得
        |
        | エラーがなければ通常は親は唯一なので一つのOidを返す

        :param bros:
        :return: ObjectId
        """
        parent_list = []
        for collection, doc in bros.items():
            if isinstance(doc, dict):
                parent_list.append(doc[self.parent].id)
            else:
                parent_list.extend(
                    [doc_dict[self.parent].id for doc_dict in doc])

        if len(set(parent_list)) == 1:
            return list(parent_list)[0]
        else:
            raise ValueError(f'兄弟で親のObjectIDが異なっています\n{parent_list}')

    def _generate_parent_id_dict(self, find_result: list) -> dict:
        """
        親IDをキーとする辞書を生成する

        :param list find_result:
        :return: dict
        """
        result = {}
        for bros in find_result:
            try:
                parent_id = self._get_uni_parent(bros)
            except ValueError as e:
                sys.exit(e)
            result.update({parent_id: bros})

        return result

    def _build_to_doc_child(self, find_result: list) -> dict:
        """
        子の検索結果（リスト）を入れ子辞書に組み立てる

        :param list find_result:
        :return: dict
        """
        # find_result = [i for i in self._child_combine_list(find_result)]
        find_result = [i for i in Utils.child_combine(find_result)]
        parent_id_dict = self._generate_parent_id_dict(find_result)
        find_result_cp = copy.deepcopy(list(reversed(find_result)))

        for bros_idx, bros in enumerate(reversed(find_result)):
            for collection, docs in bros.items():
                for doc_idx, doc in enumerate(docs):
                    # 子データが存在する場合はマージする
                    if doc['_id'] in parent_id_dict:
                        tmp = find_result_cp[bros_idx][collection][doc_idx]
                        tmp.update(parent_id_dict[doc['_id']])
                        doc.update(tmp)
                        del parent_id_dict[doc['_id']]

        return find_result[0]

    def _process_data_derived_from_mongodb(self, result_dict: dict) -> dict:
        """
        MongoDB依存の項目を処理する::
          _idとrefの削除
          型をJSONに合わせる

        :param dict result_dict:
        :return: dict result_dict
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
                        sys.exit(e)

        recursive(result_dict)
        return result_dict

    def _format_datetime(self, item: Union[str, int, float, bool, datetime]
                         ) -> Union[dict, str, int, float, bool, datetime]:
        """
        datetime型なら書式変更して辞書に入れる
        その場合は辞書を返し、それ以外の時は入ってきた値をそのまま返す

        :param item: Union[str, int, float, bool, datetime]
        :return:result Union[dict, str, int, float, bool, datetime]
        """
        # datetimeそのままだと%Y-%m-%dと%H:%M:%Sの間に"T"が入るため書式変更
        result = item
        if isinstance(item, datetime):
            result = {self.date: item.strftime("%Y-%m-%d %H:%M:%S")}
        return result
