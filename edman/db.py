import sys
import copy
from typing import Union
import jmespath
from pymongo import MongoClient, errors
from bson import ObjectId, DBRef
from tqdm import tqdm
from edman.utils import Utils
from edman import Config, Convert, File


class DB:
    """
    | DB関連クラス
    | MongoDBへの接続や各種チェック、インサート、作成や破棄など
    """

    def __init__(self, con=None) -> None:

        if con is not None:
            self.db = self._connect(**con)

        self.parent = Config.parent
        self.child = Config.child
        self.file_ref = Config.file
        self.date = Config.date

    @property
    def get_db(self):
        """
        プロパティ

        :return: DB接続インスタンス(self.db)
        """
        if self.db is not None:
            return self.db
        else:
            sys.exit('Please connect to DB.')

    @staticmethod
    def _connect(**kwargs: dict):
        """
        | DBに接続
        | self.edman_dbというメンバ変数には、このメソッドでDBオブジェクトが入る

        :param dict kwargs: DB接続情報の辞書
        :return: DB接続インスタンス(self.edman_db)
        """
        host = kwargs['host']
        port = kwargs['port']
        database = kwargs['database']
        user = kwargs['user']
        password = kwargs['password']
        statement = f'mongodb://{user}:{password}@{host}:{port}/{database}'
        client = MongoClient(statement)

        try:  # サーバの接続確認
            client.admin.command('ismaster')
        except errors.ConnectionFailure:
            sys.exit('Server not available.')

        edman_db = client[database]
        return edman_db

    def insert(self, insert_data: list) -> list:
        """
        インサート実行

        :param list insert_data: バルクインサート対応のリストデータ
        :return: results
        :rtype: list
        """
        results = []

        # tqdm用のドキュメントの合計数を取得
        total_bulk_lists = sum(
            (len(i[collection_name]) for i in insert_data for
             collection_name, bulk_list in i.items() if
             isinstance(bulk_list, list)))
        if not total_bulk_lists:  # embの場合
            total_bulk_lists = 1

        for i in insert_data:
            collection_bar = tqdm(i.keys(), desc='collections', position=0)
            doc_bar = tqdm(total=total_bulk_lists, desc='documents',
                           position=1)
            for collection, bulk_list in i.items():

                # データによっては
                # 単一のドキュメントの時にリストで囲まれていない場合がある
                if isinstance(bulk_list, dict):
                    bulk_list = [bulk_list]
                try:
                    result = self.db[collection].insert_many(bulk_list)
                    results.append({collection: result.inserted_ids})
                    # プログレスバー表示関係
                    doc_bar.update(len(bulk_list))
                    collection_bar.set_description(f'Processing {collection}')
                    collection_bar.update(1)

                except errors.BulkWriteError as bwe:
                    print('インサートに失敗しました:', bwe.details)
                    print('インサート結果:', results)
            doc_bar.close()
            collection_bar.close()
        return results

    def find_collection_from_objectid(self,
                                      oid: Union[str,
                                                 ObjectId]) -> Union[str,
                                                                     None]:
        """
        | DB内のコレクションから指定のObjectIDを探し、所属しているコレクションを返す
        | DBに負荷がかかるので使用は注意が必要

        :param oid:
        :type oid: ObjectId or str
        :return: collection
        :rtype: str or None
        """
        oid = Utils.conv_objectid(oid)
        result = None
        coll_filter = {"name": {"$regex": r"^(?!system\.)"}}
        for collection in self.db.list_collection_names(filter=coll_filter):
            find_oid = self.db[collection].find_one({'_id': oid})
            if find_oid is not None and '_id' in find_oid:
                result = collection
                break
        return result

    def doc(self, collection: str, oid: Union[ObjectId, str],
            query: Union[list, None], reference_delete=True) -> dict:
        """
        | refもしくはembのドキュメントを取得する
        | オプションでedman特有のデータ含んで取得することもできる

        :param str collection:
        :param oid:
        :type oid: ObjectId or str
        :param query:
        :type query: list or None
        :param bool reference_delete: default True
        :return: result
        :rtype: dict
        """

        oid = Utils.conv_objectid(oid)
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            sys.exit('ドキュメントが存在しません')

        # embの場合は指定階層のドキュメントを引き抜く
        # refの場合はdocの結果をそのまま入れる
        doc_result = self._get_emb_doc(doc,
                                       query) if query is not None else doc

        # クエリの指定によってはリストデータなども取得出てしまうため
        if not isinstance(doc_result, dict):
            sys.exit(f'指定されたクエリはドキュメントではありません {query}')

        result = Utils.reference_item_delete(
            doc_result, ('_id', self.parent, self.child, self.file_ref)
        ) if reference_delete else doc_result

        return result

    @staticmethod
    def _get_emb_doc(doc: dict, query: list) -> dict:
        """
        emb形式のドキュメントからクエリーに従ってデータを取得する

        :param dict doc:
        :param list query:
        :return: result
        :rtype: dict
        """
        s = ''

        for idx, i in enumerate(query):
            if i.isdecimal():
                s += '[' + i + ']'
            else:
                if idx != 0:
                    s += '.'
                s += i
        try:
            result = jmespath.search(s, doc)
        except jmespath.parser.exceptions.ParseError:
            sys.exit(f'クエリの変換がうまくいきませんでした: {s}')

        return result

    def item_delete(self, collection: str, oid: Union[ObjectId, str],
                    delete_key: str, query: Union[list, None]) -> bool:
        """
        ドキュメントの項目を削除する

        :param str collection:
        :param oid:
        :type oid: str or ObjectId
        :param str delete_key:
        :param query:
        :type query: list or None
        :return:
        :rtype: bool
        """

        oid = Utils.conv_objectid(oid)
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            sys.exit('ドキュメントが存在しません')

        if query is not None:  # emb
            try:
                doc = Utils.doc_traverse(doc, [delete_key], query,
                                         self._delete_execute)
            except Exception as e:
                sys.exit(e)
        else:  # ref
            try:
                del doc[delete_key]
            except IndexError:
                sys.exit(f'キーは存在しません: {delete_key}')

        # ドキュメント置き換え処理
        replace_result = self.db[collection].replace_one({'_id': oid}, doc)
        result = True if replace_result.modified_count == 1 else False

        return result

    @staticmethod
    def _delete_execute(doc: dict, keys: list):
        """
        ドキュメントの削除処理
        _doc_traverse()のコールバック関数

        :param dict doc:
        :param list keys:
        :return:
        """
        for key in keys:
            if key in doc:
                del doc[key]

    def _convert_datetime_dict(self, amend: dict) -> dict:
        """
        辞書内辞書になっている文字列日付時間データを、辞書内日付時間に変換

        (例)
        {'start_date': {'#date': '1981-04-23'}}
        から
        {'start_date': 1981-04-23T00:00:00}
        amendにリストデータがある場合は中身も変換対象とする

        :param dict amend:
        :return: result
        :rtype: dict
        """
        result = copy.deepcopy(amend)
        if isinstance(amend, dict):
            try:
                for key, value in amend.items():
                    if isinstance(value, dict) and self.date in value:
                        result.update(
                            {
                                key: Utils.to_datetime(
                                    amend[key][self.date])
                            })
                    elif isinstance(value, list):

                        buff = [Utils.to_datetime(i[self.date])
                                if isinstance(i, dict) and self.date in i
                                else i
                                for i in value]
                        result.update({key: buff})
                    else:
                        result.update({key: value})
            except AttributeError:
                sys.exit(f'日付変換に失敗しました.構造に問題があります. {amend}')
        return result

    def update(self, collection: str, oid: Union[str, ObjectId],
               amend_data: dict, structure: str) -> bool:
        """
        修正データを用いてDBデータをアップデート

        :param str collection:
        :param oid:
        :type oid: str or ObjectId
        :param dict amend_data:
        :param str structure:
        :return:
        :rtype: bool
        """

        oid = Utils.conv_objectid(oid)
        db_result = self.db[collection].find_one({'_id': oid})
        if db_result is None:
            sys.exit('該当するドキュメントは存在しません')

        if structure == 'emb':
            try:
                # 日付データを日付オブジェクトに変換するため、
                # 必ずコンバートしてからマージする
                convert = Convert()
                converted_amend_data = convert.emb(amend_data)
                amended = self._merge(db_result, converted_amend_data)
            except ValueError as e:
                sys.exit(e)
        elif structure == 'ref':
            # 日付データを日付オブジェクトに変換
            converted_amend_data = self._convert_datetime_dict(amend_data)
            amended = {**db_result, **converted_amend_data}
        else:
            sys.exit('structureはrefまたはembの指定が必要です')

        try:
            replace_result = self.db[collection].replace_one({'_id': oid},
                                                             amended)
        except errors.OperationFailure:
            sys.exit('アップデートに失敗しました')

        return True if replace_result.modified_count == 1 else False

    def _merge_list(self, orig: list, amend: list) -> list:
        """
        リスト(オリジナル)と修正データをマージする

        :param list orig:
        :param list amend:
        :return: result
        :rtype: list
        """
        result = copy.copy(orig)
        for i, value in enumerate(amend):
            if isinstance(value, dict):
                result[i] = self._merge(orig[i], amend[i])
            elif isinstance(value, list):
                result[i] = self._merge_list(orig[i], amend[i])
            else:
                result.append(value)
        return result

    def _merge(self, orig: dict, amend: dict) -> dict:
        """
        辞書(オリジナル)と修正データをマージする

        :param dict orig:
        :param dict amend:
        :return: result
        :rtype: dict
        """
        result = copy.copy(orig)
        for item in amend:
            if isinstance(amend[item], dict):
                result[item] = self._merge(orig[item], amend[item])
            elif isinstance(amend[item], list):
                if Utils.item_literal_check(amend[item]):
                    result[item] = amend[item]
                else:
                    result[item] = self._merge_list(orig[item], amend[item])
            else:
                result[item] = amend[item]
        return result

    def delete(self, oid: Union[str, ObjectId], collection: str,
               structure: str) -> bool:
        """
        | ドキュメントを削除する
        | 指定のoidを含む下位のドキュメントを全削除
        | refで親が存在する時は親のchildリストから指定のoidを取り除く

        :param oid:
        :type oid: str or ObjectId
        :param str collection:
        :param str structure:
        :return:
        :rtype: bool
        """
        oid = Utils.conv_objectid(oid)
        db_result = self.db[collection].find_one({'_id': oid})
        if db_result is None:
            sys.exit('該当するドキュメントは存在しません')

        if structure == 'emb':
            try:
                result = self.db[collection].delete_one({'_id': oid})
                if result.deleted_count:
                    # 添付データがあればgridfsから削除
                    file = File(self.get_db)
                    file.fs_delete(
                        sum([i for i in self._collect_emb_file_ref(
                            db_result, self.file_ref)], []))
                    return True
                else:
                    sys.exit('指定のドキュメントは削除できませんでした' + str(oid))
            except ValueError as e:
                sys.exit(e)

        elif structure == 'ref':

            try:
                # 親ドキュメントがあれば子要素リストから削除する
                if db_result.get(self.parent):
                    self._delete_reference_from_parent(db_result[self.parent],
                                                       db_result['_id'])

                # 対象のドキュメント以下のドキュメントと関連ファイルを削除する
                self._delete_documents_and_files(db_result, collection)
                return True
            except ValueError as e:
                sys.exit(e)
        else:
            sys.exit('structureはrefまたはembの指定が必要です')

    def _delete_documents_and_files(self, db_result: dict,
                                    collection: str) -> None:
        """
        指定のドキュメント以下の子ドキュメントと関連ファイルを削除する

        :param dict db_result:
        :param str collection:
        :return:
        """
        delete_doc_id_dict = {}
        delete_file_ref_list = []
        for element in [i for i in
                        self._recursive_extract_elements_from_doc(db_result,
                                                                  collection)]:
            doc_collection = list(element.keys())[0]
            id_and_refs = list(element.values())[0]

            for oid, refs in id_and_refs.items():
                if delete_doc_id_dict.get(doc_collection):
                    delete_doc_id_dict[doc_collection].append(oid)
                else:
                    delete_doc_id_dict.update({doc_collection: [oid]})

                if refs.get(self.file_ref):
                    delete_file_ref_list.extend(refs[self.file_ref])

        self._delete_documents(delete_doc_id_dict)
        # gridfsからファイルを消す
        file = File(self.get_db)
        file.fs_delete(delete_file_ref_list)

    def _delete_documents(self, delete_doc_id_dict: dict) -> None:
        """
        繰り返し指定のドキュメントを削除する

        :param dict delete_doc_id_dict:
        :return:
        """
        del_doc_count = 0
        deleted_doc_count = 0
        for collection, del_list in delete_doc_id_dict.items():
            del_doc_count += len(del_list)
            for oid in del_list:
                del_doc_result = self.db[collection].delete_one({'_id': oid})
                deleted_doc_count += del_doc_result.deleted_count
        if del_doc_count != deleted_doc_count:
            raise ValueError('削除対象と削除済みドキュメント数が一致しません')

    def _delete_reference_from_parent(self, ref: DBRef,
                                      del_oid: ObjectId) -> None:
        """
        親ドキュメントのリファレンスリストから指定のoidのリファレンスを取り除く

        :param DBRef ref:
        :param ObjectId del_oid:
        :return:
        """
        parent_doc = self.db[ref.collection].find_one({'_id': ref.id})
        children = parent_doc[self.child]

        target = None
        for child in children:
            if child.id == del_oid:
                target = child
                break
        if target is None:
            raise ValueError(
                '親となる' + parent_doc['id'] + 'に' + str(del_oid) + 'が登録されていません')
        else:
            children.remove(target)

            # 他に子要素が登録されていればself.childを更新
            if len(children) > 0:
                result = self.db[ref.collection].update_one(
                    {'_id': ref.id},
                    {'$set': {self.child: children}})
            # 他に子要素がなければself.child自体を削除
            else:
                del parent_doc[self.child]
                result = self.db[ref.collection].replace_one(
                    {'_id': ref.id}, parent_doc)

        if not result.modified_count:
            raise ValueError('親となる' + parent_doc['id'] + 'は変更できませんでした')

    def _extract_elements_from_doc(self, doc: dict, collection: str) -> dict:
        """
        コレクション別の、oidとファイルリファレンスリストを取り出す

        :param dict doc:
        :param str collection:
        :return:
        :rtype: dict
        """
        file_ref_buff = {}
        if doc.get(self.file_ref) is not None:
            file_ref_buff = {self.file_ref: doc[self.file_ref]}

        return {collection: {doc['_id']: file_ref_buff}}

    def _recursive_extract_elements_from_doc(self, doc: dict,
                                             collection: str) -> dict:
        """
        再帰処理で
        コレクション別の、oidとファイルリファレンスの辞書を取り出すジェネレータ

        :param dict doc:
        :param str collection:
        :return:
        :rtype: dict
        """
        yield self._extract_elements_from_doc(doc, collection)

        if doc.get(self.child):
            for child_ref in doc[self.child]:
                yield from self._recursive_extract_elements_from_doc(
                    self.db.dereference(child_ref), child_ref.collection)

    def _collect_emb_file_ref(self, doc: dict, request_key: str) -> list:
        """
        emb構造のデータからファイルリファレンスのリストだけを取り出すジェネレータ

        :param dict doc:
        :param str request_key:
        :return: value
        :rtype: list
        """

        for key, value in doc.items():
            if isinstance(value, dict):
                yield from self._collect_emb_file_ref(value, request_key)

            elif isinstance(value, list) and Utils.item_literal_check(value):
                if key == request_key:
                    yield value
                continue

            elif isinstance(value, list):
                if key == request_key:
                    yield value
                else:
                    for i in value:
                        yield from self._collect_emb_file_ref(i, request_key)
            else:
                continue

    def get_reference_point(self, self_result: dict) -> dict:
        """
        | ドキュメントに親や子のリファレンス項目名が含まれているか調べる
        |
        | 片方しかない場合は末端(親、または一番下の子)となる
        | 両方含まれていればこのドキュメントには親と子が存在する
        | 両方含まれていなければ、単独のドキュメント

        :param dict self_result:
        :return:
        :rtype: dict
        """
        return {key: True if self_result.get(key) else False for key in
                (self.parent, self.child)}

    def get_structure(self, collection: str, oid: ObjectId) -> str:
        """
        対象のドキュメントの構造を取得する

        :param str collection:
        :param ObjectId oid:
        :return: ref or emb
        :rtype: str
        """
        doc = self.db[collection].find_one({'_id': Utils.conv_objectid(oid)})
        if doc is None:
            sys.exit('指定のドキュメントがありません')

        if any(key in doc for key in (self.parent, self.child)):
            return 'ref'
        else:
            return 'emb'

    def structure(self, collection: str, oid: ObjectId,
                  structure_mode: str, new_collection: str) -> list:
        """
        構造をrefからembへ、またはembからrefへ変更する

        :param str collection:
        :param ObjectId oid:
        :param str structure_mode:
        :param str new_collection:
        :return: structured_result
        :rtype: list
        """

        oid = Utils.conv_objectid(oid)

        # refデータをembに変換する
        if structure_mode == 'emb':
            # 自分データ取り出し
            ref_result = self.doc(collection, oid, query=None,
                                  reference_delete=False)
            reference_point_result = self.get_reference_point(
                ref_result)
            if reference_point_result[self.child]:
                # 子データを取り出し
                children = self.get_child_all({collection: ref_result})

                # 自分のリファレンスデータとidを削除
                for del_key in (self.parent, self.child, '_id'):
                    if del_key in ref_result:
                        del ref_result[del_key]

                # 子のリファレンスデータ削除
                non_ref_children = self.delete_reference(children,
                                                         ('_id', self.parent,
                                                          self.child))
                # 自分と子要素をマージする
                ref_result.update(non_ref_children)

                convert = Convert()
                converted_edman = convert.dict_to_edman(
                    {new_collection: ref_result}, mode='emb')
                structured_result = self.insert(converted_edman)
            # 子が存在しないドキュメントの場合(新たなコレクションとして切り出す)
            else:
                # 自分のリファレンスデータとidを削除
                for del_key in (self.parent, '_id'):
                    if del_key in ref_result:
                        del ref_result[del_key]
                convert = Convert()
                converted_edman = convert.dict_to_edman(
                    {new_collection: ref_result}, mode='emb')
                structured_result = self.insert(converted_edman)

        # embからrefに変換
        elif structure_mode == 'ref':
            emb_result = self.db[collection].find_one({'_id': oid})
            del emb_result['_id']
            convert = Convert()
            converted_edman = convert.dict_to_edman(
                {new_collection: emb_result}, mode='ref')
            structured_result = self.insert(converted_edman)
            structured_result.reverse()

        else:
            sys.exit('構造はrefかembを指定してください')

        return structured_result

    def get_child_all(self, self_doc: dict) -> dict:
        """
        子のドキュメントを再帰で全部取得

        :param dict self_doc:
        :return:
        :rtype: dict
        """

        def recursive(doc_list):
            # ここでデータを取得する
            for doc in doc_list:
                tmp = self._child_storaged(doc)
                if tmp:
                    result.append(tmp)

                # 子データがある時は繰り返す
                if tmp:
                    recursive(tmp)

        result = []  # recによって書き換えられる

        recursive([self_doc])  # 再帰関数をシンプルにするため、初期データをリストで囲む
        return self._build_to_doc_child(result)  # 親子構造に組み立て

    def get_child(self, self_doc: dict, depth: int) -> dict:
        """
        | 子のドキュメントを取得
        |
        | depthで深度を設定し、階層分取得する

        :param dict self_doc:
        :param int depth:
        :return:
        :rtype: dict
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
            result = self._build_to_doc_child(result)  # 親子構造に組み立て
        return result

    def _child_storaged(self, doc: dict) -> list:
        """
        | リファレンスで子データを取得する
        |
        | 同じコレクションの場合は子データをリストで囲む

        :param dict doc:
        :return: children
        :rtype: list
        """
        doc = list(doc.values())[0]
        children = []
        # 単純にリスト内に辞書データを入れたい場合
        if self.child in doc:
            children = [
                {child_ref.collection: self.db.dereference(child_ref)}
                for child_ref in doc[self.child]]

        return children

    def _build_to_doc_child(self, find_result: list) -> dict:
        """
        子の検索結果（リスト）を入れ子辞書に組み立てる

        :param list find_result:
        :return:
        :rtype: dict
        """
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

    def _generate_parent_id_dict(self, find_result: list) -> dict:
        """
        親IDをキーとする辞書を生成する

        :param list find_result:
        :return:
        :rtype: dict
        """
        result = {}
        for bros in find_result:
            try:
                parent_id = self._get_uni_parent(bros)
            except ValueError as e:
                sys.exit(e)
            result.update({parent_id: bros})

        return result

    def _get_uni_parent(self, bros: dict) -> ObjectId:
        """
        | 兄弟データ内の親のIDを取得
        |
        | エラーがなければ通常は親は唯一なので一つのOidを返す

        :param dict bros:
        :return:
        :rtype: ObjectId
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

    @staticmethod
    def delete_reference(emb_data: dict, reference: tuple) -> dict:
        """
        ドキュメント内の特定の項目(リファレンスも)を削除する

        :param dict emb_data:
        :param tuple reference:
        :return:
        :rtype: dict
        """

        def recursive(data):
            for key, value in data.items():
                if isinstance(value, dict):
                    for del_key in reference:
                        if del_key in value:
                            del value[del_key]
                    recursive(value)
                elif isinstance(value, list) and Utils.item_literal_check(
                        value):
                    continue
                elif isinstance(value, list):
                    for i in value:
                        for del_key in reference:
                            if del_key in i:
                                del i[del_key]
                        recursive(i)
                else:
                    pass

        # 入ってくるデータのトップにコレクションが入っていないのでうまく扱えない？応急処置
        for del_key in reference:
            if del_key in emb_data:
                del emb_data[del_key]

        recursive(emb_data)
        return emb_data

    def loop_exclusion_key_and_ref(self, collection: str, key: str,
                                   exclusion: tuple) -> dict:
        """
        対象のコレクション内のドキュメントを全て、指定のキーの要素を抜き出してrefに変換してDBに入れる
        また、取り出したデータ内の指定の要素を除外することもできる

        :param str collection: 変換対象のコレクション
        :param str key: refに変換開始する対象のキー
        :param tuple exclusion: 除外するキーの設定
        :return: result
        :rtype: dict
        """
        docs = self.db[collection].find()
        if docs.count() == 0:
            sys.exit('対象のドキュメントは存在しません')
        id_list = []
        for doc in docs:
            if self.get_structure(collection, doc['_id']) == 'emb':
                id_list.append(doc['_id'])

        if len(id_list) == 0:
            sys.exit('変換対象のドキュメントは全てreferenceです')

        result_list = []
        for oid in id_list:
            emb_result = self.db[collection].find_one({'_id': oid})
            del emb_result['_id']
            convert = Convert()
            pull_result = convert.pullout_key(emb_result, key)
            if not pull_result:
                sys.exit(f'{key}は存在しません')
            if exclusion:
                result = convert.exclusion_key(pull_result, exclusion)
                if pull_result == result:
                    sys.exit(f'{list(exclusion)}は存在しません')
            else:
                result = pull_result

            converted_edman = convert.dict_to_edman(result)
            structured_result = self.insert(converted_edman)
            structured_result.reverse()
            result_list.append(structured_result)

        result = {}
        if len(result_list):
            result.update({'result': result_list})
            print('\r\n')  # 改行できない問題を回避

        return result
