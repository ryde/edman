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
    DB関連クラス
    MongoDBへの接続や各種チェック、インサート、作成や破棄など
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
        DBに接続
        self.edman_dbというメンバ変数には、このメソッドでDBオブジェクトが入る

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
        :return: list results
        """
        results = []

        # tqdm用のドキュメントのリスト数を取得
        total_bulk_lists = sum(
            (len(i[bulk_list]) for i in insert_data for bulk_list in i))

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
        DB内のコレクションから指定のObjectIDを探し、所属しているコレクションを返す
        DBに負荷がかかるので使用は注意が必要

        :param ObjectId or str oid:
        :return str or None collection:
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
        refもしくはembのドキュメントを取得する
        オプションでedman特有のデータ含んで取得することもできる

        :param str collection:
        :param ObjectId or str oid:
        :param list or None query:
        :param bool reference_delete: default True
        :return dict result:
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
        :return dict result:
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
        :param str or ObjectId oid:
        :param str delete_key:
        :param list or None query:
        :return bool:
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
        :return: dict result
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
        :param str or ObjectId oid:
        :param dict amend_data:
        :param str structure:
        :return bool:
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
        :return list result:
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
        :return dict result:
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
        ドキュメントを削除する
        指定のoidを含む下位のドキュメントを全削除
        refで親が存在する時は親のchildリストから指定のoidを取り除く

        :param str or ObjectId oid:
        :param str collection:
        :param str structure:
        :return bool:
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
            result = self.db[ref.collection].update_one(
                {'_id': ref.id},
                {'$set': {self.child: children}})

        if not result.modified_count:
            raise ValueError('親となる' + parent_doc['id'] + 'は変更できませんでした')

    def _extract_elements_from_doc(self, doc: dict, collection: str) -> dict:
        """
        コレクション別の、oidとファイルリファレンスリストを取り出す

        :param dict doc:
        :param str collection:
        :return:
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
        :return list value:
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

    def get_structure(self, collection: str, oid: ObjectId) -> str:
        doc = self.db[collection].find_one({'_id': Utils.conv_objectid(oid)})
        if doc is None:
            sys.exit('指定のドキュメントがありません')

        if any(key in doc for key in (self.parent, self.child)):
            return 'ref'
        else:
            return 'emb'

    def structure(self, collection: str, oid: ObjectId,
                  structure_mode: str) -> bool:

        # refデータをembに変換する
        if structure_mode == 'emb':
            # 自分データ取り出し
            ref_result = self.doc(collection, oid, query=None,
                                  reference_delete=False)
            # print(ref_result)
            # TODO 子データを取り出し

            # TODO 自分と子データをマージ
            # TODO _ed_parentと_ed_childと_idを消す(_ed_fileだけ残す)
            # TODO この時点で新規入力のjson+ファイルリファレンスの状態になる
            # TODO 取り出しデータを dict_to_edmanに入れる(_ed_fileが削除されそうな件は修正、もしくは新規メソッドで対応)

            # embデータをrefに変換する
        elif structure_mode == 'ref':
            # emb_result = self.db[collection].find({'_id': oid})
            # TODO トップの_id削除
            # TODO _ed_fileを保ったままコンバート
            # TODO コンバートしたデータを_ed_fileを保ったままinsert
            pass

        else:
            sys.exit('構造はrefかembを指定してください')

        return True
