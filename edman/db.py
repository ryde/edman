import copy
import urllib.parse
from datetime import datetime
from logging import INFO, getLogger
from typing import Any, Generator

from bson import DBRef, ObjectId
from jmespath import exceptions as jms_exceptions
from jmespath import search as jms_search
from pymongo import MongoClient, errors

from edman import Config, Convert, File
from edman.exceptions import (EdmanDbConnectError, EdmanDbProcessError,
                              EdmanFormatError, EdmanInternalError)
from edman.utils import Utils


class DB:
    """
    | DB関連クラス
    | MongoDBへの接続や各種チェック、インサート、作成や破棄など
    """

    def __init__(self, con=None) -> None:

        if con is not None:
            try:
                self.db, self.client = self._connect(**con)

            except EdmanDbConnectError:
                raise
            except Exception:
                raise

        self.parent = Config.parent
        self.child = Config.child
        self.file_ref = Config.file
        self.date = Config.date

        # ログ設定(トップに伝搬し、利用側でログとして取得してもらう)
        self.logger = getLogger(__name__)
        self.logger.setLevel(INFO)
        self.logger.propagate = True

    @property
    def get_db(self):
        """
        プロパティ

        :return: DB接続インスタンス(self.db)
        """
        if self.db is not None:
            return self.db
        else:
            raise EdmanDbConnectError('Please connect to DB.')

    @staticmethod
    def _connect(**kwargs: dict):
        """
        | DBに接続
        | self.edman_dbというメンバ変数には、このメソッドでDBオブジェクトが入る
        |
        | auth enabled example
        | kwargs = {
        |   'user':'user_name',
        |   'host':'127.0.0.1',
        |   'port':'27017',
        |   'database':'db_name',
        |   'password':'password',
        |   'options':['authSource=db_name']
        | }
        | auth enabled and LDAP connection example
        | kwargs = {
        |   'user':'ldap_user_name',
        |   'host':'127.0.0.1',
        |   'port':'27017',
        |   'database':'db_name',
        |   'password':'password',
        |   'options':['authMechanism=PLAIN']
        | }

        :param dict kwargs: DB接続情報の辞書
        :return: DB接続インスタンス(self.edman_db)
        """
        host = kwargs['host']
        port = kwargs['port']
        database = str(kwargs['database'])
        user = urllib.parse.quote_plus(str(kwargs['user']))
        password = urllib.parse.quote_plus(str(kwargs['password']))

        statement = ""
        if kwargs.get('options'):
            for option in kwargs['options']:
                connector = '&' if len(statement) else '?'
                statement += connector + option

        mongo_uri = f'mongodb://{user}:{password}@{host}:{port}/{statement}'
        client: MongoClient = MongoClient(mongo_uri)

        try:  # サーバの接続確認
            client.admin.command('ping')
        except errors.OperationFailure:
            raise EdmanDbConnectError('Invalid account.')
        except errors.ConnectionFailure:
            raise EdmanDbConnectError('Server not available.')
        except Exception as e:
            raise EdmanDbConnectError(e)

        edman_db = client[database]

        try:  # 現状、承認を確認する方法がないため、これで代用
            edman_db.list_collection_names()
        except errors.OperationFailure:
            raise EdmanDbConnectError('Authentication failed.')
        except Exception as e:
            raise EdmanDbConnectError(e)

        return edman_db, client

    def create_user_and_role(self, db_name: str, user_name: str, pwd: str,
                             role='readWrite', role_name='edman') -> None:
        """
        指定のロールとユーザを作成
        DB内部ユーザのみ対象
        DB内部ユーザを作成するメソッドなので、各ユーザのDBにロールとユーザ情報を作成する
        admin権限のみ使える

        :param str db_name:
        :param str user_name:
        :param str pwd:
        :param str role: default 'readWrite'
        :param str role_name: default 'edman'
        :return:
        """
        # ロール作成
        try:
            self.create_role_for_dbuser(db_name, role_name, role)
        except EdmanDbProcessError:
            raise

        # ユーザ作成とロール適応
        try:
            self.client[db_name].command(
                "createUser",
                user_name,
                pwd=pwd,
                roles=[role_name]
            )
        except AttributeError:
            raise EdmanDbProcessError('接続処理されていません')
        except (errors.OperationFailure, errors.InvalidName):
            self.client[db_name].command('dropRole', role_name)
            raise EdmanDbProcessError('ユーザの作成処理でエラーが起きました')
        except Exception:
            raise

    def create_role_for_dbuser(self, db_name: str, role_name: str,
                               role='readWrite') -> None:
        """
        ロールを作成

        admin権限のみ使える
        ユーザDBにロールを作成

        :param str db_name:
        :param str role_name:
        :param str role: default 'readWrite'
        :return:
        """
        try:
            self.client[db_name].command(
                "createRole",
                role_name,
                privileges=[
                    {
                        "resource": {"db": db_name, "collection": ""},
                        "actions": ["changeOwnPassword"]
                    }
                ],
                roles=[
                    {
                        'role': role,
                        'db': db_name,
                    },
                ],
            )
        except AttributeError:
            raise EdmanDbProcessError('接続処理されていません')
        except (errors.OperationFailure, errors.InvalidName):
            raise EdmanDbProcessError('ロール作成処理でエラーが起きました')
        except Exception:
            raise

    def create_role(self, db_name: str, role_name: str,
                    role='readWrite') -> None:
        """
        ロールを作成

        admin権限のみ使える
        LDAPユーザ用　admin DBにロールを作成、role_nameはグループのdn

        :param str db_name:
        :param str role_name:
        :param str role: default 'readWrite'
        :return:
        """
        try:
            self.db.command(
                "createRole",
                role_name,
                privileges=[
                    {
                        "resource": {"db": db_name, "collection": ""},
                        "actions": ["changeOwnPassword"]
                    }
                ],
                roles=[
                    {
                        'role': role,
                        'db': db_name,
                    },
                ],
            )
        except AttributeError:
            raise EdmanDbProcessError('接続処理されていません')
        except (errors.OperationFailure, errors.InvalidName):
            raise EdmanDbProcessError('ロール作成処理でエラーが起きました')
        except Exception:
            raise

    def delete_user_and_role(self, user_name: str, db_name: str,
                             role_name='edman', admin_name='admin') -> None:
        """
        ユーザ削除
        adminのみ実行可能

        :param str user_name:
        :param str db_name:
        :param str role_name: default 'edman'
        :param str admin_name: default 'admin'
        :return:
        """
        if user_name == admin_name:
            raise EdmanDbProcessError(
                f"{admin_name}は管理者なので削除できません")
        try:
            cl = self.client
            cl[db_name].command("dropUser", user_name)
            self.delete_role(role_name, db_name)
        except AttributeError:
            raise EdmanDbProcessError('接続処理されていません')
        except (errors.OperationFailure, errors.InvalidName):
            raise EdmanDbProcessError(
                'ユーザ及びロールの削除処理でエラーが起きました')
        except Exception:
            raise

    def delete_db(self, delete_db_name: str, admin_db='admin') -> None:
        """
        DBの削除

        :param str delete_db_name:
        :param str admin_db:
        :return:
        """
        if delete_db_name == admin_db:
            raise EdmanDbProcessError('管理者DBは削除できません')
        try:
            cl = self.client
            if delete_db_name in cl.list_database_names():
                cl.drop_database(delete_db_name)
        except AttributeError:
            raise EdmanDbProcessError('接続処理されていません')
        except errors.OperationFailure:
            raise EdmanDbProcessError('DBの削除処理でエラーが起きました')
        except Exception:
            raise

    def delete_role(self, role_name: str, target_db: str) -> None:
        """
        指定されたDB内のロールの削除
        admin権限のみ

        :param str role_name:
        :param str target_db:
        :return:
        """
        try:
            cl = self.client
            cl[target_db].command("dropRole", role_name)
        except AttributeError:
            raise EdmanDbProcessError('接続処理されていません')
        except errors.InvalidName:
            raise EdmanDbProcessError('ロール名が不正です')
        except errors.OperationFailure:
            raise EdmanDbProcessError('ロール削除処理でエラーが起きました')
        except Exception:
            raise

    def insert(self, insert_data: list) -> list[dict[str, list[ObjectId]]]:
        """
        インサート実行

        :param list insert_data: バルクインサート対応のリストデータ
        :return: results
        :rtype: list
        """
        results: list[dict[str, list[ObjectId]]] = []
        for i in insert_data:
            for collection, bulk_list in i.items():
                if isinstance(bulk_list, dict):
                    bulk_list = [bulk_list]
                try:
                    result = self.db[collection].insert_many(bulk_list)
                except errors.BulkWriteError as e:
                    raise EdmanDbProcessError(
                        f'インサートに失敗しました:{e.details}\nインサート結果:{results}')
                results.append({collection: result.inserted_ids})

        return results

    def find_collection_from_objectid(self,
                                      oid: str | ObjectId) -> str | None:
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

    def doc(self, collection: str, oid: ObjectId | str,
            query: list | None, reference_delete=True) -> dict | None:
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
        :rtype: dict or None
        """
        doc = self.db[collection].find_one({'_id': Utils.conv_objectid(oid)})

        if doc is None:
            result = None
        else:
            # embの場合は指定階層のドキュメントを引き抜く
            # refの場合はdocの結果をそのまま入れる
            try:
                if query is not None:
                    doc_result = self._get_emb_doc(dict(doc), query)
                else:
                    doc_result = doc
            except EdmanInternalError:
                raise

            # クエリの指定によってはリストデータなども取得出てしまうため
            if not isinstance(doc_result, dict):
                raise EdmanInternalError(
                    f'指定されたクエリはドキュメントではありません {query}')

            result = Utils.item_delete(
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
        s = Utils.generate_jms_query(query)
        try:
            result = jms_search(s, doc)
        except jms_exceptions.ParseError:
            raise EdmanInternalError(f'クエリの変換が出来ませんでした: {s}')

        return result

    def item_delete(self, collection: str, oid: ObjectId | str,
                    delete_key: str, query: list | None) -> bool:
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
            raise EdmanDbProcessError('ドキュメントが存在しません')

        doc = dict(doc)
        if query is not None:  # emb
            try:
                doc = Utils.doc_traverse(doc, [delete_key], query,
                                         Utils.item_delete)
            except Exception:
                raise
        else:  # ref
            try:
                del doc[delete_key]
            except IndexError:
                raise EdmanInternalError(f'キーは存在しません: {delete_key}')

        # ドキュメント置き換え処理
        replace_result = self.db[collection].replace_one({'_id': oid}, doc)
        result = True if replace_result.modified_count == 1 else False

        return result

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
                        buff: str | datetime | Any = Utils.to_datetime(
                            amend[key][self.date])
                    elif isinstance(value, list):
                        buff = [Utils.to_datetime(i[self.date])
                                if isinstance(i, dict) and self.date in i
                                else i
                                for i in value]
                    else:
                        buff = value
                    result.update({key: buff})
            except AttributeError:
                raise EdmanInternalError(
                    f'日付変換に失敗しました.構造に問題があります. {amend}')
        return result

    def update(self, collection: str, oid: str | ObjectId,
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
            raise EdmanInternalError('該当するドキュメントは存在しません')

        db_result = dict(db_result)
        if structure == 'emb':
            convert = Convert()
            try:
                # 日付データを日付オブジェクトに変換するため、
                # 必ずコンバートしてからマージする
                converted_amend_data = convert.emb(amend_data)
                amended = self._merge(db_result, converted_amend_data)
            except ValueError:
                raise
        elif structure == 'ref':
            # 日付データを日付オブジェクトに変換
            converted_amend_data = self._convert_datetime_dict(amend_data)
            amended = {**db_result, **converted_amend_data}
        else:
            raise EdmanFormatError('structureはrefまたはembの指定が必要です')

        try:
            replace_result = self.db[collection].replace_one({'_id': oid},
                                                             amended)
        except errors.OperationFailure:
            raise EdmanDbProcessError('アップデートに失敗しました')

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

    def delete(self, oid: str | ObjectId, collection: str,
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
            raise EdmanInternalError('該当するドキュメントは存在しません')

        db_result = dict(db_result)
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
                    raise EdmanDbProcessError(
                        '指定のドキュメントは削除できませんでした' + str(oid))
            except ValueError:
                raise

        elif structure == 'ref':
            try:
                # 親ドキュメントがあれば子要素リストから削除する
                if db_result.get(self.parent):
                    self._delete_reference_from_parent(db_result[self.parent],
                                                       db_result['_id'])
                # 対象のドキュメント以下のドキュメントと関連ファイルを削除する
                self._delete_documents_and_files(db_result, collection)
                return True
            except ValueError:
                raise
        else:
            raise EdmanFormatError('structureはrefまたはembの指定が必要です')

    def _delete_documents_and_files(self, db_result: dict,
                                    collection: str) -> None:
        """
        指定のドキュメント以下の子ドキュメントと関連ファイルを削除する

        :param dict db_result:
        :param str collection:
        :return:
        """
        delete_doc_id_dict: dict[str, list[ObjectId]] = {}
        delete_file_ref_list = []
        for element in self._recursive_extract_elements_from_doc(db_result,
                                                                 collection):
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
                f'親となる{parent_doc["id"]}に{str(del_oid)}が登録されていません')
        else:
            children.remove(target)

            # 子要素が登録されていればself.childを更新
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
            raise ValueError(
                f'親となる{parent_doc["id"]}は変更できませんでした')

    def _extract_elements_from_doc(self, doc: dict, collection: str) -> dict:
        """
        コレクション別の、oidとファイルリファレンスリストを取り出す

        :param dict doc:
        :param str collection:
        :return:
        :rtype: dict
        """
        return {collection: {
            doc['_id']: {self.file_ref: doc.get(self.file_ref, {})}}}

    def _recursive_extract_elements_from_doc(self, doc: dict,
                                             collection: str) -> Generator:
        """
        再帰処理で
        コレクション別の、oidとファイルリファレンスの辞書を取り出すジェネレータ

        :param dict doc:
        :param str collection:
        :return:
        :rtype: Generator
        """
        yield self._extract_elements_from_doc(doc, collection)
        if doc.get(self.child):
            for child_ref in doc[self.child]:
                yield from self._recursive_extract_elements_from_doc(
                    self.db.dereference(child_ref), child_ref.collection)

    def _collect_emb_file_ref(self, doc: dict, request_key: str) -> Generator:
        """
        emb構造のデータからファイルリファレンスのリストだけを取り出すジェネレータ

        :param dict doc:
        :param str request_key:
        :return: value
        :rtype: Generator
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
        :return: result
        :rtype: str
        """
        doc = self.db[collection].find_one({'_id': Utils.conv_objectid(oid)})
        if doc is None:
            raise EdmanInternalError('該当するドキュメントは存在しません')
        if any(key in doc for key in (self.parent, self.child)):
            result = 'ref'
        else:
            result = 'emb'
        return result

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

        convert = Convert()
        # refデータをembに変換する
        if structure_mode == 'emb':
            # 自分データ取り出し
            ref_result = self.doc(collection, oid, query=None,
                                  reference_delete=False)
            if ref_result is None:
                raise EdmanInternalError("ドキュメントが存在しません")
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

                converted_edman = convert.dict_to_edman(
                    {new_collection: ref_result}, mode='emb')
                structured_result = self.insert(converted_edman)
            # 子が存在しないドキュメントの場合(新たなコレクションとして切り出す)
            else:
                # 自分のリファレンスデータとidを削除
                for del_key in (self.parent, '_id'):
                    if del_key in ref_result:
                        del ref_result[del_key]
                converted_edman = convert.dict_to_edman(
                    {new_collection: ref_result}, mode='emb')
                structured_result = self.insert(converted_edman)

        # embからrefに変換
        elif structure_mode == 'ref':
            emb_result = self.db[collection].find_one({'_id': oid})
            del emb_result['_id']
            converted_edman = convert.dict_to_edman(
                {new_collection: emb_result}, mode='ref')
            structured_result = self.insert(converted_edman)
            structured_result.reverse()

        else:
            raise EdmanFormatError('structureはrefまたはembの指定が必要です')

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
                if tmp := self._child_storaged(doc):
                    result.append(tmp)
                # 子データがある時は繰り返す
                if tmp:
                    recursive(tmp)

        result: list = []  # recによって書き換えられる
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

        def recursive(doc_list, d):
            """
            再帰で結果リスト組み立て
            """
            if d > 0:
                # ここでデータを取得する
                for doc in doc_list:
                    if tmp := self._child_storaged(doc):
                        data.append(tmp)
                        d -= 1
                    # 子データがある時は繰り返す
                    if tmp:
                        recursive(tmp, d)

        data: list = []  # recによって書き換えられる
        if depth > 0:  # depthが効くのは必ず1以上
            recursive([self_doc], depth)  # 再帰関数をシンプルにするため、初期データをリストで囲む
            result: dict = self._build_to_doc_child(data)  # 親子構造に組み立て
        else:
            result = {}
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
        children = []
        # 単純にリスト内に辞書データを入れたい場合
        doc = list(doc.values())[0]  # {コレクション:ドキュメント}なのでドキュメントだけ分離
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

        def f(b):
            try:
                parent_id = self._get_uni_parent(b)
            except ValueError:
                raise
            return parent_id

        return {f(bros): bros for bros in find_result}

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
            raise ValueError(
                f'兄弟間で親のObjectIDが異なっています {parent_list}')

    @staticmethod
    def delete_reference(emb_data: dict, reference: tuple) -> dict:
        """
        ドキュメント内の特定の項目(リファレンスも)を削除する

        :param dict or list emb_data:
        :param tuple reference:
        :return:
        :rtype: dict
        """

        def recursive(data):
            for key, value in data.items():
                if isinstance(value, dict):
                    for k in reference:
                        if k in value:
                            del value[k]
                    recursive(value)
                elif isinstance(value, list) and Utils.item_literal_check(
                        value):
                    continue
                elif isinstance(value, list):
                    for i in value:
                        for k in reference:
                            if k in i:
                                del i[k]
                        recursive(i)
                else:
                    continue

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
        :return:
        :rtype: dict
        """

        if self.db[collection].estimated_document_count() == 0:
            raise EdmanInternalError('該当するドキュメントは存在しません')

        id_list = [doc['_id'] for doc in self.db[collection].find() if
                   self.get_structure(collection, doc['_id']) == 'emb']

        result_list = []
        convert = Convert()
        for oid in id_list:
            emb_result = self.db[collection].find_one({'_id': oid})
            del emb_result['_id']
            pull_result = convert.pullout_key(emb_result, key)
            if not pull_result:
                raise EdmanInternalError(f'{key}は存在しません')
            if exclusion:
                result = convert.exclusion_key(pull_result, exclusion)
                if pull_result == result:
                    raise EdmanInternalError(
                        f'{list(exclusion)}は存在しません')
            else:
                result = pull_result

            structured_result = self.insert(convert.dict_to_edman(result))
            structured_result.reverse()
            result_list.append(structured_result)

        return {'result': result_list} if result_list else {}

    def get_collections(self, coll_filter=None, gf_filter=True) -> list:
        """
        コレクションを取得

        :param dict or None coll_filter:
        :param bool gf_filter: default True
        :return: result
        :rtype: list
        """
        collections = [collection for collection in
                       self.db.list_collection_names(filter=coll_filter)]
        if gf_filter:
            result = list(
                set(collections) - {Config.fs_files, Config.fs_chunks})
        else:
            result = collections
        result.sort()
        return result

    @staticmethod
    def pack_list(types: list, target: list) -> list:
        """
        typesが少ない時に、最後の値で足りない分を埋める
        例
        types = [str, int, int, str]
        target = ['1', '2', '3', '4', '5']
        出力は [str, int, int, str, str]

        types = [int]
        target = ['1', '2', '3', '4', '5']
        出力は [int, int, int, int, int]

        :param types:
        :param target:
        :return: types
        :rtype: list
        """
        if len(target) > len(types):
            types.extend([types[-1] for _ in range(len(target) - len(types))])
        return types

    def bson_type(self, bson_data: dict, search_filters=None) -> dict:
        """
        DB内のデータをJSONに従って型変更をする
        DBにあってJSONにないキーは無視
        スペルミスも含め型一覧にない型を指定した時はstrに変換
        |
        |   JSON例:
        |   {
        |       "コレクション名":{
        |           "キー": "変更する型",
        |           "キー2": "変更する型",
        |       },
        |       "コレクション名2":{
        |           "キー": ["変更する型","変更する型"],
        |       }
        |   }

        |   値がリストの時
        |       ・双方どちらかがリストでない時は無視
        |       ・JSON側が単一、DB側が複数の時は単一の型で全て変換する
        |           JSON:['str']
        |           DB:['1','2','3']
        |       ・JSON側よりDB側が少ない時はJSON側は切り捨て
        |           JSON:['str'、'int', 'int']
        |           DB:['1',2]
        |       ・DB側よりJSON側が少ない時は、リストの最後の型で繰り返す
        |           JSON:['str'、'int']
        |           DB:['1',2,3,4,5]

        |   型一覧:
        |   [int,float,bool,str,datetime]
        |
        |   search_filtersを指定すると該当するドキュメントのみ変換する
        | search_filters = {'collection_name':{'_id':ObjectId('OBJECTID')}}

        :param dict bson_data:
        :param search_filters: default None
        :return: result
        :rtype: dict
        """
        result: dict[str, Any] = {}

        for collection, items in bson_data.items():

            # フィルタ、プロジェクション作成
            items_keys = list(items.keys())
            projection = {k: 1 for k in items_keys}
            # reference = {'$or': [
            #     {
            #         self.child: {'$exists': True}
            #     },
            #     {
            #         self.parent: {'$exists': True}
            #     }]}
            # docs = self.db[collection].find(reference, projection=projection)

            if search_filters is not None and isinstance(search_filters, dict):
                search_filter = search_filters.get(collection, {})
            else:
                search_filter = {}
            docs = self.db[collection].find(filter=search_filter,
                                            projection=projection)

            for doc in docs:
                update_params = {}
                log_buff = []
                for item_key in items_keys:
                    if item_key in doc:
                        db_value = doc[item_key]
                        json_value = items[item_key]

                        # JSONの値がリストの場合
                        if isinstance(json_value, list):
                            if isinstance(db_value, list):
                                # JSONとDBのリストを同じ個数にパックする
                                # JSON側が少ない時はJSON側の最後の値で埋める
                                # DB側が少ない時はDBと同じ個数でJSON側を切り捨て
                                j = self.pack_list(json_value, db_value)
                                list_buff = []
                                for idx, list_value in enumerate(db_value):
                                    f = Utils.type_cast_conv(j[idx])

                                    # datetimeからdatetimeには変換できない.
                                    # その他もデータも念のために一度strに変換してからfに渡す
                                    # Noneは変換しない
                                    if list_value is not None:
                                        if not isinstance(list_value, str):
                                            list_value = str(list_value)
                                        u_param = f(list_value)
                                    else:
                                        u_param = None
                                    list_buff.append(u_param)
                                param = {item_key: list_buff}
                            else:
                                # DB側がリストじゃない時は無視
                                param = {}
                        else:
                            # JSONがリストじゃないのにDBがリストの時は無視
                            if isinstance(db_value, list):
                                param = {}
                            else:
                                # DBもJSONも単一の型の時
                                f = Utils.type_cast_conv(json_value)

                                # datetimeからdatetimeには変換できない.
                                # その他もデータも念のために一度strに変換してからfに渡す
                                # Noneは変換しない
                                if db_value is not None:
                                    if not isinstance(db_value, str):
                                        db_value = str(db_value)
                                    param = {item_key: f(db_value)}
                                else:
                                    param = {}

                        if param:
                            update_params.update(param)
                            log_buff.append(item_key)
                # update
                if update_params:
                    res = self.db[collection].update_one(
                        {'_id': doc['_id']},
                        {'$set': update_params})
                    update_result = res.modified_count
                else:
                    update_result = None

                # ログ作成
                out_buff = {
                    str(doc['_id']): {
                        'target param': log_buff,
                        'update_result': update_result
                    }
                }
                if result.get(collection):
                    result[collection].update(out_buff)
                else:
                    result.update({collection: out_buff})

        return result

    def get_ref_depth(self, doc: dict, reference_key: str) -> int:
        """
        要素への階層の数を取得する

        :param dict doc:
        :param str reference_key: DBRefが格納されているキー名 例:_ed_parent, _ed_child
        :return:
        :rtype: int
        """
        result = 0
        if reference_key in doc:
            # 子要素の場合はリストで入っている
            # 各枝の中で一番多い数を取得する=最大の世代数
            if isinstance(doc[reference_key], list):
                result = 1
                result_list = []
                for dbref_doc in doc[reference_key]:
                    # tmp = 1
                    tmp = self.get_ref_depth(self.db.dereference(dbref_doc),
                                             reference_key)
                    result_list.append(tmp)
                result += max(result_list)
            else:
                # 親要素はツリーを遡っていくだけ
                result = 1
                result += self.get_ref_depth(
                    self.db.dereference(doc[reference_key]), reference_key)
        return result

    def get_root_dbref(self, doc: dict) -> None | DBRef:
        """
        ref形式のドキュメントのルートのDBRef要素を取得する
        ※root要素内にはparentのdbref要素は存在しないので、上から2階層目のparentのdbrefを取得する
        :param dict doc:
        :return: parent_ref
        :rtype: None or DBRef
        """
        if (parent_ref := doc.get(Config.parent)) is not None:
            if (over_first_degree_ref := self.get_root_dbref(
                    self.db.dereference(doc[Config.parent]))) is not None:
                parent_ref = over_first_degree_ref
        return parent_ref

    def delete_collections(self):
        """
        DB内のコレクション及びドキュメントを全て削除する
        grid.fsのコレクションも含む

        :return:
        """
        try:
            for collection in self.get_collections(gf_filter=False):
                self.get_db[collection].drop()
        except errors.OperationFailure:
            raise EdmanDbProcessError('コレクションの削除に失敗しました')
        else:
            if after_collections := self.get_collections(gf_filter=False):
                raise EdmanDbProcessError(
                    f'削除できないコレクションがあります {after_collections}')
