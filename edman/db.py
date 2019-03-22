import sys
import copy
import re
import os
from pathlib import Path
from typing import Union
import jmespath
import pymongo
from pymongo.errors import ConnectionFailure, BulkWriteError, OperationFailure
from bson import ObjectId
from tqdm import tqdm
from edman.convert import Convert
from edman.utils import Utils
from edman.config import Config


class DB:
    """
    DB関連クラス
    MongoDBへの接続や各種チェック、インサート、作成や破棄など
    """

    def __init__(self) -> None:
        self.edman_db = None

        self.parent = Config.parent
        self.child = Config.child
        self.file_ref = Config.file
        self.date = Config.date

    @property
    def db(self):
        """
        プロパティ

        :return: DB接続インスタンス(self.edman_db)
        """
        if self.edman_db is not None:
            return self.edman_db
        else:
            sys.exit('Please connect to DB.')

    def connect(self, **kwargs: dict):
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
        client = pymongo.MongoClient(statement)

        try:  # サーバの接続確認
            client.admin.command('ismaster')
        except ConnectionFailure:
            sys.exit('Server not available.')

        self.edman_db = client[database]
        return self.edman_db

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
                    result = self.edman_db[collection].insert_many(bulk_list)
                    results.append({collection: result.inserted_ids})
                    # プログレスバー表示関係
                    doc_bar.update(len(bulk_list))
                    collection_bar.set_description(f'Processing {collection}')
                    collection_bar.update(1)

                except BulkWriteError as bwe:
                    print('インサートに失敗しました:', bwe.details)
                    print('インサート結果:', results)
            doc_bar.close()
            collection_bar.close()
        return results

    def collections(self, include_system_collections=False) -> tuple:
        """
        検索対象コレクション取得
        collection_names()が廃止予定なので同等の機能を持たせた

        :param include_system_collections: default False
        :return:
        """
        collections = self.db.list_collection_names()
        if not include_system_collections:
            collections = tuple(
                [s for s in collections if not re.match('^(system\.)', s)])
        return tuple(set(collections))

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
        for collection in self.collections():
            find_oid = self.db[collection].find_one({'_id': oid})
            if find_oid is not None and '_id' in find_oid:
                result = collection
                break
        return result

    def create(self, admin: dict, user: dict, ini_dir: Path, host='127.0.0.1',
               port=27017) -> None:
        """
        ユーザ権限のDBを作成する

        :param dict admin: 管理者のユーザ情報
        :param dict user: 作成するユーザ情報
        :param str host: ホスト名
        :param int port: 接続ポート
        :param Path ini_dir: 接続情報用iniファイルの格納場所
        :return:
        """
        admindb = admin['dbname']
        adminname = admin['name']
        adminpwd = admin['pwd']

        userdb = user['dbname']
        username = user['name']
        userpwd = user['pwd']

        try:
            client = pymongo.MongoClient(host, port)
            client[admindb].command('ismaster')
        except ConnectionFailure:
            sys.exit('DB server not exists.')

        try:  # DB管理者認証
            client[admindb].authenticate(adminname, adminpwd)
        except OperationFailure:
            sys.exit('Authenticate failed.')

        if userdb in client.list_database_names():
            sys.exit('DB name is duplicated.')

        # 指定のDBを作成
        try:
            client[userdb].command(
                "createUser",
                username,
                pwd=userpwd,
                roles=[
                    {
                        'role': 'dbOwner',
                        'db': userdb,
                    },
                ],
            )
            client[userdb].authenticate(username, userpwd)
        except OperationFailure:
            sys.exit('DB creation failed.')

        # 初期データを入力
        # MongoDBはデータが入力されるまでDBが作成されないため
        db = client[userdb]
        init_collection = 'init'
        try:
            result = db[init_collection].insert_one({'generate': True})
            db[init_collection].delete_one({'_id': result.inserted_id})
            db[init_collection].drop()
            print('DB Create OK.')
        except OperationFailure:
            print(f"""
            Initialization failed.
            Please delete manually if there is data remaining.
            """)

        # iniファイル書き出し処理
        ini_data = {
            'host': host,
            'port': port,
            'username': username,
            'userpwd': userpwd,
            'dbname': userdb
        }
        self._create_ini(ini_data, ini_dir)

    @staticmethod
    def _create_ini(ini_data: dict, ini_dir: Path) -> None:
        """
        指定のディレクトリにiniファイルを作成
        同名ファイルがあった場合はname_[file_count +1].iniとして作成

        :param dict ini_data: 接続情報用iniファイルに記載するデータ
        :param Path ini_dir: 格納場所
        :return:
        """
        # この値は現在固定
        name = 'db'
        ext = '.ini'

        default_filename = name + ext
        ini_files = tuple(
            [i.name for i in tuple(ini_dir.glob(name + '*' + ext))])

        if default_filename in ini_files:
            filename = name + '_' + str(len(ini_files) + 1) + ext
            print(f'{default_filename} is exists.Create it as a [{filename}].')
        else:
            filename = default_filename

        # iniファイルの内容
        put_data = [
            '[DB]',
            'mongo_statement = mongodb://' + ini_data['username'] + ':' +
            ini_data[
                'userpwd'] + '@' + ini_data['host'] + ':' + str(
                ini_data['port']) + '/',
            'db_name = ' + ini_data['dbname'] + '\n'
        ]

        # iniファイルの書き出し
        savefile = ini_dir / filename
        try:
            with savefile.open("w") as file:
                file.writelines('\n'.join(put_data))
                file.flush()
                os.fsync(file.fileno())

                print(f'Create {savefile}')
        except IOError:
            print('ini file not create.Please create it manually.')

    @staticmethod
    def destroy(user: dict, host: str, port: int, admin=None,
                del_user=False) -> None:
        """
        DBを削除する
        del_userがTrue, かつadmin_accountがNoneでない時はユーザも削除する

        :param dict user: 削除対象のユーザデータ
        :param str host: ホスト名
        :param int port: ポート番号
        :param dict or None admin: ユーザを削除する場合のみ管理者のデータが必要
        :param bool del_user: ユーザ削除フラグ
        :return: None
        """
        if del_user and admin is None:
            sys.exit('You need administrator privileges to delete users.')

        userdb = user['dbname']
        userid = user['name']
        userpwd = user['pwd']

        client = pymongo.MongoClient(host, port)

        # DB削除
        try:
            client[userdb].authenticate(userid, userpwd)
            client.drop_database(userdb)
            print('DB delete OK.')
        except OperationFailure:
            sys.exit('Delete DB failed. Delete DB in Mongo shell.')

        # admin権限にてユーザを削除
        if del_user:
            admindb = admin['dbname']
            adminid = admin['name']
            adminpwd = admin['pwd']
            try:
                client[admindb].authenticate(adminid, adminpwd)
                db = client[userdb]
                db.command("dropUser", userid)
                print('DB User delete OK.')
            except OperationFailure:
                sys.exit('Delete user failed. Delete user in Mongo shell.')

    def _reference_item_delete(self, doc: dict) -> dict:
        """
        _id、親と子のリファレンス、ファイルリファレンスなどを削除

        :param dict doc:
        :return: dict item
        """

        if '_id' in doc:
            del doc['_id']
        if self.child in doc:
            del doc[self.child]
        if self.parent in doc:
            del doc[self.parent]
        if self.file_ref in doc:
            del doc[self.file_ref]
        return doc

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

        result = self._reference_item_delete(
            doc_result) if reference_delete else doc_result

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

    def _convert_datetime(self, amend: dict) -> dict:
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
            converted_amend_data = self._convert_datetime(amend_data)
            amended = {**db_result, **converted_amend_data}
        else:
            sys.exit('structureはrefまたはembの指定が必要です')

        try:
            replace_result = self.db[collection].replace_one({'_id': oid},
                                                             amended)
        except OperationFailure:
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
