import sys
import os
import json
import ast
from pathlib import Path
from typing import Tuple, Iterator, Union
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure


class Action:
    """
    ラッパーやファイル読み込み操作、書き出しなどの操作用
    """

    @staticmethod
    def file_gen(files: Tuple[Path]) -> Union[Iterator[dict], Iterator[str]]:
        """
        ファイルタプルからデータを取り出すジェネレータ
        XMLならstr、jsonなら辞書を返す

        :param tuple files:中身はPathオブジェクト
        :yield dict read_data or str read_data:
        """
        for file in files:

            if 'xml' in file.suffix:
                try:
                    with file.open() as f:
                        read_data = f.read()
                        print(f'{str(file)} '
                              f'Converting from XML to dictionary...')
                except IOError:
                    sys.exit(f'File is not read. {str(file)}')
                yield read_data

            if 'json' in file.suffix:
                try:
                    with file.open(encoding='utf8') as f:
                        read_data = json.load(f)
                        print(f'Processing to {file}')
                except json.JSONDecodeError:
                    sys.exit(f'File is not json format. {file}')
                yield read_data

    @staticmethod
    def files_read(file_or_dir_path: Union[str, Path], suffix=None) -> tuple:
        """
        ファイルのパスを、
        単一ファイルもディレクトリ内の複数ファイルもタプルにして返す
        拡張子の指定をすると、ディレクトリ内の該当のファイルのみ取得

        :param str or Path file_or_dir_path:
        :param str or None suffix:
        :return tuple files:
        """
        if isinstance(file_or_dir_path, str):
            p = Path(file_or_dir_path)
        elif isinstance(file_or_dir_path, Path):
            p = file_or_dir_path
        else:
            sys.exit('file_read() is a str or path Object is required.')

        if not p.exists():
            sys.exit('That path does not exist.')

        if suffix is None:
            files = tuple(sorted(p.glob('*'))) if p.is_dir() else (p,)
            message = ''
        else:
            files = (
                tuple(sorted(p.glob(f'*.{suffix}')))
                if p.is_dir() else (p,)
                if f'.{suffix}' in p.suffix else tuple()
            )
            message = suffix

        if not len(files):
            sys.exit(f'{message} files not in directory.')

        return files

    @staticmethod
    def query_eval(raw_query: str) -> dict:
        """
        文字列のクエリを受け取り、辞書に変換する

        :param str raw_query:
        :return dict query:
        """
        error_message = 'クエリが正しくありません。\"{}\"で囲みpythonの辞書形式にしてください'
        try:
            query = ast.literal_eval(raw_query)
        except SyntaxError:
            sys.exit(error_message)
        if not isinstance(query, dict):
            sys.exit(error_message)
        return query

    @staticmethod
    def file_query_eval(raw_query: str, structure: str) -> list:
        """
        embの場合文字列のクエリを受け取り、リストに変換する
        refの場合はNoneを返す

        :param raw_query:
        :param structure:
        :return:
        """
        # embのときだけクエリが必須
        if 'ref' in structure:
            query = None
        elif 'emb' in structure:
            if raw_query is None:
                sys.exit('クエリがありません')
            # embの時はリストに変換
            error_message = 'クエリがリスト形式ではありません.'
            try:
                query = ast.literal_eval(raw_query)
            except ValueError:
                sys.exit(error_message)
            if not isinstance(query, list):
                sys.exit(error_message)
        else:
            sys.exit('ref or embを指定してください')
        return query

    @staticmethod
    def create(admin: dict, user: dict, ini_dir: Path, host='127.0.0.1',
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
        print('DB Create OK.')

        # 初期データを入力
        # MongoDBはデータが入力されるまでDBが作成されないため
        # db = client[userdb]
        # init_collection = 'init'
        # try:
        #     result = db[init_collection].insert_one({'generate': True})
        #     db[init_collection].delete_one({'_id': result.inserted_id})
        #     db[init_collection].drop()
        #     print('DB Create OK.')
        # except OperationFailure:
        #     print(f"""
        #     Initialization failed.
        #     Please delete manually if there is data remaining.
        #     """)

        # iniファイル書き出し処理
        ini_data = {
            'host': host,
            'port': port,
            'username': username,
            'userpwd': userpwd,
            'dbname': userdb
        }
        Action.create_ini(ini_data, ini_dir)

    @staticmethod
    def create_ini(ini_data: dict, ini_dir: Path) -> None:
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
        # put_data = [
        #     '[DB]',
        #     'mongo_statement = mongodb://' + ini_data['username'] + ':' +
        #     ini_data[
        #         'userpwd'] + '@' + ini_data['host'] + ':' + str(
        #         ini_data['port']) + '/',
        #     'db_name = ' + ini_data['dbname'] + '\n'
        # ]
        put_data = [
            '[DB]',
            '# DB user settings\n',
            '# MongoDB default port 27017',
            'port = ' + str(ini_data['port']) + '\n',
            '# MongoDB server host',
            'host = ' + ini_data['host'] + '\n',
            'database = ' + ini_data['dbname'],
            'user = ' + ini_data['username'],
            'password = ' + ini_data['userpwd'] + '\n',
            '[COLLECTION]',
            '# Collection name in MongoDB of file section in XML',
            'file = ' + '_file' + '\n'
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
