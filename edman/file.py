import sys
import os
import copy
from typing import Union, Tuple, Iterator, List
from pathlib import Path
import gridfs
import jmespath
from bson import ObjectId
from edman.utils import Utils
from edman.config import Config


class File:
    """
    ファイル取扱クラス
    """

    def __init__(self, db=None) -> None:

        if db is not None:
            self.db = db
            self.fs = gridfs.GridFS(self.db)
        self.file_ref = Config.file

    def connect(self):
        return self.fs

    @staticmethod
    def file_gen(files: Tuple[Path]) -> Iterator:
        """
        ファイルタプルからファイルを取り出すジェネレータ

        :param tuple files: 中身はPathオブジェクト
        :yield fp:
        """
        for file in files:
            try:
                with file.open('rb') as f:
                    fp = f.read()
            except IOError:
                sys.exit(f'ファイル読み込みできませんでした {file}')
            yield (file.name, fp)

    def add_file_reference(self, collection: str, oid: Union[ObjectId, str],
                           file_path: Tuple[Path], structure: str,
                           query=None) -> bool:
        """
        ドキュメントにファイルリファレンスを追加する

        :param str collection:
        :param ObjectId or str oid:
        :param tuple file_path:
        :param str structure:
        :param list or None query:
        :return bool:
        """

        oid = Utils.conv_objectid(oid)

        # ドキュメント存在確認&対象ドキュメント取得
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            sys.exit('対象のドキュメントが存在しません')

        if structure == 'emb':
            # クエリーがドキュメントのキーとして存在するかチェック
            if not self._query_check(query, doc):
                sys.exit('対象のドキュメントに対してクエリーが一致しません.')

        # ファイルのインサート
        inserted_file_oids = [self.fs.put(file[1], filename=file[0]) for file
                              in self.file_gen(file_path)]

        if structure == 'ref':
            new_doc = self._file_list_attachment(doc, inserted_file_oids)

        elif structure == 'emb':
            try:
                new_doc = Utils.doc_traverse(doc, inserted_file_oids, query,
                                             self._file_list_attachment)
            except Exception as e:
                sys.exit(e)
        else:
            sys.exit('構造はrefかembが必要です')

        # ドキュメント差し替え
        replace_result = self.db[collection].replace_one({'_id': oid}, new_doc)

        if replace_result.modified_count == 1:
            return True
        else:  # 差し替えができなかった時は添付ファイルは削除
            self._fs_delete(inserted_file_oids)
            return False

    def delete(self, delete_oid: ObjectId, collection: str,
               oid: Union[ObjectId, str],
               structure: str, query=None) -> bool:
        """
        該当のoidをファイルリファレンスから削除し、GridFSからファイルを削除

        :param ObjectId delete_oid:
        :param str collection:
        :param str oid:
        :param str structure:
        :param list or None query:
        :return bool:
        """

        oid = Utils.conv_objectid(oid)

        # ドキュメント存在確認&コレクション存在確認&対象ドキュメント取得
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            sys.exit('対象のコレクション、またはドキュメントが存在しません')

        # ファイルリスト取得
        files_list = self.get_file_ref(doc, structure, query)

        # リファレンスデータを編集
        if len(files_list) > 0:
            # 何らかの原因で重複があった場合を避けるため一度setにする
            files_list = list(set(files_list))
            files_list.remove(delete_oid)
        else:
            sys.exit('ファイルが存在しません')

        # ドキュメントを新しいファイルリファレンスに置き換える
        if structure == 'ref':
            try:
                new_doc = self._file_list_replace(doc, files_list)
            except Exception as e:
                sys.exit(e)
        elif structure == 'emb':
            try:
                new_doc = Utils.doc_traverse(doc, files_list, query,
                                             self._file_list_replace)
            except Exception as e:
                sys.exit(e)
        else:
            sys.exit('structureはrefまたはembの指定が必要です')

        replace_result = self.db[collection].replace_one({'_id': oid}, new_doc)

        # fsから該当ファイルを削除
        if replace_result.modified_count:
            self._fs_delete([delete_oid])

        # ファイルが削除されたか検証
        if self.fs.exists(delete_oid):
            return False
        else:
            return True

    def _fs_delete(self, oids: list) -> None:
        """
        fsからファイル削除

        :param list oids:
        :return:
        """
        for oid in oids:
            if self.fs.exists(oid):
                self.fs.delete(oid)

    @staticmethod
    def _query_check(query: list, doc: dict) -> bool:
        """
        クエリーが正しいか評価

        :param list query:
        :param dict doc:
        :return bool result:
        """
        result = False
        for key in query:
            if key.isdecimal():
                key = int(key)
            try:
                doc = doc[key]
            except (KeyError, IndexError):  # インデクスの指定ミスは即時終了
                result = False
                break
        else:  # for~elseを利用していることに注意
            if isinstance(doc, dict):
                result = True
        return result

    def get_file_ref(self, doc: dict, structure: str, query=None) -> list:
        """
        ファイルリファレンス情報を取得

        :param dict doc:
        :param str structure:
        :param list or None query:
        :return list files_list:
        """
        if structure == 'emb' and query is None:
            sys.exit('embにはクエリが必要です')
        if structure != 'emb' and structure != 'ref':
            sys.exit('構造の選択はembまたはrefが必要です')

        files_list = []
        if structure == 'ref':
            if self.file_ref in doc:
                files_list = doc[self.file_ref]
        else:
            if not self._query_check(query, doc):
                sys.exit('対象のドキュメントに対してクエリーが一致しません.')
            # docから対象クエリを利用してファイルのリストを取得
            # deepcopyを使用しないとなぜか子のスコープのqueryがクリヤーされる
            query_c = copy.deepcopy(query)
            try:
                files_list = self._get_emb_files_list(doc, query_c)
            except Exception as e:
                sys.exit(e)
        return files_list

    def get_file_names(self, collection: str, oid: Union[ObjectId, str],
                       structure: str,
                       query: Union[list, None]) -> dict:
        """
        ファイル一覧を取得

        :param str collection:
        :param str oid:
        :param str structure:
        :param list or None query: embの時だけ必要. refの時はNone
        :return dict result:
        """
        oid = Utils.conv_objectid(oid)

        # ドキュメント存在確認&コレクション存在確認&対象ドキュメント取得
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            sys.exit('対象のコレクション、またはドキュメントが存在しません')

        # ファイルリスト取得
        files_list = self.get_file_ref(doc, structure, query)

        result = {}
        if files_list:
            # ファイルリストを元にgridfsからファイル名を取り出す
            for file_oid in files_list:
                fs_out = self.fs.get(file_oid)
                result.update({file_oid: fs_out.filename})
        else:
            sys.exit('関連ファイルはありません')

        return result

    def download(self, oid: ObjectId, path: Union[str, Path]) -> bool:
        """
        Gridfsからデータをダウンロードし、ファイルに保存

        :param ObjectId oid:
        :param str or Path path:
        :return bool result:
        """
        result = False

        # パスがstrならpathlibにする
        p = Path(path) if isinstance(path, str) else path

        # パスが正しいか検証
        if not p.exists():
            sys.exit('パスが正しくないです')

        # ダウンロード処理
        if self.fs.exists(oid):
            fs_out = self.fs.get(oid)
            save_path = p / fs_out.filename

            try:
                with save_path.open('wb') as f:
                    f.write(fs_out.read())
                    f.flush()
                    os.fsync(f.fileno())
            except IOError:
                sys.exit('ファイルに書き込めませんでした')

            if save_path.exists():
                result = True

        else:
            sys.exit('指定のファイルはDBに存在しません')

        return result

    def _file_list_attachment(self, doc: dict,
                              files_oid: List[ObjectId]) -> dict:
        """
        辞書データにファイルのoidを挿入する
        docにself.file_refがあれば、追加する処理
        oidが重複していないものだけ追加
        ファイルが同じでも別のoidが与えられていれば追加される

        :param dict doc:
        :param list files_oid: ObjectIdのリスト
        :return dict doc:
        """

        if self.file_ref in doc:
            doc[self.file_ref].extend(files_oid)
            files_oid = sorted(list(set(doc[self.file_ref])))
        # self.file_refがなければ作成してfiles_oidを値として更新
        if len(files_oid) != 0:
            doc.update({self.file_ref: files_oid})

        return doc

    def _file_list_replace(self, doc: dict, files_oid: list) -> dict:
        """
        ドキュメントのファイルリファレンスを入力されたリストに置き換える
        もし空リストならファイルリファレンス自体を削除する
         すでにファイルリファレンスデータが存在していることを前提としているため、
         docにファイルリファレンスデータが無かった場合は例外を発生する

        :param dict doc:
        :param list files_oid:
        :return dict doc:
        """
        if self.file_ref in doc:
            if len(files_oid) == 0:
                del doc[self.file_ref]
            else:
                doc[self.file_ref] = files_oid
        else:
            raise ValueError(f'{self.file_ref}がないか削除された可能性があります')

        return doc

    def _get_emb_files_list(self, doc: dict, query: list) -> list:
        """
        ドキュメントからファイルリファレンスのリストを取得する

        :param dict doc:
        :param list query:
        :return list:
        """
        s = ''

        for idx, i in enumerate(query):
            if i.isdecimal():
                s += '[' + i + ']'
            else:
                if idx != 0:
                    s += '.'
                s += i

        s += '.' + self.file_ref + '[]'

        return jmespath.search(s, doc)
