import os
import shutil
import copy
import gzip
from tempfile import TemporaryDirectory, NamedTemporaryFile
import datetime
import zipfile
from typing import Union, Tuple, Iterator, List, Any
from pathlib import Path
import gridfs
from gridfs.errors import NoFile, GridFSError
import jmespath
from bson import ObjectId
from edman.utils import Utils
from edman.exceptions import (EdmanFormatError, EdmanDbProcessError,
                              EdmanInternalError)
from edman import Config


class File:
    """
    ファイル取扱クラス
    """

    def __init__(self, db=None) -> None:

        if db is not None:
            self.db = db
            self.fs = gridfs.GridFS(self.db)
        self.file_ref = Config.file
        self.comp_level = Config.gzip_compress_level

    @staticmethod
    def file_gen(files: Tuple[Path]) -> Iterator:
        """
        ファイルタプルからファイルを取り出すジェネレータ

        :param tuple files: 中身はPathオブジェクト
        :return: ファイル名と内容(str)のタプル
        :rtype: tuple
        """
        for file in files:
            try:
                with file.open('rb') as f:
                    fp = f.read()
            except IOError:
                raise
            yield file.name, fp

    def delete(self, delete_oid: ObjectId, collection: str,
               oid: Union[ObjectId, str], structure: str, query=None) -> bool:
        """
        該当のoidをファイルリファレンスから削除し、GridFSからファイルを削除

        :param ObjectId delete_oid:
        :param str collection:
        :param str oid:
        :param str structure:
        :param query:
        :type query: list or None
        :return:
        :rtype: bool
        """
        oid = Utils.conv_objectid(oid)

        # ドキュメント存在確認&コレクション存在確認&対象ドキュメント取得
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            raise EdmanInternalError('対象のコレクション、またはドキュメントが存在しません')

        # ファイルリスト取得
        files_list = self.get_file_ref(doc, structure, query)

        # リファレンスデータを編集
        if len(files_list) > 0:
            # 何らかの原因で重複があった場合を避けるため一度setにする
            files_list = list(set(files_list))
            files_list.remove(delete_oid)
        else:
            raise EdmanDbProcessError('ファイルが存在しません')

        # ドキュメントを新しいファイルリファレンスに置き換える
        if structure == 'ref':
            try:
                new_doc = self.file_list_replace(doc, files_list)
            except Exception:
                raise
        elif structure == 'emb':
            try:
                new_doc = Utils.doc_traverse(doc, files_list, query,
                                             self.file_list_replace)
            except Exception:
                raise
        else:
            raise EdmanFormatError('structureはrefまたはembの指定が必要です')

        replace_result = self.db[collection].replace_one({'_id': oid}, new_doc)
        # fsから該当ファイルを削除
        if replace_result.modified_count:
            self.fs_delete([delete_oid])

        # ファイルが削除されればOK
        return False if self.fs.exists(delete_oid) else True

    def fs_delete(self, oids: list) -> None:
        """
        fsからファイル削除

        :param list oids:
        :return:
        """
        for oid in oids:
            if self.fs.exists(oid):
                self.fs.delete(oid)

    def get_file_ref(self, doc: dict, structure: str, query=None) -> list:
        """
        ファイルリファレンス情報を取得

        :param dict doc:
        :param str structure:
        :param query:
        :type: list or None
        :return: files_list
        :rtype: list
        """
        if structure == 'emb' and query is None:
            raise EdmanFormatError('embにはクエリが必要です')
        if structure != 'emb' and structure != 'ref':
            raise EdmanFormatError('構造の選択はembまたはrefが必要です')

        if structure == 'ref':
            files_list = doc.get(self.file_ref, [])
        else:
            if not Utils.query_check(query, doc):
                EdmanFormatError('対象のドキュメントに対してクエリーが一致しません.')
            # docから対象クエリを利用してファイルのリストを取得
            # deepcopyを使用しないとなぜか子のスコープのqueryがクリヤーされる
            query_c = copy.deepcopy(query)
            try:
                files_list = self._get_emb_files_list(doc, query_c)
            except Exception:
                raise

        return files_list

    def get_file_names(self, collection: str, oid: Union[ObjectId, str],
                       structure: str, query=None) -> dict:
        """
        ファイル一覧を取得
        ファイルが存在しなければ空の辞書を返す

        :param str collection:
        :param str oid:
        :param str structure:
        :param query: embの時だけ必要. refの時はNone
        :type query: list or None
        :return: result
        :rtype: dict
        """
        oid = Utils.conv_objectid(oid)
        result = {}

        # ドキュメント存在確認&コレクション存在確認&対象ドキュメント取得
        if (doc := self.db[collection].find_one({'_id': oid})) is None:
            raise EdmanDbProcessError(
                f'ドキュメントまたはコレクションが存在しません oid:{oid} collection:{collection}')

        # gridfsからファイル名を取り出す
        for file_oid in self.get_file_ref(doc, structure, query):
            try:
                fs_out = self.fs.get(file_oid)
            except gridfs.errors.NoFile:
                continue
            else:
                result.update({file_oid: fs_out.filename})
        return result

    def download(self, file_oid: list[ObjectId],
                 path: Union[str, Path]) -> bool:
        """
        Gridfsからデータをダウンロードし、ファイルに保存
        metadataに圧縮指定があれば伸長する

        :param list file_oid:
        :param path:
        :type path: str or Path
        :return: result
        :rtype: bool
        """
        # 定型的な前処理があればここに追加する
        return self._grid_out(file_oid, path)

    def _grid_out(self, file_oid_list: List[ObjectId],
                  path: Union[str, Path]) -> bool:
        """
        Gridfsからデータを取得し、ファイルに保存
        metadataに圧縮指定があれば伸長する
        複数のファイルを指定すると、複数のファイルが作成される

        :param list file_oid_list:
        :param path:
        :type path: str or Path
        :return: result
        :rtype: bool
        """

        # パスがstrならpathlibにする
        p = Path(path) if isinstance(path, str) else path

        # パスが正しいか検証
        if not p.exists():
            raise FileNotFoundError
        # ファイルが存在するか検証
        if False in map(self.fs.exists, file_oid_list):
            raise EdmanDbProcessError('指定のファイルはDBに存在しません')
        else:
            # ダウンロード処理
            results = []
            for file_oid in file_oid_list:
                fs_out = self.fs.get(file_oid)
                save_path = p / fs_out.filename
                try:
                    with save_path.open('wb') as f:
                        tmp = fs_out.read()
                        # if hasattr(fs_out,
                        #            'compress') and fs_out.compress == 'gzip':
                        #     tmp = gzip.decompress(tmp)
                        f.write(tmp)
                        f.flush()
                        os.fsync(f.fileno())
                except IOError:
                    raise
                results.append(save_path.exists())
        return all(results)

    def upload(self, collection: str, oid: Union[ObjectId, str],
               file_path: Tuple[Tuple[Any, bool]], structure: str,
               query=None) -> bool:
        """
        ドキュメントにファイルリファレンスを追加する
        ファイルのインサート処理、圧縮処理なども行う
        :param str collection:
        :param oid:
        :type oid: ObjectId or str
        :param tuple file_path:ドキュメントに添付する単数or複数のファイルパス
        :param str structure:
        :param query:
        :type query: list or None
        :return:
        :rtype: bool
        """

        oid = Utils.conv_objectid(oid)

        # ドキュメント存在確認&対象ドキュメント取得
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            raise EdmanInternalError('対象のドキュメントが存在しません')
        if structure == 'emb':
            # クエリーがドキュメントのキーとして存在するかチェック
            if not Utils.query_check(query, doc):
                raise EdmanFormatError('対象のドキュメントに対してクエリーが一致しません.')

        # ファイルのインサート
        inserted_file_oids = self.grid_in(file_path)
        if structure == 'ref':
            new_doc = self.file_list_attachment(doc, inserted_file_oids)
        elif structure == 'emb':
            try:
                new_doc = Utils.doc_traverse(doc, inserted_file_oids, query,
                                             self.file_list_attachment)
            except Exception:
                raise
        else:
            raise EdmanFormatError('構造はrefかembが必要です')

        # ドキュメント差し替え
        replace_result = self.db[collection].replace_one({'_id': oid}, new_doc)

        if replace_result.modified_count == 1:
            return True
        else:  # 差し替えができなかった時は添付ファイルは削除
            self.fs_delete(inserted_file_oids)
            return False

    def grid_in(self, files: Tuple[Tuple[Any, bool]]) -> list[Any]:
        """
        Gridfsへ複数のデータをアップロードし

        :param tuple files:
        :return: inserted
        :rtype: list
        """
        inserted = []
        for file, compress in files:
            try:
                with file.open('rb') as f:
                    fb = f.read()
                    # if compress:
                    #     fb = gzip.compress(fb, compresslevel=self.comp_level)
                    #     compress = 'gzip'
                    # else:
                    #     compress = None
                    # metadata = {'filename': os.path.basename(f.name),
                    #             'compress': compress}
                    metadata = {'filename': os.path.basename(f.name)}

            except (IOError, OSError) as e:
                raise EdmanDbProcessError(e)
            try:
                inserted.append(self.fs.put(fb, **metadata))
            except GridFSError as e:
                raise EdmanDbProcessError(e)
        return inserted

    def file_list_attachment(self, doc: dict,
                             files_oid: List[ObjectId]) -> dict:
        """
        辞書データにファイルのoidを挿入する
        docにself.file_refがあれば、追加する処理
        oidが重複していないものだけ追加
        ファイルが同じでも別のoidが与えられていれば追加される

        :param dict doc:
        :param list files_oid: ObjectIdのリスト
        :return: doc
        :rtype: dict
        """
        if self.file_ref in doc:
            doc[self.file_ref].extend(files_oid)
            files_oid = sorted(list(set(doc[self.file_ref])))
        # self.file_refがなければ作成してfiles_oidを値として更新
        if files_oid:
            doc.update({self.file_ref: files_oid})
        return doc

    def file_list_replace(self, doc: dict, files_oid: list) -> dict:
        """
        ドキュメントのファイルリファレンスを入力されたリストに置き換える
        もし空リストならファイルリファレンス自体を削除する
        すでにファイルリファレンスデータが存在していることを前提としているため、
        docにファイルリファレンスデータが無かった場合は例外を発生する

        :param dict doc:
        :param list files_oid:
        :return: doc
        :rtype: dict
        """
        if self.file_ref in doc:
            if files_oid:
                doc[self.file_ref] = files_oid
            else:
                del doc[self.file_ref]
        else:
            raise ValueError(f'{self.file_ref}がないか削除された可能性があります')
        return doc

    def _get_emb_files_list(self, doc: dict, query: list) -> list:
        """
        ドキュメントからファイルリファレンスのリストを取得する

        :param dict doc:
        :param list query:
        :return:
        :rtype:list
        """
        s = Utils.generate_jms_query(query)
        s += '.' + self.file_ref + '[]'
        return jmespath.search(s, doc)

    @staticmethod
    def generate_zip_filename(filename=None) -> str:
        """
        '%Y%m%d%H%M%S'.zipのファイル名を生成
        任意の文字列を指定すると, '%Y%m%d%H%M%S' + 任意の文字列 + .zipを生成

        :param any filename: strにキャストされる
        :return:
        :rtype: str
        """
        filename = filename or None
        now = datetime.datetime.now()
        name = now.strftime('%Y%m%d%H%M%S')
        if filename is not None:
            name = name + str(filename)
        return name + '.zip'

    def zipped_contents(self, downloads: dict, json_tree_file_name: str,
                        encoded_json: bytes) -> str:
        """
        jsonと添付ファイルを含むzipファイルを生成
        zipファイル内部にjson_tree_file_name.jsonのjsonファイルを含む
        添付ファイルがなく、jsonファイルだけ取得したい場合はzipped_jsonを利用

        :param dict downloads:
        :param str json_tree_file_name:
        :param bytes encoded_json:
        :rtype: str
        :return:
        """

        zip_suffix = '.zip'
        json_suffix = '.json'

        with TemporaryDirectory() as tmpdir:
            dir_path_list = [os.path.join(tmpdir, str(doc_oid)) for
                             doc_oid in downloads]

            for dir_path in dir_path_list:
                os.mkdir(dir_path)

            for file_refs, dir_path in zip([i for i in downloads.values()],
                                           dir_path_list):
                for file_ref in file_refs:
                    try:
                        content = self.fs.get(file_ref)
                    except NoFile:
                        raise EdmanDbProcessError('指定の関連ファイルが存在しません')
                    except GridFSError:
                        raise
                    else:
                        # 圧縮設定の場合はその拡張子を追加
                        # if content.compress is not None:
                        #     filename = content.name + '.' + content.compress
                        # else:
                        #     filename = content.name
                        filename = content.name
                        filepath = os.path.join(dir_path, filename)
                    try:
                        with open(filepath, 'wb') as f:
                            f.write(content.read())
                    except (FileNotFoundError, IOError):
                        EdmanInternalError('ファイルを保存することが出来ませんでした')
                    except GridFSError:
                        raise
            try:
                # jsonファイルとして一時ディレクトリに保存
                with open(os.path.join(tmpdir,
                                       json_tree_file_name + json_suffix),
                          'wb') as f:
                    f.write(encoded_json)
            except (FileNotFoundError, IOError):
                EdmanInternalError('ファイルを保存することが出来ませんでした')
            try:
                # 最終的にDLするzipファイルを作成
                with NamedTemporaryFile() as fp:
                    zip_filepath = shutil.make_archive(fp.name, zip_suffix[1:],
                                                       tmpdir)
            except Exception:
                raise
        return zip_filepath

    @staticmethod
    def zipped_json(encoded_json: bytes, json_tree_file_name: str) -> str:
        """
        jsonファイルとzipファイルを名前付きテンポラリとして生成し、パスを生成
        zipファイル内部にjson_tree_file_name.jsonのjsonファイルを含む
        添付ファイルがなく、jsonファイルだけ取得したい場合に使用する
        # リファクタリング対象

        :param bytes encoded_json: json文字列としてダンプしたものを指定の文字コードでバイト列に変換したもの
        :param str json_tree_file_name: zipファイル内に配置するjsonファイルの名前
        :return:
        :rtype:str
        """

        json_suffix = '.json'
        zip_suffix = '.zip'
        try:
            # jsonファイルとして一時ファイルに保存
            with NamedTemporaryFile(suffix=json_suffix,
                                    delete=False) as fp:
                filepath = fp.name
                fp.write(encoded_json)
        except Exception:
            raise
        try:
            # jsonファイルをzipで圧縮して一時ファイルに保存
            with NamedTemporaryFile(suffix=zip_suffix,
                                    delete=False) as fp:
                with zipfile.ZipFile(fp.name, 'w',
                                     zipfile.ZIP_DEFLATED) as archive:
                    zip_filepath = fp.name
                    archive.write(filepath,
                                  arcname=json_tree_file_name + json_suffix)
        except Exception:
            raise
        return zip_filepath

    def get_fileref_and_generate_dl_list(self, docs: dict,
                                         attach_key: str) -> tuple[dict, dict]:
        """
        json出力用の辞書内のファイルリファレンスをファイルパス文字列リストに置き換える
            例:
            {"_ed_file":[ObjectId('file_oid_1'),ObjectId('file_oid_2')]}
            ↓
            {"_ed_attachment":["document_oid/sample.jpg","document_oid/sample2.jpg"]}
        同時にダウンロード処理用の辞書を作成する
            {ObjectId('document_oid'):[ObjectId('file_oid_1'),ObjectId('file_oid_2')]}

        :param dict docs:
        :param str attach_key:
        :return:
        :rtype: tuple
        """
        dl_list = {}

        def recursive(data: dict, doc_oid=None):
            c_docs = {}
            for key, value in data.items():
                if isinstance(data[key], dict):
                    # ドキュメントのoidを取得
                    if '_id' in data[key]:
                        doc_oid = data[key]['_id']
                    c_docs.update({key: recursive(data[key], doc_oid)})
                elif isinstance(value, list) and Utils.item_literal_check(
                        value):
                    if Config.file in key:
                        # ファイルリファレンスオブジェクト取得
                        try:
                            file_out = [self.fs.get(i) for i in value]
                        except NoFile:
                            raise EdmanDbProcessError('指定の関連ファイルが存在しません')
                        except GridFSError:
                            raise
                        # ファイルパス生成
                        tmp = {
                            attach_key: [
                                str(data['_id']) + '/' + i.filename for i
                                in file_out]
                        }
                        dl_list.update({data['_id']: value})
                    else:
                        tmp = {key: value}
                    c_docs.update(tmp)
                elif isinstance(data[key], list):
                    c_docs.update(
                        {key: [recursive(item, doc_oid) for item in
                               data[key]]})
                else:
                    c_docs.update({key: value})
            return c_docs

        new_docs = recursive(docs)
        return new_docs, dl_list
