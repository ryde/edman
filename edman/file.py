import binascii
import copy
import datetime
import gzip
import json
import os
import shutil
import zipfile
from logging import INFO, getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import IO, Any, Iterator, List, Tuple

import gridfs
import jmespath
from bson import ObjectId
from gridfs.errors import GridFSError, NoFile

from edman import Config
from edman.exceptions import (EdmanDbProcessError, EdmanFormatError,
                              EdmanInternalError)
from edman.utils import Utils


class File:
    """
    ファイル取扱クラス
    """

    def __init__(self, db=None) -> None:

        if db is not None:
            self.db = db
            self.fs = gridfs.GridFS(self.db)
        self.file_ref = Config.file
        # self.comp_level = Config.gzip_compress_level
        self.file_attachment = Config.file_attachment

        # ログ設定(トップに伝搬し、利用側でログとして取得してもらう)
        self.logger = getLogger(__name__)
        self.logger.setLevel(INFO)
        self.logger.propagate = True

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
               oid: ObjectId | str, structure: str, query=None) -> bool:
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
        if structure not in ['ref', 'emb']:
            raise EdmanFormatError('structureはrefまたはembの指定が必要です')

        # ドキュメント存在確認&コレクション存在確認&対象ドキュメント取得
        if (doc := self.db[collection].find_one({'_id': oid})) is None:
            raise EdmanInternalError(
                '対象のコレクション、またはドキュメントが存在しません')

        # ファイルリスト取得
        files_list = self.get_file_ref(doc, structure, query)
        if len(files_list) == 0:
            raise EdmanDbProcessError('ファイルが存在しません')

        # リファレンスデータを編集
        # 何らかの原因で重複があった場合を避けるため一度setにする
        files_list = list(set(files_list))
        files_list.remove(delete_oid)

        # ドキュメントを新しいファイルリファレンスに置き換える
        try:
            if structure == 'ref':
                new_doc = self.file_list_replace(doc, files_list)
            else:
                new_doc = Utils.doc_traverse(doc, files_list, query,
                                             self.file_list_replace)
        except Exception:
            raise
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
                EdmanFormatError(
                    '対象のドキュメントに対してクエリーが一致しません.')
            # docから対象クエリを利用してファイルのリストを取得
            # deepcopyを使用しないとなぜか子のスコープのqueryがクリヤーされる
            query_c = copy.deepcopy(query)
            try:
                files_list = self._get_emb_files_list(doc, query_c)
            except Exception:
                raise

        return files_list

    def get_file_names(self, collection: str, oid: ObjectId | str,
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

    def download(self, file_oid: list[ObjectId], path: str | Path) -> bool:
        """
        Gridfsからデータをダウンロードし、ファイルに保存

        :param list file_oid:
        :param path:
        :type path: str or Path
        :return: result
        :rtype: bool
        """
        # 定型的な前処理があればここに追加する
        return self._grid_out(file_oid, path)

    def _grid_out(self, file_oid_list: List[ObjectId],
                  path: str | Path) -> bool:
        """
        Gridfsからデータを取得し、ファイルに保存
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

        # ダウンロード処理
        results = []
        for file_oid in file_oid_list:
            fs_out = self.fs.get(file_oid)
            save_path = p / fs_out.filename
            try:
                with save_path.open('wb') as f:
                    tmp = fs_out.read()
                    f.write(tmp)
                    f.flush()
                    os.fsync(f.fileno())
            except IOError:
                raise
            results.append(save_path.exists())

        return all(results)

    def upload(self, collection: str, oid: ObjectId | str,
               file_path: Tuple[Path], structure: str,
               query=None) -> bool:
        """
        ドキュメントにファイルリファレンスを追加する
        ファイルのインサート処理なども行う
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
        if structure not in ['ref', 'emb']:
            raise EdmanFormatError('構造はrefかembが必要です')

        # ドキュメント存在確認&対象ドキュメント取得
        doc = self.db[collection].find_one({'_id': oid})
        if doc is None:
            raise EdmanInternalError('対象のドキュメントが存在しません')
        if structure == 'emb':
            # クエリーがドキュメントのキーとして存在するかチェック
            if not Utils.query_check(query, doc):
                raise EdmanFormatError(
                    '対象のドキュメントに対してクエリーが一致しません.')

        # ファイルのインサート
        inserted_file_oids = self.grid_in(file_path)
        if structure == 'ref':
            new_doc = self.file_list_attachment(doc, inserted_file_oids)
        else:
            try:
                new_doc = Utils.doc_traverse(doc, inserted_file_oids, query,
                                             self.file_list_attachment)
            except Exception:
                raise

        # ドキュメント差し替え
        replace_result = self.db[collection].replace_one({'_id': oid}, new_doc)
        if replace_result.modified_count == 1:
            result = True
        else:  # 差し替えができなかった時は添付ファイルは削除
            self.fs_delete(inserted_file_oids)
            result = False

        return result

    def grid_in(self, files: Tuple[Path, ...]) -> list[ObjectId]:
        """
        Gridfsへ複数のデータをアップロード

        :param tuple files:
        :return: inserted
        :rtype: list
        """
        inserted = []
        for file in files:
            try:
                with file.open('rb') as f:
                    fb = f.read()
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
            raise ValueError(
                f'{self.file_ref}がないか削除された可能性があります')
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
                        encoded_json: bytes, p: Path) -> str:
        """
        jsonと添付ファイルを含むzipファイルを生成
        zipファイル内部にjson_tree_file_name.jsonのjsonファイルを含む
        添付ファイルがなく、jsonファイルだけ取得したい場合はzipped_jsonを利用

        :param dict downloads:
        :param str json_tree_file_name:
        :param bytes encoded_json:
        :param Path p:
        :rtype: str
        :return:
        """
        work = p
        p = work / 'archives'
        p.mkdir()
        for file_refs, dir_path in zip([i for i in downloads.values()],
                                       [p / str(doc_oid) for doc_oid in
                                        downloads]):
            os.mkdir(dir_path)
            for file_ref in file_refs:
                # 添付ファイルをダウンロード
                try:
                    content = self.fs.get(file_ref)
                except NoFile:
                    raise EdmanDbProcessError(
                        '指定の関連ファイルが存在しません')
                except GridFSError:
                    raise

                # 添付ファイルを保存
                filepath = dir_path / content.name

                content_data = content.read()
                try:
                    # gzip圧縮されている場合は解凍する
                    if binascii.hexlify(content_data[:2]) == b'1f8b':
                        content_data = gzip.decompress(content_data)
                except binascii.Error:
                    EdmanInternalError(
                        'gzipファイルの解凍に失敗しました: ' + content.name)
                try:
                    with open(filepath, 'wb') as f:
                        f.write(content_data)
                except (FileNotFoundError, IOError):
                    EdmanInternalError(
                        'ファイルを保存することが出来ませんでした: ' + content.name)

            # jsonファイルを保存
            json_file = json_tree_file_name + '.json'
            json_path = p / json_file
            try:
                with json_path.open('wb') as f:
                    f.write(encoded_json)
            except (FileNotFoundError, IOError):
                EdmanInternalError(
                    'JSONファイルを保存することが出来ませんでした')

        # 最終的にDLするzipファイルを作成
        base = work / 'dl'
        try:
            zip_filepath = shutil.make_archive(str(base), format='zip',
                                               root_dir=str(p))
        except Exception:
            raise
        return zip_filepath

    @staticmethod
    def zipped_json(encoded_json: bytes, json_tree_file_name: str,
                    p: Path) -> Path:
        """
        zipファイル内部にjson_tree_file_name.jsonのjsonファイルを含む
        添付ファイルがなく、jsonファイルだけ取得したい場合に使用する

        :param bytes encoded_json: json文字列としてダンプしたものを指定の文字コードでバイト列に変換したもの
        :param str json_tree_file_name: zipファイル内に配置するjsonファイルの名前
        :param Path p: ファイルを保存するディレクトリのパス
        :return:zip_filepath
        :rtype:Path
        """
        zip_filename = json_tree_file_name + '.zip'
        filename = json_tree_file_name + '.json'
        jsonfile = p / filename
        zip_filepath = p / zip_filename

        try:
            with jsonfile.open('wb') as f:
                f.write(encoded_json)
        except Exception:
            raise
        try:
            with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as f:
                f.write(jsonfile, arcname=filename)
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
                            raise EdmanDbProcessError(
                                '指定の関連ファイルが存在しません')
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

    def upload_zipped(self, zip_file: IO) -> dict | None:
        """
        zipファイルを解凍し、ファイルをgridfsに格納、結果のoidを含めたjsonを返す

        :param IO zip_file: アップロードされたzipファイル
        :return:
        :rtype: dict
        """
        entry_json = None

        with TemporaryDirectory() as td:
            p = Path(td)
            with zipfile.ZipFile(zip_file) as ex_zip:
                ex_zip.extractall(td)

            json_list = list(p.glob('*.json'))
            if not json_list:
                raise EdmanInternalError('jsonファイルが存在しません')
            if len(json_list) > 1:
                raise EdmanInternalError(
                    'jsonファイルは一つだけしか含めることはできません')
            target_json = json_list[0]
            # jsonデータ取り出し
            with target_json.open() as f:
                json_data = json.load(f)
            try:
                # 添付ファイルを取り出す
                files_list = self.generate_upload_list(json_data)
                # 解凍したデータ内に、実際にデータが存在するか照合する(ダウンロードするパスを接合する)
                path_list = self.generate_file_path_dict(files_list, p)
                # gridfsにファイルを格納するために専用のタプルを作成
                paths = tuple([v for v in path_list.values()])
                # grid.fsに入れる
                grid_in_results = self.grid_in(paths)
                # grid_inはinserted_idしか返さないため、jsonのファイルパスをキーとしてinserted_oidをバリューとする辞書を作成する
                gf_inserted_dict = {i: j for i, j in
                                    zip(path_list, grid_in_results)}
            except Exception:
                raise
            else:
                # jsonデータにoidを書き加え、添付ファイル用キーを削除
                entry_json = self.json_rewrite(json_data, gf_inserted_dict)

        return entry_json

    def generate_upload_list(self, data: dict) -> list[str]:
        """
        json辞書からキーfiles_dir_keyの値であるリストを抽出し、新しいリストにする

        :param dict data:
        :return: result
        :rtype: list
        """
        result = []
        for key, value in data.items():
            if isinstance(value, dict):
                result.extend(self.generate_upload_list(value))
            elif isinstance(value, list) and Utils.item_literal_check(
                    value) and (key != self.file_attachment):
                # 配列の中身が連続データなら処理しないでスキップ
                continue
            elif isinstance(value, list):
                # ファイル添付のキーをフック
                if key == self.file_attachment:
                    result.extend(value)
                else:
                    for i in value:
                        result.extend(self.generate_upload_list(i))
            else:
                continue
        return result

    def json_rewrite(self, data: dict, files_dict: dict) -> dict:
        """
        元のjsonのファイルパスをinsert済みのファイルのoidに書き換える

        :param dict data:
        :param dict files_dict:
        :return:
        :rtype: dict
        """
        result: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result.update(
                    {key: self.json_rewrite(value, files_dict)})
            elif isinstance(value, list) and Utils.item_literal_check(
                    value) and (key != self.file_attachment):
                # キー名が添付ファイルを示すキーではなく、配列の中身がリテラルなら処理しないで書き換え
                result.update({key: value})
            elif isinstance(value, list):
                if key == self.file_attachment:
                    buff = []
                    for filepath in value:
                        if files_dict.get(filepath) is not None:
                            buff.append(files_dict[filepath])
                    result.update({self.file_ref: buff})
                else:
                    result.update({
                        key: [self.json_rewrite(i, files_dict)
                              for i in value]})
            else:
                result.update({key: value})
        return result

    @staticmethod
    def generate_file_path_dict(files_list: list, p: Path) -> dict[str, Path]:
        """
        files_listから添付ファイルの存在確認をし、添付ファイルのファイルパスを値とする辞書を作成

        :param list files_list:
        :param Path p:
        :return: result
        :rtype: dict
        """
        result = {}
        for json_file_path in files_list:
            p1 = Path(json_file_path)
            j = p / p1
            if j.exists():
                result.update({json_file_path: j})
            else:
                raise EdmanInternalError(
                    'JSON内のパスとファイル配置に違いがありましたので中止します')
        return result
