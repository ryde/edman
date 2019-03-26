import sys
import json
import ast
from pathlib import Path
from typing import Tuple, Iterator, Union


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
    def files_read(file_or_dir_path: Union[str, Path], suffix: str) -> tuple:
        """
        ファイルのパスを、
        単一ファイルもディレクトリ内の複数ファイルもタプルにして返す

        :param str or Path file_or_dir_path:
        :param str suffix:
        :return tuple files:
        """
        if isinstance(file_or_dir_path, str):
            p = Path(file_or_dir_path)
        elif isinstance(file_or_dir_path, Path):
            p = file_or_dir_path
        else:
            sys.exit('file_read() is a str or path Object is required.')

        if p.exists():
            if p.is_dir():
                files = tuple(sorted(p.glob(f'*.{suffix}')))
            else:
                files = (p,) if f'.{suffix}' in p.suffix else tuple()
            if len(files) != 0:
                return files
            else:
                sys.exit(f'{suffix} files not in directory.')
        else:
            sys.exit('That path does not exist.')

    @staticmethod
    def add_files_read(file_or_dir_path: Union[str, Path]) -> tuple:
        """
        ファイルのパスを、
        単一ファイルもディレクトリ内の複数ファイルもタプルにして返す
        拡張子の指定なし
        多種多様なファイルをアップロードする場合などに使用

        :param str or Path file_or_dir_path:
        :return tuple files:
        """
        if isinstance(file_or_dir_path, str):
            p = Path(file_or_dir_path)
        elif isinstance(file_or_dir_path, Path):
            p = file_or_dir_path
        else:
            sys.exit('file_read() is a str or path Object is required.')

        if p.exists():
            if p.is_dir():
                files = tuple(sorted(p.glob('*')))
            else:
                files = (p,)
            if len(files) != 0:
                return files
            else:
                sys.exit('files not in directory.')
        else:
            sys.exit('That path does not exist.')

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
