import sys
from collections import defaultdict
from typing import Union, Callable
from datetime import datetime
import dateutil.parser
from bson import ObjectId, errors


class Utils:
    """
    各クラス共通の静的メソッド
    インスタンス化禁止
    """

    def __init__(self):
        raise NotImplementedError('not allowed')

    @staticmethod
    def item_literal_check(list_child: Union[dict, list]) -> bool:
        """
        リストデータ内にリテラルやオブジェクトのデータだけあればTrue
        それ以外はFalse

        OKパターン
        list_child = [1,2,3]
        list_child = [1,2,objectId()]

        NGパターン
        list_child = {'A':'B'}
        list_child = ['A':[1,2,3]]
        list_child = [1,2,{'A':'B'}]

        :param dict or list list_child:
        :return bool:
        """
        result = True
        if isinstance(list_child, dict):
            result = False
        if isinstance(list_child, list):
            for j in list_child:
                if isinstance(j, dict) or isinstance(j, list):
                    result = False
                    break
        return result

    @staticmethod
    def doc_traverse(doc: dict, target_keys: list, query: list,
                     f: Callable) -> dict:
        """
        ドキュメントを走査し、クエリで指定した階層に指定の関数オブジェクトを適応
        関数適応後のドキュメントを返す

        :param dict doc:
        :param list target_keys: コールバック関数の適応対象の辞書のキーのリスト
        :param list query:
        :param Callable f: コールバック関数
        :return dict doc:
        """
        query.reverse()  # リストの削除処理速度向上のため、逆リストにする

        def rec(doc):
            """
            再帰中にクエリを一つづつ消費し、最後のクエリに到達したら更新
            """
            for key, value in doc.items():

                # クエリを全て消費しているなら終了
                if len(query) == 0:
                    break

                if isinstance(value, dict):

                    if key == query[-1]:
                        del query[-1]
                        if len(query) == 0:  # 最終クエリに到達しているなら更新
                            f(value, target_keys)
                        else:
                            rec(value)
                    else:
                        rec(value)

                # リストデータは項目と同じ扱いなので繰り返す
                elif isinstance(value, list) and Utils.item_literal_check(
                        value):
                    continue

                elif isinstance(value, list):

                    # 現在のクエリが数値(リストのインデックス)なら再帰に入る
                    if query[-1].isdecimal():
                        del query[-1]
                        for i in value:
                            rec(i)

                    # 現在のクエリとループのキーが違う場合は繰り返す
                    elif query[-1] != key:
                        continue

                    # 現在のキーがクエリと一致している場合
                    # 次のクエリが数値(リストのインデックスの時)、
                    # インデックスを直接指定して再帰に入る
                    elif len(query) >= 2 and query[-2].isdecimal():
                        needle = int(query[-2])
                        del query[-2:]
                        if len(query) == 0:
                            f(value[needle], target_keys)
                        else:
                            rec(value[needle])
                    else:
                        # リスト内の辞書を先に入る時、
                        # 現在のクエリがインデックスでないのはおかしい
                        # したがって例外処理の対象になる
                        raise ValueError('クエリが正しくありません.リストの場合、インデックスを指定してください')

                else:  # 項目の部分は関係ないので繰り返す
                    continue

        try:
            rec(doc)
        except ValueError as e:
            raise e
        except IndexError as e:
            raise e
        if len(query) != 0:
            raise ValueError(f'クエリが実行されませんでした. 実行されなかったクエリ:{query}')

        return doc

    @staticmethod
    def conv_objectid(oid: Union[str, ObjectId]) -> ObjectId:
        """
        文字列だった場合ObjectIdを変換する
        元々ObjectIdならそのまま

        :param ObjectId or str oid:
        :return ObjectId result:
        """

        if isinstance(oid, str):
            try:
                result = ObjectId(oid)
            except errors.InvalidId:
                sys.exit('無効なObjectIdです.')
        else:
            result = oid
        return result

    @staticmethod
    def to_datetime(s: str) -> Union[datetime, str]:
        """
        日付もしくは日付時間をdatetimeオブジェクトに変換
        日付や日付時間にならないものは文字列に変換

        :param str s:
        :return: datetime object or str(s)
        """
        if not isinstance(s, str):
            return str(s)
        try:
            return dateutil.parser.parse(s)
        except ValueError:
            return str(s)

    @staticmethod
    def query_check(query: list, doc: dict) -> bool:
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

    @staticmethod
    def reference_item_delete(doc: dict, del_keys: tuple) -> dict:
        """
        _id、親と子のリファレンス、ファイルリファレンスなどを削除

        :param dict doc:
        :param tuple del_keys:
        :return: dict item
        """
        for key in del_keys:
            if key in doc:
                del doc[key]
        return doc

    @staticmethod
    def child_combine(rec_result: list) -> dict:
        """
        | 同じコレクションのデータをリストでまとめるジェネレータ
        |
        | コレクション:ドキュメントのリストを作成
        | {collection:[{key:value},{key:value}...]}

        :param list rec_result:
        :return: dict
        """
        for bros in rec_result:
            tmp_bros = defaultdict(list)

            for docs in bros:
                for collection, doc in docs.items():
                    tmp_bros[collection].append(doc)
            yield dict(tmp_bros)
