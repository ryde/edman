import os
from datetime import datetime
from enum import Enum, auto
from pathlib import Path

from bson.json_util import dumps

from edman.exceptions import EdmanFormatError


class JsonManager:
    """
    JSONファイルの取扱いクラス
    """

    @staticmethod
    def save(report_data: dict, path: str | Path, name: str,
             date=False) -> None:
        """
        JSONファイルに書き出し

        :param dict report_data: 対象の辞書データ
        :param path: ファイルパス
        :type path: str or Path
        :param str name: ファイル名
        :param bool date: 日付 ファイル名先頭に追加
        :return: None
        """
        if not isinstance(report_data, dict):
            raise EdmanFormatError('Not Dict Data')

        date_str = datetime.today().strftime(
            "%Y%m%d%H%M%S%f") + "_" if date else ""
        filename = date_str + name + '.json'
        p = path if isinstance(path, Path) else Path(path)
        savepath = p / filename

        with savepath.open("w", encoding='utf8') as file:
            file.write(dumps(report_data, ensure_ascii=False, indent=4))
            file.flush()
            os.fsync(file.fileno())


class GetJsonStructure(Enum):
    manual_select = auto()
    all_doc = auto()
    uni_doc = auto()

    @staticmethod
    def members():
        return [*GetJsonStructure.__members__.values()]
