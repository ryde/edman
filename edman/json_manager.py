import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Union
from bson.json_util import dumps
from edman.exceptions import EdmanFormatError


class JsonManager:
    """
    JSONファイルの取扱いクラス
    """

    @staticmethod
    def save(report_data: dict, path: Union[str, Path], name: str,
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

        date_str = ''
        if date:
            date_str = datetime.today().strftime("%Y%m%d%H%M%S%f") + '_'

        filename = date_str + name + '.json'
        p = path if isinstance(path, Path) else Path(path)
        savepath = p / filename

        with savepath.open("w", encoding='utf8') as file:
            file.write(dumps(report_data, ensure_ascii=False, indent=4))
            file.flush()
            os.fsync(file.fileno())
            sys.stdout.write('\r\n')  # tqdmとの干渉を防ぐための改行
            print('saved:' + str(savepath))
