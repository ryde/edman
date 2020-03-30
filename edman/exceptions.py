class EdmanError(Exception):
    def __init__(self, message):
        self._message = message

    def __str__(self):
        return f'{self.__class__.__name__} {self._message}'


class EdmanDbConnectError(EdmanError):
    """
    DB接続に関するエラー
    """
    pass


class EdmanDbProcessError(EdmanError):
    """
    DB実行処理に関するエラー
    """
    pass


class EdmanInternalError(EdmanError):
    """
    Edmanの処理に関するエラー
    """
    pass


class EdmanFormatError(EdmanError):
    """
    Edmanの名称や決まりごとに関するエラー
    """
    pass
