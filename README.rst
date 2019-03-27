EDMAN
=====


Description
-----------

KEK IMSS SBRC/PF Experimental Data Management System.


Requirement
-----------
-   pymongo
-   python-dateutil
-   jmespath
-   tqdm

and MongoDB.

Usage
-----

|  scriptsディレクトリにモジュール動作用スクリプトがあります。
|  scripts/ini/db.ini.sampleをdb.iniに変更後、中身を設定してください。

|  entry_script.py: jsonファイルからMongoDBに投入
|  find_script.py: データを検索し、jsonに保存
|  delete.py: データ内の項目を消す
|  update.py: データの更新(更新用jsonファイルを用意)
|  file_add_script.py:  該当データにファイルを添付する
|  file_dl_script.py: 添付ファイルをダウンロード
|  file_delete_script.py: 添付ファイルを削除
|  db_create.py: データベース作成操作支援用
|  db_destroy.py: データベース削除操作支援用


詳しくは::

  scriptname.py -h

Install
-------
|  Please install MongoDB in advance.

pip install::

 pip install EDMAN

Licence
-------
todo


Author
------

[yuskyamada](https://github.com/yuskyamada)

[ryde](https://github.com/ryde)

✨🍰✨
