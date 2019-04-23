EDMAN
=====

|py_version| |circleci|

|  KEK IMSS SBRC/PF Experimental Data Management System.
|  jsonファイル(階層構造になった実験データ等)をMongoDBに投入します。

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
|
|  ◯emb(Embedded)とref(reference)について
|  embはjsonファイルの構造をそのままドキュメントとしてMongoDBに投入します。
|  refはjsonの親子構造を解析し、各親をコレクションとして登録、データはドキュメントとして投入します。
|
|  ◯スクリプトで使用するクエリについて
|
|  検索用クエリ
|    検索の際はpymongoのフィルタ形式で指定します
|    クエリ形式は "{pymongoでのフィルタ条件}"
|      参照:  http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
|
|  階層指定クエリ
|    emb(Embedded)形式でデータが入っている場合は下記のようなクエリで指定します
|    構造上、embの時はクエリを使用しなければデータに到達できません
|    例:

::

       {
           "collectionA":[
               {
                   "collectionB":{"data1":"value1"}
               },
               {
                   "collectionC:{
                       "data2":"value2",
                       "CollectionD":{
                           "data3":"value3",
                           "data4":"value4"
                       }
                   }
               }
           ]
       }

|   ・data4を消したい場合
|   "['collectionA', '1', 'collectionC', 'collectionD']"
|   リストで消したい項目の直近の親までを指定する
|   データが複数あり、リストで囲まれていた場合は添字を数字で指定
|
|  ◯各スクリプトファイル
|  entry_script.py: jsonファイルからMongoDBに投入
|  find_script.py: データを検索し、jsonに保存 クエリ1を使用します
|  delete.py: データ内の項目を消す embの時クエリ2を使用します
|  update.py: データの更新(更新用jsonファイルを用意)
|  file_add_script.py:  該当データにファイルを添付する embの時クエリ2を使用します
|  file_dl_script.py: 添付ファイルをダウンロード embの時クエリ2を使用します
|  file_delete_script.py: 添付ファイルを削除 embの時クエリ2を使用します
|  db_create.py: データベース及びユーザ作成操作支援用(MongoDBの管理者アカウントが必要)
|  db_destroy.py: データベース削除操作支援用(ユーザ削除はソース書き換えが必要)
|
オプションなど詳しくは::

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

.. |py_version| image:: https://img.shields.io/badge/python-3.6-blue.svg
    :alt: Use python

.. |circleci| image:: https://circleci.com/gh/ryde/edman_test/tree/develop.svg?style=svg&circle-token=f669e73a212627c6f4e57e18fa7002c3454d07fd
    :alt: Build status on circleCi
