EDMAN
=====

|py_version| |circleci|

|  KEK IMSS SBRC/PF Experimental Data Management System.
|  jsonファイル(階層構造になった実験データ等)を親子構造を解析し、MongoDBに投入します。

Requirement
-----------
-   pymongo
-   python-dateutil
-   jmespath
-   tqdm

and MongoDB.

Modules Usage
-------------

◯Create

::

    import json
    from edman import DB, Convert

    # Load json into a dictionary
    json_dict = json.load(json_file)

    # json to json for edman
    convert = Convert()
    converted_edman = convert.dict_to_edman(json_dict)

    # insert
    con = {'port':'27017', 'host':'localhost', 'database':'database_name', 'auth_database':'auth_database_name', 'user':'mongodb_user_name', 'password':'monogodb_user_password'}
    db = DB(con)
    result = db.insert(converted_edman)

◯Read

::

    from path import Path
    from edman import DB, JsonManager, Search

    con = {'port':'27017', 'host':'localhost', 'database':'database_name', 'auth_database':'auth_database_name', 'user':'mongodb_user_name', 'password':'monogodb_user_password'}
    db = DB(con)
    search = Search(db)
    collection = 'target_collection'

    # Same syntax as pymongo's find query
    query = {'_id':'OBJECTID'}

    # example, 2 top levels of parents and 3 lower levels of children (ref mode)
    search_result = search.find(collection, query, parent_depth=2, child_depth=3)

    # Save search results
    dir = Path('path_to')
    jm = JsonManager()
    jm.save(search_result, dir, name='filename', date=True)

◯Update

::

    import json
    from edman import DB

    modified_data = json.load(modified_json_file)

    # emb example
    # Same key will be modified, new key will be added
    # modified_data = {'key': 'modified value', 'child': {'key': 'value'}}

    # ref example
    # Same key will be modified, new key will be added
    # modified_data = {'key': 'modified value', 'new_key': 'value'}

    con = {'port':'27017', 'host':'localhost', 'database':'database_name', 'auth_database':'auth_database_name', 'user':'mongodb_user_name', 'password':'monogodb_user_password'}
    db = DB(con)
    result = db.update(collection, objectid, modified_data, structure='ref')

◯Delete

::

    from edman import DB

    con = {'port':'27017', 'host':'localhost', 'database':'database_name', 'auth_database':'auth_database_name', 'user':'mongodb_user_name', 'password':'monogodb_user_password'}
    db = DB(con)
    result = db.delete(objectid, collection, structure='ref')

Json Format
-----------
| example

::

    {
        "Beamtime":
        [
            {
                "date": {"#date": "2019-09-17"},
                "expInfo":[
                        {
                            "time": {"#date": "2019/09/17 13:21:45"},
                            "int_value": 135,
                            "float_value":24.98
                        },
                        {
                            "time": {"#date": "2019/09/17 13:29:12"},
                            "string_value": "hello world"
                        }
                ]
            },
            {
                "date": {"#date": "2019-09-18"},
                "expInfo":[
                        {
                            "array_value": ["string", 1234, 56.78, true, null],
                            "Bool": false,
                            "Null type": null
                        }
                ]
            }
        ]
    }

| #date{}で囲むと日付書式がdatetime型に変換されます。書式はdateutilと同等。
|     https://dateutil.readthedocs.io/en/stable/parser.html#module-dateutil.parser
| 使用できる型はjsonに準拠。整数、浮動小数点数、ブール値、null型、配列も使用可。
| jsonのオブジェクト型はEdmanでは階層構造として認識されます。
|
| 予約コレクション名
|   ・他ドキュメントのリファレンスと同じ名前(_ed_parent,_ed_child,_ed_file) ※システム構築時にのみ変更可
| 予約フィールド名
|   ・日付表現の変換に使用(#date) ※システム構築時にのみ変更可
|   ・ObjectIdと同じフィールド名(_id)
| その他MongoDBで禁止されているフィールド名は使用不可
|      https://docs.mongodb.com/manual/reference/limits/#naming-restrictions
|
| MongoDBの1つのドキュメントの容量上限は16MBですが、
|     emb形式の場合はObjectId及びファイル追加ごとのリファレンスデータを含むため、16MBより少なくなります。
|     ref形式の場合は1階層につきObjectId、及びroot(一番上の親)以外は親への参照もデフォルトで含め、子要素やファイルが多いほど参照が増えるため16MBより少なくなります。
|
|  ◯emb(Embedded)とref(reference)について
|  embはjsonファイルの構造をそのままドキュメントとしてMongoDBに投入します。
|   ・親子構造を含め全て一つのコレクションに保存します。
|  refはjsonの親子構造を解析し、オブジェクト単位をコレクションとし、親子それぞれをドキュメントとして保存します。
|   ・親子関係はリファレンスによって繋がっているので指定のツリーを呼び出すことができます。

Scripts Usage
-------------

|  scriptsはpip経由ではインストールされませんので、github経由でダウンロードしてください。
|  scriptsディレクトリにモジュール動作用スクリプトがあります。
|  scripts/ini/db.ini.sampleをdb.iniに変更後、中身を設定してください。
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
|       "['collectionA', '1', 'collectionC', 'collectionD']"
|   リストで消したい項目の直近の親までを指定する
|   データが複数あり、リストで囲まれていた場合は添字を数字で指定
|
|  ◯各スクリプトファイル
|  entry.py: jsonファイルからMongoDBに投入
|  find.py: データを検索し、jsonに保存 クエリ1を使用します
|  item_delete.py: データ内の項目を消す embの時クエリ2を使用します
|  update.py: データの更新(更新用jsonファイルを用意)
|  delete.py: ドキュメントの削除(embは全削除、refは指定したobjectid以下を削除)
|  file_add.py:  該当データにファイルを添付する embの時クエリ2を使用します
|  file_dl.py: 添付ファイルをダウンロード embの時クエリ2を使用します
|  file_delete.py: 添付ファイルを削除 embの時クエリ2を使用します
|  db_create.py: データベース及びユーザ作成操作支援用(MongoDBの管理者アカウントが必要)
|  db_destroy.py: データベース削除操作支援用(ユーザ削除はソース書き換えが必要)
|  structure_convert.py: DB内のembをrefへ変換、またはその逆を行います
|  pullout.py: コレクション内のembのキーを指定し、そのキーを含む階層を全てrefに変換します
|  action.py: 上記の操作スクリプト用のモジュール

オプションなど詳しくは::

  scriptname.py -h

Install
-------
|  Please install MongoDB in advance.

pip install::

 pip install edman

Licence
-------
MIT

API Document
------------
https://yuskyamada.github.io/EDMAN/

Author
------

[yuskyamada](https://github.com/yuskyamada)

[ryde](https://github.com/ryde)

.. |py_version| image:: https://img.shields.io/badge/python-3.6-blue.svg
    :alt: Use python

.. |circleci| image:: https://circleci.com/gh/yuskyamada/EDMAN/tree/develop.svg?style=svg
    :alt: Build status on circleCi
    :target: https://circleci.com/gh/yuskyamada/EDMAN/tree/develop