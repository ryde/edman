edman
=====

|py_version|

|  KEK IMSS SBRC/PF Experimental Data Management System.
|  jsonファイル(階層構造になった実験データ等)を親子構造を解析し、MongoDBに投入します。

Requirement
-----------
-   pymongo
-   python-dateutil
-   jmespath

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
    con = {'port':'27017', 'host':'localhost', 'user':'mongodb_user_name', 'password':'monogodb_user_password', 'database':'database_name', 'options':['authSource=auth_database_name']}
    db = DB(con)
    result = db.insert(converted_edman)

◯Read

::

    from path import Path
    from edman import DB, JsonManager, Search

    con = {'port':'27017', 'host':'localhost', 'user':'mongodb_user_name', 'password':'monogodb_user_password', 'database':'database_name', 'options':['authSource=auth_database_name']}
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

    con = {'port':'27017', 'host':'localhost', 'user':'mongodb_user_name', 'password':'monogodb_user_password', 'database':'database_name', 'options':['authSource=auth_database_name']}
    db = DB(con)
    result = db.update(collection, objectid, modified_data, structure='ref')

◯Delete

::

    from edman import DB

    con = {'port':'27017', 'host':'localhost', 'user':'mongodb_user_name', 'password':'monogodb_user_password', 'database':'database_name', 'options':['authSource=auth_database_name']}
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

|  scriptsはedman_cliを利用してください
|  https://github.com/ryde/edman_cli

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

.. |py_version| image:: https://img.shields.io/badge/python-3.10-blue.svg
    :alt: Use python
