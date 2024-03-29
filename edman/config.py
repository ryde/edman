class Config:
    """
    このパッケージを利用したシステム上での共通の定義

    **デフォルトのままをおすすめします**

      | 定義を変更した場合、このパッケージを利用している他のシステムと、データ交換ができなくなります
      | DBにデータが入っている状態で、この定義を変更した場合、データが破壊される可能性があります

    | それでも変更したい場合は、単一、もしくは同じシステム内で定義を統一すること
    | その場合、他のシステムとデータ交換したくなった場合は独自に変換スクリプトを作成してください
    """
    # ドキュメント内でedmanが使用するリファレンス用のキー
    parent = '_ed_parent'  # 親のリファレンス情報
    child = '_ed_child'  # 子のリファレンス情報
    file = '_ed_file'  # Grid.fsのリファレンス情報

    # Grid.fsのデフォルトコレクション名
    fs_files = 'fs.files'  # ファイルコレクション名
    fs_chunks = 'fs.chunks'  # ファイルチャンクコレクション名

    # ユーザがJSON内で使用するキー
    # 日付に変換する場合
    # 例: "startDate": {"#date": "2020-07-01 00:00:00"}
    date = '#date'
    # JSON内で使用する添付ファイルディレクトリ用
    # 例: "_ed_attachment":["dir1/sample_photo.jpg", "dir1/experiment.cbf"]
    file_attachment = '_ed_attachment'
