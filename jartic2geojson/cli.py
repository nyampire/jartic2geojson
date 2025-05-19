#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CLI - 交通規制情報CSVファイルをGeoJSONに変換するコマンドラインインターフェース
"""

import sys
from jartic2geojson.config import parse_args
from jartic2geojson.core.converter import convert_csv_to_geojson


def main():
    """コマンドラインからの実行のエントリーポイント"""

    # コマンドライン引数を解析
    args = parse_args()

    # 変換処理実行
    try:
        output_files = convert_csv_to_geojson(
            input_file=args.input_file,
            output_dir=args.output_dir,
            method=args.method,
            debug=args.debug,
            split_by_regulation=args.split_by_regulation,
            preserve_oneway_order=args.preserve_oneway_order
        )

        print(f"\n合計 {len(output_files)} 個のGeoJSONファイルが生成されました。")

    except Exception as e:
        print(f"エラー: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
