#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
交通規制情報CSVファイルをGeoJSONに変換するコマンドラインスクリプト
直接実行可能なメインエントリーポイント
ジオメトリ修正機能の統合付き
"""

import sys
import os
import argparse
from datetime import datetime
import traceback
from pathlib import Path

def main():
    """メイン処理を実行するための関数"""
    # コマンドライン引数を解析
    parser = argparse.ArgumentParser(
        description='交通規制情報CSVファイルから交通規制情報を含むGeoJSONに変換します。'
    )

    parser.add_argument(
        'input_file',
        help='入力CSVファイルのパス'
    )

    parser.add_argument(
        '--output_dir', '-o',
        default='output',
        help='出力ディレクトリ（デフォルト: output）'
    )

    parser.add_argument(
        '--method', '-m',
        choices=['convex_hull', 'fix_intersections'],
        default='convex_hull',
        help='交差ラインの処理方法: convex_hull (凸包を使用) または fix_intersections (交差を修正)'
    )

    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='デバッグモード: 詳細な処理情報を表示します'
    )

    parser.add_argument(
        '--split_by_regulation', '-s',
        action='store_true',
        help='共通規制種別コード別にファイルを分割します'
    )

    parser.add_argument(
        '--preserve_oneway_order', '-p',
        action='store_true',
        default=True,
        help='一方通行（コード11）の座標順序を厳密に保持します（デフォルト: True）'
    )

    # ジオメトリ修正オプションを追加
    parser.add_argument(
        '--fix-geometry', '-f',
        action='store_true',
        help='変換後にGEOSを使用してジオメトリを修正します'
    )

    # ジオメトリ修正関連のオプション
    parser.add_argument(
        '--fix-threads',
        type=int,
        default=1,
        help='ジオメトリ修正の並列スレッド数 (デフォルト: 1, 0=CPUコア数)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='ジオメトリ修正のチャンクサイズ (デフォルト: 1000)'
    )

    parser.add_argument(
        '--memory-limit',
        type=float,
        default=80.0,
        help='ジオメトリ修正のメモリ使用率上限 (%, デフォルト: 80.0)'
    )

    args = parser.parse_args()

    # 出力ディレクトリが存在しない場合は作成
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # 必要なモジュールをローカルでインポート
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, script_dir)

    # 必要な関数を直接インポート
    try:
        from jartic2geojson.config import RegulationCodes
        from jartic2geojson.utils.column_detector import ColumnDetector, handle_regulation_code_selection
        from jartic2geojson.utils.coordinate_utils import CoordinateParser, CoordinateProcessor
        from jartic2geojson.utils.file_handler import FileHandler
        from jartic2geojson.utils.geometry_processor import GeometryProcessor

        # コンバーター本体をインポート
        from jartic2geojson.core.converter import convert_csv_to_geojson

        print(f"入力ファイル: {args.input_file}")
        print(f"出力ディレクトリ: {args.output_dir}")

        # 変換実行
        output_files = convert_csv_to_geojson(
            input_file=args.input_file,
            output_dir=args.output_dir,
            method=args.method,
            debug=args.debug,
            split_by_regulation=args.split_by_regulation,
            preserve_oneway_order=args.preserve_oneway_order
        )

        print(f"\n合計 {len(output_files)} 個のGeoJSONファイルが生成されました。")

        # ジオメトリ修正が要求された場合
        if args.fix_geometry and output_files:
            print("\nジオメトリの修正を実行します...")

            # 修正済みGeoJSONの出力ディレクトリ
            geos_output_dir = os.path.join(args.output_dir, 'fixed')
            geos_log_dir = os.path.join(args.output_dir, 'logs')

            # 必要なディレクトリを作成
            os.makedirs(geos_output_dir, exist_ok=True)
            os.makedirs(geos_log_dir, exist_ok=True)

            try:
                # ジオメトリ修正モジュールをインポート
                try:
                    from jartic2geojson.postprocess.geometry_fixer import process_geojson, fix_geojson_files
                except ImportError:
                    print(f"エラー: ジオメトリ修正モジュールが見つかりません。")
                    print(f"jartic2geojson/postprocess/geometry_fixer.py が存在するか確認してください。")
                    print(f"または repair_geometries.py を使って個別に処理することもできます。")
                    print(f"スクリプトの場所: {__file__}")
                    print(f"現在のパス: {sys.path}")

                    if args.debug:
                        print("\nインポートエラーの詳細:")
                        traceback.print_exc()

                    sys.exit(1)

                print(f"修正対象のGeoJSONファイル: {len(output_files)}個")

                # 出力ファイル一覧を表示
                if args.debug:
                    print("\n対象ファイル:")
                    for f in output_files:
                        print(f"  {f}")

                # GeoJSONファイルを処理
                results = fix_geojson_files(
                    input_dir=args.output_dir,
                    output_dir=geos_output_dir,
                    log_dir=geos_log_dir,
                    recursive=False,  # 直接ファイルリストを使うため再帰的検索は不要
                    chunk_size=args.chunk_size,
                    memory_limit=args.memory_limit,
                    verbose=args.debug
                )

                if not results:
                    print(f"ジオメトリ修正: 処理できるファイルがありませんでした。")
                else:
                    # 処理結果の集計
                    total_features = sum(r["total_features"] for r in results)
                    total_invalid = sum(r["invalid_features"] for r in results)
                    total_fixed = sum(r["fixed_features"] for r in results)

                    print(f"\nジオメトリ修正が完了しました。")
                    print(f"  修正済みファイル: {geos_output_dir}")
                    print(f"  ログファイル: {geos_log_dir}")
                    print(f"  処理したフィーチャ数: {total_features}")
                    print(f"  無効なジオメトリ: {total_invalid}")
                    print(f"  修正されたジオメトリ: {total_fixed}")

                    if total_invalid > 0:
                        print(f"  修正成功率: {total_fixed / total_invalid * 100:.2f}%")

            except Exception as e:
                print(f"ジオメトリ修正中にエラーが発生しました: {e}")
                if args.debug:
                    traceback.print_exc()
                sys.exit(1)

    except ImportError as e:
        print(f"エラー: モジュールのインポートに失敗しました: {e}")
        print("これはjartic2geojsonパッケージのインポートパスに問題があることを示しています。")
        print(f"スクリプトの場所: {__file__}")
        print(f"現在のパス: {sys.path}")
        print("以下の構造になっていることを確認してください:")
        print("\njartic2geojson/")
        print("├── __init__.py")
        print("├── cli.py")
        print("├── config.py")
        print("├── core/")
        print("│   ├── __init__.py")
        print("│   └── converter.py")
        print("├── postprocess/")
        print("│   ├── __init__.py")
        print("│   └── geometry_fixer.py")
        print("└── utils/")
        print("    ├── __init__.py")
        print("    ├── column_detector.py")
        print("    ├── coordinate_utils.py")
        print("    ├── file_handler.py")
        print("    └── geometry_processor.py")
        print("\nconvert_jartic.py")

        sys.exit(1)
    except Exception as e:
        print(f"エラー: {e}")
        if args.debug:
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
