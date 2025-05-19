"""
設定管理モジュール - コマンドライン引数やデフォルト設定を管理します。
"""

import os
import argparse
from typing import Dict, Any, NamedTuple

# 一方通行などの規制コードの定数
class RegulationCodes:
    """交通規制の種別コード定数"""
    ONEWAY = 11  # 一方通行の共通規制種別コード

# デフォルト設定
DEFAULT_CONFIG = {
    "output_dir": "output",
    "method": "convex_hull",
    "debug": False,
    "split_by_regulation": False,
    "preserve_oneway_order": True,
}

class CommandLineArgs(NamedTuple):
    """コマンドライン引数を格納する型付きタプル"""
    input_file: str
    output_dir: str
    method: str
    debug: bool
    split_by_regulation: bool
    preserve_oneway_order: bool


def parse_args() -> CommandLineArgs:
    """
    コマンドライン引数を解析する関数

    Returns:
        CommandLineArgs: 解析されたコマンドライン引数
    """
    parser = argparse.ArgumentParser(
        description='交通規制情報CSVファイルから交通規制情報を含むGeoJSONに変換します。'
    )

    parser.add_argument(
        'input_file',
        help='入力CSVファイルのパス'
    )

    parser.add_argument(
        '--output_dir', '-o',
        default=DEFAULT_CONFIG['output_dir'],
        help=f'出力ディレクトリ（デフォルト: {DEFAULT_CONFIG["output_dir"]}）'
    )

    parser.add_argument(
        '--method', '-m',
        choices=['convex_hull', 'fix_intersections'],
        default=DEFAULT_CONFIG['method'],
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
        default=DEFAULT_CONFIG['preserve_oneway_order'],
        help='一方通行（コード11）の座標順序を厳密に保持します（デフォルト: True）'
    )

    args = parser.parse_args()

    # 出力ディレクトリが存在しない場合は作成
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    return CommandLineArgs(
        input_file=args.input_file,
        output_dir=args.output_dir,
        method=args.method,
        debug=args.debug,
        split_by_regulation=args.split_by_regulation,
        preserve_oneway_order=args.preserve_oneway_order
    )


def get_config() -> Dict[str, Any]:
    """
    設定値を環境変数とデフォルト値から取得する関数
    環境変数がある場合はそれを優先、ない場合はデフォルト値を使用

    Returns:
        Dict[str, Any]: 設定値の辞書
    """
    config = DEFAULT_CONFIG.copy()

    # 環境変数から設定を読み込む
    env_prefix = "JARTIC2GEOJSON_"
    for key in DEFAULT_CONFIG.keys():
        env_key = f"{env_prefix}{key.upper()}"
        if env_key in os.environ:
            env_value = os.environ[env_key]

            # 値の型に応じた変換
            if isinstance(DEFAULT_CONFIG[key], bool):
                config[key] = env_value.lower() in ('true', 'yes', '1', 'y')
            elif isinstance(DEFAULT_CONFIG[key], int):
                config[key] = int(env_value)
            else:
                config[key] = env_value

    return config
