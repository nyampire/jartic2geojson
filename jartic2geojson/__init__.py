"""
jartic2geojson - 交通規制情報CSVファイルから地理情報を含むGeoJSONへの変換ツール

このパッケージは交通規制情報を含むCSVファイルをGeoJSONフォーマットに変換するための
ツールセットを提供します。特に一方通行などの交通規制の方向性情報を正確に保持します。
"""

__version__ = "1.0.0"
__author__ = "Your Name"

# 主要コンポーネントをインポート
from jartic2geojson.core.converter import convert_csv_to_geojson
from jartic2geojson.config import DEFAULT_CONFIG
