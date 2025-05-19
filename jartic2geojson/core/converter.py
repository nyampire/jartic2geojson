"""
変換メイン処理モジュール - CSVデータからGeoJSONへの変換を行います。
"""

import os
import sys
import geopandas as gpd
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union
from shapely.geometry import Point, LineString, Polygon

# パッケージの各モジュールをインポート
from jartic2geojson.config import RegulationCodes
from jartic2geojson.utils.column_detector import ColumnDetector, handle_regulation_code_selection
from jartic2geojson.utils.coordinate_utils import CoordinateParser, CoordinateProcessor
from jartic2geojson.utils.file_handler import FileHandler
from jartic2geojson.utils.geometry_processor import GeometryProcessor


class GeoJSONConverter:
    """CSVからGeoJSONへの変換を行うクラス"""

    def __init__(self,
                input_file: str,
                output_dir: str,
                method: str = 'convex_hull',
                debug: bool = False,
                split_by_regulation: bool = False,
                preserve_oneway_order: bool = True):
        """
        初期化関数

        Args:
            input_file: 入力CSVファイルのパス
            output_dir: 出力ディレクトリ
            method: 交差ラインの処理方法
            debug: デバッグモード
            split_by_regulation: 規制種別ごとに分割するかどうか
            preserve_oneway_order: 一方通行の座標順序を保持するかどうか
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.method = method
        self.debug = debug
        self.split_by_regulation = split_by_regulation
        self.preserve_oneway_order = preserve_oneway_order

        # ユーティリティクラスの初期化
        self.file_handler = FileHandler(debug)
        self.geometry_processor = GeometryProcessor(debug)

        # 処理時間記録用
        self.start_time = None
        self.end_time = None

    def convert(self) -> List[str]:
        """
        CSVデータをGeoJSONに変換する

        Returns:
            List[str]: 出力されたファイルパスのリスト
        """
        # 開始時間を記録
        self.start_time = datetime.now()

        # 出力ファイルリストを初期化
        output_files = []

        try:
            # データファイルを読み込み
            df = self._load_data()

            # カラム情報を検出
            columns = self._detect_columns(df)

            # 規制コードカラムがない場合の処理
            if not columns['regulation_code_column'] and self.split_by_regulation:
                columns['regulation_code_column'], self.split_by_regulation = self._handle_regulation_code_selection(
                    df, columns['regulation_code_column']
                )

            # 規制種別コードでデータ分割
            regulation_batches = self._split_data_by_regulation(df, columns)

            # 各バッチ処理
            for regulation_code, filtered_df in regulation_batches:
                output_file = self._process_regulation_batch(
                    regulation_code, filtered_df, columns
                )

                if output_file:
                    output_files.append(output_file)

            # 処理時間を記録・表示
            self._show_completion_info()

            return output_files

        except Exception as e:
            print(f"エラー: 変換処理中に例外が発生しました: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            sys.exit(1)

    def _load_data(self) -> pd.DataFrame:
        """
        入力データを読み込む

        Returns:
            pd.DataFrame: 読み込まれたデータフレーム
        """
        try:
            print(f"入力ファイル: {self.input_file} の処理を開始します...")
            df = self.file_handler.read_csv_file(self.input_file)

            # データフレーム情報の表示
            if self.debug:
                print("\nデータフレームのカラム名:")
                for i, col in enumerate(df.columns):
                    print(f"{i}: '{col}'")

                print(f"\nデータ件数: {len(df)}行")
                print("\nデータの最初の行:")
                print(df.iloc[0])

            return df

        except Exception as e:
            print(f"エラー: ファイルの処理に失敗しました: {e}")
            sys.exit(1)

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """
        データフレームから必要なカラムを検出する

        Args:
            df: データフレーム

        Returns:
            Dict[str, Optional[str]]: 検出されたカラム名の辞書
        """
        detector = ColumnDetector(df, self.debug)
        return detector.detect_all_columns()

    def _handle_regulation_code_selection(self,
                                         df: pd.DataFrame,
                                         regulation_code_column: Optional[str]) -> Tuple[Optional[str], bool]:
        """
        規制コードカラムの選択処理

        Args:
            df: データフレーム
            regulation_code_column: 既に検出された規制コードカラム

        Returns:
            Tuple[Optional[str], bool]: (規制コードカラム, 分割フラグ)
        """
        # インタラクティブなコンソール入力ハンドラの代わりに
        # GUIでの選択を実装する場合や、自動選択の場合は
        # この部分をカスタマイズする

        def console_input_handler(candidates: List[Dict[str, Any]]) -> Optional[int]:
            """コンソールから入力を受け取るハンドラ"""
            print("\n共通規制種別コードを含むと思われるカラム候補:")

            for candidate in candidates:
                i = candidate['index']
                col = candidate['name']
                sample_values = candidate['samples']
                print(f"{i}: '{col}'")
                print(f"   例: {', '.join(sample_values)}")

            try:
                selection = int(input("\n共通規制種別コードのカラムインデックスを入力してください: "))
                if 0 <= selection < len(df.columns):
                    return selection
                else:
                    print("無効なインデックスです。分割せずに処理を続行します。")
                    return None
            except ValueError:
                print("入力が無効です。分割せずに処理を続行します。")
                return None

        # 非対話モードの場合は最初の候補を自動選択
        if not self.debug:
            detector = ColumnDetector(df, self.debug)
            candidates = detector.get_column_candidates('regulation')
            if candidates:
                return df.columns[candidates[0]['index']], True
            return None, False

        # 対話モードの場合は入力ハンドラを使用
        return handle_regulation_code_selection(
            df, regulation_code_column, self.split_by_regulation, console_input_handler
        )

    def _split_data_by_regulation(self,
                                 df: pd.DataFrame,
                                 columns: Dict[str, Optional[str]]) -> List[Tuple[Optional[str], pd.DataFrame]]:
        """
        規制種別コードでデータを分割する

        Args:
            df: データフレーム
            columns: 検出された列情報

        Returns:
            List[Tuple[Optional[str], pd.DataFrame]]: (規制コード, フィルタリングされたデータフレーム)のリスト
        """
        batches = []

        # 共通規制種別コード別にデータを分ける
        if self.split_by_regulation and columns['regulation_code_column']:
            regulation_codes = df[columns['regulation_code_column']].dropna().unique()
            print(f"共通規制種別コード: {regulation_codes}")

            for regulation_code in regulation_codes:
                filtered_df = df[df[columns['regulation_code_column']] == regulation_code]

                if len(filtered_df) == 0:
                    print(f"  スキップ: 共通規制種別コード {regulation_code} に該当するデータが見つかりませんでした")
                    continue

                batches.append((str(regulation_code), filtered_df))
        else:
            # 分割しない場合は全データを処理
            batches.append((None, df))

        return batches

    def _process_regulation_batch(self,
                                 regulation_code: Optional[str],
                                 filtered_df: pd.DataFrame,
                                 columns: Dict[str, Optional[str]]) -> Optional[str]:
        """
        規制コード別のデータバッチを処理する

        Args:
            regulation_code: 規制コード
            filtered_df: フィルタリングされたデータフレーム
            columns: 検出された列情報

        Returns:
            Optional[str]: 出力ファイルのパス（成功時）
        """
        # 出力ファイル名を生成
        if regulation_code is not None:
            output_file = os.path.join(self.output_dir, f"regulation_{regulation_code}.geojson")
            print(f"\n共通規制種別コード {regulation_code} の処理を開始")
        else:
            output_file = os.path.join(self.output_dir, "all_regulations.geojson")

        # ジオメトリとプロパティのリストを初期化
        geometry_list = []
        properties_list = []

        # データフレームの各行を処理
        for idx, row in filtered_df.iterrows():
            # 行の処理結果を取得
            geometry, properties = self._process_row(idx, row, filtered_df, columns)

            # 有効なデータのみ追加
            if geometry is not None:
                geometry_list.append(geometry)
                properties_list.append(properties)

        # データがない場合はスキップ
        if len(geometry_list) == 0:
            print(f"  スキップ: 出力するデータがありません - {output_file}")
            return None

        # GeoDataFrameを作成・保存
        gdf = self._create_and_save_geodataframe(
            geometry_list, properties_list, output_file
        )

        # 統計情報を表示
        self._show_statistics(regulation_code, geometry_list, properties_list, output_file)

        return output_file

    def _process_row(self,
                    idx: int,
                    row: pd.Series,
                    filtered_df: pd.DataFrame,
                    columns: Dict[str, Optional[str]]) -> Tuple[Optional[Union[Point, LineString, Polygon]], Dict[str, Any]]:
        """
        データフレームの1行を処理する

        Args:
            idx: 行インデックス
            row: データ行
            filtered_df: フィルタリングされたデータフレーム
            columns: 検出された列情報

        Returns:
            Tuple[Optional[Union[Point, LineString, Polygon]], Dict[str, Any]]: (ジオメトリ, プロパティ)のタプル
        """
        # ユニークキーを取得（存在する場合）
        unique_key = str(row[columns['unique_key_column']]) if columns['unique_key_column'] and pd.notna(row[columns['unique_key_column']]) else f"行{idx}"

        if self.debug:
            print(f"\n処理中 - {unique_key}:")

        # 座標データを取得
        coord_str = row[columns['coord_column']]

        # 一方通行の判定
        is_oneway = False
        if columns['regulation_code_column'] and pd.notna(row[columns['regulation_code_column']]):
            current_regulation_code = str(row[columns['regulation_code_column']]).strip()
            is_oneway = (current_regulation_code == str(RegulationCodes.ONEWAY))

        # 方向コードを取得（一方通行の場合）
        direction_code = None
        if is_oneway and columns['direction_code_column'] and pd.notna(row[columns['direction_code_column']]):
            direction_code = str(row[columns['direction_code_column']]).strip()

        # 座標を解析
        if is_oneway and self.preserve_oneway_order:
            # 一方通行データは重複座標を維持する
            coordinates = CoordinateParser.parse_coordinates(coord_str, self.debug, preserve_duplicates=True)
            if self.debug:
                print(f"  一方通行データ: 重複座標を維持します")
        else:
            # 通常の処理：重複座標を除去
            coordinates = CoordinateParser.parse_coordinates(coord_str, self.debug)

        # 座標がない場合はスキップ
        if len(coordinates) == 0:
            if self.debug:
                print(f"  スキップ: 座標データがありません - {unique_key}")
            return None, {}

        # 一方通行の場合は座標の順序を検証
        if is_oneway and self.preserve_oneway_order:
            if self.debug:
                print(f"\n===== 一方通行データの処理 - ID: {unique_key} =====")
                print(f"方向コード: {direction_code}")
                print(f"元の座標数: {len(coordinates)}")
                print(f"元の座標 (変換前): {coordinates}")

            # 座標のコピーを保存
            original_coords = coordinates.copy()

            # 座標を検証し、必要に応じて方向を調整（仕様反転版）
            coordinates = CoordinateProcessor.validate_oneway_coordinates(
                coordinates, unique_key, direction_code, self.debug
            )

            # 座標が実際に変化したかチェック
            coords_changed = (original_coords != coordinates)

            if self.debug:
                if direction_code == "1":
                    print(f"方向コード1（禁止方向）: 座標を逆順に変換")
                    print(f"変換後の座標: {coordinates}")
                    print(f"座標が変化したか: {coords_changed}")
                elif direction_code == "2":
                    print(f"方向コード2（指定方向）: 座標をそのまま使用")
                    print(f"変換後の座標: {coordinates}")
                    print(f"座標が変化したか: {coords_changed}")

        # 座標データの詳細表示を強化
        if self.debug:
            print(f"  最終的な座標数: {len(coordinates)}")
            print(f"  最終的な座標: {coordinates}")

            # 座標が2点以下の場合は警告
            if len(coordinates) <= 2 and is_oneway:
                print(f"  警告: 一方通行データの座標が2点以下です。方向情報が不十分な可能性があります。")

        # 点・線・面コードを取得
        geometry_type = None
        if columns['geometry_type_column'] and pd.notna(row[columns['geometry_type_column']]):
            geometry_type = str(row[columns['geometry_type_column']]).strip()
            if self.debug:
                print(f"  点・線・面コード: {geometry_type}")

        # ジオメトリを生成
        geometry = self.geometry_processor.process_geometry(
            coordinates,
            geometry_type,
            is_oneway,
            self.preserve_oneway_order,
            self.method,
            unique_key
        )

        # ジオメトリがNoneの場合はスキップ
        if geometry is None:
            if self.debug:
                print(f"  スキップ: 有効なジオメトリが生成できませんでした - {unique_key}")
            return None, {}

        # ジオメトリを検証・修正
        geometry = self.geometry_processor.validate_and_fix_geometry(geometry, unique_key)

        # プロパティを作成
        properties = self.geometry_processor.create_properties(
            row,
            filtered_df,
            columns['coord_column'],
            is_oneway,
            self.preserve_oneway_order,
            columns['direction_code_column']
        )

        return geometry, properties

    def _create_and_save_geodataframe(self,
                                     geometry_list: List[Union[Point, LineString, Polygon]],
                                     properties_list: List[Dict[str, Any]],
                                     output_file: str) -> gpd.GeoDataFrame:
        """
        GeoDataFrameを作成して保存する

        Args:
            geometry_list: ジオメトリのリスト
            properties_list: プロパティのリスト
            output_file: 出力ファイルパス

        Returns:
            gpd.GeoDataFrame: 作成されたGeoDataFrame
        """
        # GeoDataFrame作成前のデータ確認
        if self.debug:
            print("\n===== GeoDataFrame作成前のデータ確認 =====")
            for i, geom in enumerate(geometry_list):
                if isinstance(geom, LineString) and properties_list[i].get('is_oneway', False):
                    print(f"一方通行LineString {i}: {list(geom.coords)}")
                    print(f"方向コード: {properties_list[i].get('direction_code_value', 'なし')}")
                    print(f"座標順序: {properties_list[i].get('coordinate_order', 'なし')}")

        # GeoDataFrameを作成
        gdf = gpd.GeoDataFrame(properties_list, geometry=geometry_list, crs="EPSG:4326")

        # GeoDataFrame作成後の確認
        if self.debug:
            print("\n===== GeoDataFrame作成後のデータ確認 =====")
            for idx, row in gdf.iterrows():
                if isinstance(row.geometry, LineString) and row.get('is_oneway', False):
                    print(f"一方通行LineString {idx}: {list(row.geometry.coords)}")
                    print(f"方向コード: {row.get('direction_code_value', 'なし')}")
                    print(f"座標順序: {row.get('coordinate_order', 'なし')}")

        # GeoJSONとして保存
        gdf.to_file(output_file, driver="GeoJSON")

        # 保存したGeoJSONを読み込んで確認
        if self.debug:
            print("\n===== 保存後のGeoJSONデータ確認 =====")
            try:
                saved_gdf = gpd.read_file(output_file)
                for idx, row in saved_gdf.iterrows():
                    if isinstance(row.geometry, LineString) and row.get('is_oneway', False):
                        print(f"一方通行LineString {idx}: {list(row.geometry.coords)}")
                        print(f"方向コード: {row.get('direction_code_value', 'なし')}")
                        print(f"座標順序: {row.get('coordinate_order', 'なし')}")
            except Exception as e:
                print(f"保存後のGeoJSON読み込みに失敗: {e}")

        return gdf

    def _show_statistics(self,
                        regulation_code: Optional[str],
                        geometry_list: List[Union[Point, LineString, Polygon]],
                        properties_list: List[Dict[str, Any]],
                        output_file: str) -> None:
        """
        処理結果の統計情報を表示する

        Args:
            regulation_code: 規制コード
            geometry_list: ジオメトリのリスト
            properties_list: プロパティのリスト
            output_file: 出力ファイルパス
        """
        # ジオメトリカウントを計算
        counts = GeometryProcessor.count_geometries(geometry_list)

        # 結果を表示
        regulation_name = f"共通規制種別コード {regulation_code}" if regulation_code is not None else "全データ"
        print(f"\n{regulation_name} の GeoJSON ファイルを保存しました: {output_file}")
        print(f"総データ数: {counts['total_count']}件")

        # 処理方法の表示
        applied_methods = GeometryProcessor.format_method_description(self.method, counts['polygon_count'])
        if applied_methods:
            print(f"適用した処理: {', '.join(applied_methods)}")

        # ジオメトリ数の表示
        if counts['point_count'] > 0:
            print(f"ポイント数: {counts['point_count']}件")
        if counts['line_count'] > 0:
            print(f"ライン数: {counts['line_count']}件")
        if counts['polygon_count'] > 0:
            print(f"ポリゴン数: {counts['polygon_count']}件")

        # 一方通行のデータがあれば、その件数も表示
        if regulation_code == str(RegulationCodes.ONEWAY) or regulation_code is None:
            oneway_count = sum(1 for p in properties_list if p.get('oneway_preserved', False))
            if oneway_count > 0:
                prohibited_count = sum(1 for p in properties_list if p.get('direction_code_type') == 'prohibited')
                designated_count = sum(1 for p in properties_list if p.get('direction_code_type') == 'designated')
                unknown_count = sum(1 for p in properties_list if p.get('direction_code_type') == 'unknown')

                print(f"一方通行(座標順序保持): {oneway_count}件")
                if prohibited_count > 0:
                    print(f"  - 禁止方向(コード1): {prohibited_count}件 (座標順序維持)")
                if designated_count > 0:
                    print(f"  - 指定方向(コード2): {designated_count}件 (座標を逆順に変換)")
                if unknown_count > 0:
                    print(f"  - 方向コード不明: {unknown_count}件")

                print(f"\n【補足】一方通行の座標順序について")
                print(f"  - 方向コード=1（禁止）: 始点(進入禁止地点)→終点(一方通行の開始地点)の順序（座標維持）")
                print(f"  - 方向コード=2（指定）: 元の順序は始点(一方通行の開始地点)→終点(進入禁止地点)だが、座標を逆順に統一")
                print(f"  - 全ての一方通行データは「進入禁止地点→一方通行開始地点」の禁止方向に統一されています")

    def _show_completion_info(self) -> None:
        """処理完了情報を表示する"""
        self.end_time = datetime.now()
        processing_time = self.end_time - self.start_time

        print("\n処理が完了しました。")
        print(f"処理時間: {processing_time.total_seconds():.2f}秒")


def convert_csv_to_geojson(input_file: str,
                          output_dir: str = 'output',
                          method: str = 'convex_hull',
                          debug: bool = False,
                          split_by_regulation: bool = False,
                          preserve_oneway_order: bool = True) -> List[str]:
    """
    CSVファイルをGeoJSONに変換する関数

    Args:
        input_file: 入力CSVファイルのパス
        output_dir: 出力ディレクトリ
        method: 交差ラインの処理方法
        debug: デバッグモード
        split_by_regulation: 規制種別ごとに分割するかどうか
        preserve_oneway_order: 一方通行の座標順序を保持するかどうか

    Returns:
        List[str]: 出力されたファイルパスのリスト
    """
    converter = GeoJSONConverter(
        input_file=input_file,
        output_dir=output_dir,
        method=method,
        debug=debug,
        split_by_regulation=split_by_regulation,
        preserve_oneway_order=preserve_oneway_order
    )

    return converter.convert()
