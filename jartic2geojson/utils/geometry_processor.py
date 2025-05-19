"""
ジオメトリ処理ユーティリティ - 座標からジオメトリオブジェクトを生成する機能を提供します。
"""

from typing import List, Dict, Optional, Any, Tuple, Union
import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point, LineString
from scipy.spatial import ConvexHull


class GeometryType:
    """ジオメトリタイプの定数"""
    POINT = "1"
    LINE = "2"
    POLYGON = "3"


class GeometryProcessor:
    """座標からジオメトリを生成・処理するクラス"""
    
    def __init__(self, debug_mode: bool = False):
        """
        初期化関数
        
        Args:
            debug_mode: デバッグ情報を表示するかどうか
        """
        self.debug_mode = debug_mode
    
    def process_geometry(self, 
                         coords: List[List[float]], 
                         geometry_type: Optional[str], 
                         is_oneway: bool, 
                         preserve_oneway_order: bool, 
                         method: str, 
                         unique_key: str) -> Optional[Union[Point, LineString, Polygon]]:
        """
        座標からジオメトリを生成する関数
        
        Args:
            coords: 座標のリスト
            geometry_type: 点・線・面コード (1: 点, 2: 線, 3: 面)
            is_oneway: 一方通行データかどうか
            preserve_oneway_order: 一方通行の座標順序を保持するかどうか
            method: 交差ラインの処理方法 ('convex_hull' または 'fix_intersections')
            unique_key: デバッグ用のユニークID
            
        Returns:
            Optional[Union[Point, LineString, Polygon]]: 生成されたジオメトリオブジェクト
        """
        # 座標数が不足している場合は早期リターン
        if len(coords) == 0:
            if self.debug_mode:
                print(f"  警告: 座標データがありません - {unique_key}")
            return None
        
        # ジオメトリタイプに基づいて処理を分岐
        if self._is_point_geometry(geometry_type, coords):
            return self._create_point_geometry(coords, geometry_type, unique_key)
            
        elif self._is_line_geometry(geometry_type, coords, is_oneway, preserve_oneway_order):
            return self._create_line_geometry(coords, geometry_type, is_oneway, preserve_oneway_order, unique_key)
            
        elif self._is_polygon_geometry(geometry_type, coords):
            return self._create_polygon_geometry(coords, method, unique_key)
            
        else:
            # コードがない場合は座標数から推測
            if self.debug_mode:
                print(f"  点・線・面コードがないため座標数から判断: {unique_key}")
                
            return self._infer_geometry_from_coords(coords, is_oneway, preserve_oneway_order, method, unique_key)
    
    def _is_point_geometry(self, geometry_type: Optional[str], coords: List[List[float]]) -> bool:
        """
        ポイントジオメトリかどうかを判定
        
        Args:
            geometry_type: ジオメトリタイプ
            coords: 座標リスト
            
        Returns:
            bool: ポイントジオメトリの場合はTrue
        """
        return geometry_type == GeometryType.POINT or (geometry_type != GeometryType.POLYGON and len(coords) == 1)
    
    def _is_line_geometry(self, 
                          geometry_type: Optional[str], 
                          coords: List[List[float]],
                          is_oneway: bool,
                          preserve_oneway_order: bool) -> bool:
        """
        ラインジオメトリかどうかを判定
        
        Args:
            geometry_type: ジオメトリタイプ
            coords: 座標リスト
            is_oneway: 一方通行データかどうか
            preserve_oneway_order: 一方通行の座標順序を保持するかどうか
            
        Returns:
            bool: ラインジオメトリの場合はTrue
        """
        return (geometry_type == GeometryType.LINE and len(coords) >= 2) or (is_oneway and preserve_oneway_order)
    
    def _is_polygon_geometry(self, geometry_type: Optional[str], coords: List[List[float]]) -> bool:
        """
        ポリゴンジオメトリかどうかを判定
        
        Args:
            geometry_type: ジオメトリタイプ
            coords: 座標リスト
            
        Returns:
            bool: ポリゴンジオメトリの場合はTrue
        """
        return geometry_type == GeometryType.POLYGON and len(coords) >= 3
    
    def _create_point_geometry(self, 
                               coords: List[List[float]], 
                               geometry_type: Optional[str], 
                               unique_key: str) -> Point:
        """
        ポイントジオメトリを生成
        
        Args:
            coords: 座標リスト
            geometry_type: ジオメトリタイプ
            unique_key: デバッグ用ID
            
        Returns:
            Point: 生成されたポイントジオメトリ
        """
        if self.debug_mode:
            if geometry_type == GeometryType.POINT:
                print(f"  ポイントとして処理 (コード1): {unique_key}")
            else:
                print(f"  警告: 座標が1点しかないためポイントとして処理: {unique_key}")
                if geometry_type:
                    print(f"  点・線・面コード: {geometry_type}")

        # 複数の座標がある場合は最初の座標を使用
        point_coord = coords[0]
        return Point(point_coord)
    
    def _create_line_geometry(self, 
                              coords: List[List[float]], 
                              geometry_type: Optional[str], 
                              is_oneway: bool, 
                              preserve_oneway_order: bool, 
                              unique_key: str) -> LineString:
        """
        ラインジオメトリを生成
        
        Args:
            coords: 座標リスト
            geometry_type: ジオメトリタイプ
            is_oneway: 一方通行データかどうか
            preserve_oneway_order: 一方通行の座標順序を保持するかどうか
            unique_key: デバッグ用ID
            
        Returns:
            LineString: 生成されたラインジオメトリ
        """
        if self.debug_mode:
            print(f"  ラインとして処理 (コード2): {unique_key}")

        # 一方通行の場合は座標順序の確認
        if is_oneway and preserve_oneway_order:
            if self.debug_mode:
                print(f"  一方通行ライン: 座標順序を確認")
                print(f"  LineString作成前の座標: {coords}")
                
        geometry = LineString(coords)
        
        if self.debug_mode and is_oneway and preserve_oneway_order:
            print(f"  LineString作成後の座標: {list(geometry.coords)}")
            
        return geometry
    
    def _create_polygon_geometry(self, 
                                coords: List[List[float]], 
                                method: str, 
                                unique_key: str) -> Polygon:
        """
        ポリゴンジオメトリを生成
        
        Args:
            coords: 座標リスト
            method: 交差ラインの処理方法
            unique_key: デバッグ用ID
            
        Returns:
            Polygon: 生成されたポリゴンジオメトリ
        """
        if self.debug_mode:
            print(f"  ポリゴンとして処理 (コード3): {unique_key}")

        # 問題のある行かどうか判定
        problematic_ids = ["14202503000000600000050500200001"]
        is_problematic = unique_key in problematic_ids

        if is_problematic and self.debug_mode:
            print(f"  問題のある行を検出: {unique_key}")

        # 問題のある行または点数が多い場合は選択された方法で処理
        if is_problematic or (method == 'convex_hull' and len(coords) > 10):
            if method == 'convex_hull':
                return self._apply_convex_hull(coords, unique_key)
            else:  # method == 'fix_intersections'
                return self._fix_intersections(coords, unique_key)
        else:
            # 通常のポリゴン処理
            if coords[0] != coords[-1]:
                coords.append(coords[0])
            return Polygon(coords)
    
    def _apply_convex_hull(self, coords: List[List[float]], unique_key: str) -> Polygon:
        """
        凸包を適用してポリゴンを生成
        
        Args:
            coords: 座標リスト
            unique_key: デバッグ用ID
            
        Returns:
            Polygon: 凸包を適用したポリゴン
        """
        try:
            points = np.array(coords)
            hull = ConvexHull(points)
            hull_points = [points[i] for i in hull.vertices]
            # 最初の点を最後にも追加して閉じる
            hull_points.append(hull_points[0])
            geometry = Polygon(hull_points)
            if self.debug_mode:
                print(f"  凸包を適用: {len(hull_points)-1}点")
            return geometry
        except Exception as e:
            if self.debug_mode:
                print(f"  凸包の計算に失敗: {e}")
            # エラーが発生した場合は単純に点を繋いでポリゴンを作成
            if coords[0] != coords[-1]:
                coords.append(coords[0])
            return Polygon(coords)
    
    def _fix_intersections(self, coords: List[List[float]], unique_key: str) -> Polygon:
        """
        交差を修正してポリゴンを生成
        
        Args:
            coords: 座標リスト
            unique_key: デバッグ用ID
            
        Returns:
            Polygon: 交差を修正したポリゴン
        """
        sorted_coords = self._sort_coords_by_angle(coords)
        # 最初の点が最後の点と同じになるように閉じる
        if sorted_coords[0] != sorted_coords[-1]:
            sorted_coords.append(sorted_coords[0])
        geometry = Polygon(sorted_coords)
        if self.debug_mode:
            print(f"  交差を修正")
        return geometry
    
    def _infer_geometry_from_coords(self, 
                                   coords: List[List[float]], 
                                   is_oneway: bool, 
                                   preserve_oneway_order: bool, 
                                   method: str, 
                                   unique_key: str) -> Union[Point, LineString, Polygon]:
        """
        座標の数から適切なジオメトリタイプを推測する
        
        Args:
            coords: 座標のリスト
            is_oneway: 一方通行データかどうか
            preserve_oneway_order: 一方通行の座標順序を保持するかどうか
            method: 交差ラインの処理方法
            unique_key: デバッグ用ID
            
        Returns:
            Union[Point, LineString, Polygon]: 推測されたジオメトリ
        """
        if len(coords) == 1:
            # 1点だけの場合はポイント
            if self.debug_mode:
                print(f"  ポイントとして処理 (座標数から判断): {unique_key}")
            return Point(coords[0])
        elif len(coords) == 2:
            # 2点の場合はライン
            if self.debug_mode:
                print(f"  ラインとして処理 (座標数から判断): {unique_key}")
            return LineString(coords)
        else:
            # 3点以上の場合はポリゴン、ただし一方通行は例外
            if self.debug_mode:
                print(f"  ポリゴンとして処理 (座標数から判断): {unique_key}")

            # 一方通行の場合は特別な処理
            if is_oneway and preserve_oneway_order:
                # 一方通行はラインとして処理
                if self.debug_mode:
                    print(f"  一方通行と判断: ラインとして処理")
                    print(f"  ライン作成前の座標: {coords}")
                line = LineString(coords)
                if self.debug_mode:
                    print(f"  ライン作成後の座標: {list(line.coords)}")
                return line
            # その他の場合は通常のポリゴン処理
            elif method == 'convex_hull' and len(coords) > 10:
                return self._apply_convex_hull(coords, unique_key)
            elif method == 'fix_intersections':
                return self._fix_intersections(coords, unique_key)
            else:
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                return Polygon(coords)
    
    def _sort_coords_by_angle(self, coords: List[List[float]]) -> List[List[float]]:
        """
        座標を重心からの角度でソートする
        
        Args:
            coords: 座標リスト
            
        Returns:
            List[List[float]]: ソートされた座標リスト
        """
        if len(coords) < 3:
            return coords

        # 重心を計算
        centroid_x = sum(p[0] for p in coords) / len(coords)
        centroid_y = sum(p[1] for p in coords) / len(coords)

        # 点を重心に対する角度でソート（反時計回り）
        def angle_to_centroid(point):
            return np.arctan2(point[1] - centroid_y, point[0] - centroid_x)

        sorted_coords = sorted(coords, key=angle_to_centroid)
        return sorted_coords
    
    def validate_and_fix_geometry(self, 
                                  geometry: Union[Point, LineString, Polygon], 
                                  unique_key: str) -> Union[Point, LineString, Polygon]:
        """
        ジオメトリの検証と修正を行う関数
        
        Args:
            geometry: 検証・修正対象のジオメトリ
            unique_key: デバッグ用ID
            
        Returns:
            Union[Point, LineString, Polygon]: 検証・修正後のジオメトリ
        """
        # ポリゴンが無効な場合は修正を試みる
        if isinstance(geometry, Polygon) and not geometry.is_valid:
            if self.debug_mode:
                print(f"  無効なポリゴンを修正: {unique_key}")
            geometry = geometry.buffer(0)  # バッファリングで自己交差を修正

        return geometry
    
    def create_properties(self, 
                          row: pd.Series, 
                          df: pd.DataFrame, 
                          coord_column: str, 
                          is_oneway: bool, 
                          preserve_oneway_order: bool,
                          direction_code_column: Optional[str] = None) -> Dict[str, Any]:
        """
        プロパティ辞書を作成する関数
        
        Args:
            row: データ行
            df: データフレーム
            coord_column: 座標カラム名
            is_oneway: 一方通行データかどうか
            preserve_oneway_order: 一方通行の座標順序を保持するかどうか
            direction_code_column: 方向コードカラム名
            
        Returns:
            Dict[str, Any]: 生成されたプロパティ辞書
        """
        properties = {}
        for col in df.columns:
            if col != coord_column:
                value = row[col]
                if pd.notna(value):
                    # 日付型の場合は文字列に変換
                    if isinstance(value, pd.Timestamp):
                        value = value.strftime('%Y/%m/%d')
                    properties[col] = value

        # 一方通行の場合、特別なメタデータを追加
        if is_oneway and preserve_oneway_order:
            properties.update(
                self._create_oneway_properties(row, direction_code_column)
            )

        return properties
    
    def _create_oneway_properties(self, 
                                 row: pd.Series, 
                                 direction_code_column: Optional[str]) -> Dict[str, Any]:
        """
        一方通行用のプロパティを作成
        
        Args:
            row: データ行
            direction_code_column: 方向コードカラム名
            
        Returns:
            Dict[str, Any]: 一方通行用プロパティ
        """
        properties = {
            'oneway_preserved': True,
            'is_oneway': True,
            'regulation_code': '11'  # 一方通行コード
        }

        # 方向コードを取得
        direction_code = None
        if direction_code_column and pd.notna(row[direction_code_column]):
            direction_code = row[direction_code_column]

        # 方向コードに基づいたメタデータを追加
        if direction_code is not None:
            # 数値として処理を試みる
            direction_code_num = None
            try:
                direction_code_num = float(direction_code)
            except (ValueError, TypeError):
                pass

            direction_code_str = str(direction_code).strip()

            # 禁止方向（コード1）
            if direction_code_str == "1" or direction_code_str == "1.0" or direction_code_num == 1:
                properties.update({
                    'direction_code_type': 'prohibited',
                    'direction_code_value': '1',
                    'direction_type_desc': '禁止方向（推奨）',
                    # 座標順序：進入禁止地点→一方通行開始地点（禁止方向）
                    'start_point_type': 'entry_prohibited',
                    'end_point_type': 'oneway_start',
                    'coordinate_order': 'original',
                    'coordinate_order_desc': '座標順序維持（禁止方向）',
                    'direction_desc': '進入禁止地点→一方通行開始地点（禁止方向）'
                })

            # 指定方向（コード2）
            elif direction_code_str == "2" or direction_code_str == "2.0" or direction_code_num == 2:
                properties.update({
                    'direction_code_type': 'designated',
                    'direction_code_value': '2',
                    'direction_type_desc': '指定方向（非推奨）',
                    # 元の座標順序：一方通行開始地点→進入禁止地点（通行可能方向）
                    # 座標を逆順にして「進入禁止地点→一方通行開始地点」（禁止方向）に変換
                    'start_point_type': 'entry_prohibited',
                    'end_point_type': 'oneway_start',
                    'coordinate_order': 'reversed',
                    'coordinate_order_desc': '座標を逆順に変換して禁止方向に統一',
                    'original_coordinate_desc': '元の座標は一方通行開始地点→進入禁止地点（通行可能方向）',
                    'direction_desc': '進入禁止地点→一方通行開始地点（禁止方向に変換済み）'
                })
            else:
                # 方向コード不明
                properties.update({
                    'direction_code_type': 'unknown',
                    'direction_code_value': str(direction_code) if direction_code is not None else 'none',
                    'direction_type_desc': f'方向コード不明: {direction_code}',
                    'start_point_type': 'unknown',
                    'end_point_type': 'unknown',
                    'coordinate_order': 'unknown',
                    'coordinate_order_desc': '方向コード不明のため座標順序不明'
                })

        return properties
    
    @staticmethod
    def count_geometries(geometry_list: List[Union[Point, LineString, Polygon]]) -> Dict[str, int]:
        """
        ジオメトリのタイプ別カウントを行う関数
        
        Args:
            geometry_list: ジオメトリのリスト
            
        Returns:
            Dict[str, int]: タイプ別カウント辞書
        """
        point_count = sum(1 for g in geometry_list if isinstance(g, Point))
        line_count = sum(1 for g in geometry_list if isinstance(g, LineString))
        polygon_count = sum(1 for g in geometry_list if isinstance(g, Polygon))
        total_count = len(geometry_list)

        return {
            'point_count': point_count,
            'line_count': line_count,
            'polygon_count': polygon_count,
            'total_count': total_count
        }
    
    @staticmethod
    def format_method_description(method: str, polygon_count: int) -> List[str]:
        """
        処理方法の説明文を生成する関数
        
        Args:
            method: 処理方法
            polygon_count: ポリゴン数
            
        Returns:
            List[str]: 適用された処理方法のリスト
        """
        applied_methods = []
        if method == 'convex_hull' and polygon_count > 0:
            applied_methods.append("凸包（長いポリゴンのみ）")
        elif method == 'fix_intersections' and polygon_count > 0:
            applied_methods.append("交差修正")

        return applied_methods


# 下位互換性のためのユーティリティ関数
def process_geometry(coords: List[List[float]], 
                     geometry_type: Optional[str], 
                     is_oneway: bool, 
                     preserve_oneway_order: bool, 
                     method: str, 
                     unique_key: str, 
                     debug_mode: bool = False) -> Optional[Union[Point, LineString, Polygon]]:
    """
    座標からジオメトリを生成する関数
    
    Args:
        coords: 座標のリスト
        geometry_type: 点・線・面コード
        is_oneway: 一方通行データかどうか
        preserve_oneway_order: 一方通行の座標順序を保持するかどうか
        method: 交差ラインの処理方法
        unique_key: デバッグ用のユニークID
        debug_mode: デバッグ情報を表示するかどうか
        
    Returns:
        Optional[Union[Point, LineString, Polygon]]: 生成されたジオメトリ
    """
    processor = GeometryProcessor(debug_mode)
    return processor.process_geometry(coords, geometry_type, is_oneway, preserve_oneway_order, method, unique_key)


def validate_and_fix_geometry(geometry: Union[Point, LineString, Polygon], 
                             unique_key: str, 
                             debug_mode: bool = False) -> Union[Point, LineString, Polygon]:
    """
    ジオメトリの検証と修正を行う関数
    
    Args:
        geometry: 検証・修正対象のジオメトリ
        unique_key: デバッグ用ID
        debug_mode: デバッグ情報を表示するかどうか
        
    Returns:
        Union[Point, LineString, Polygon]: 検証・修正後のジオメトリ
    """
    processor = GeometryProcessor(debug_mode)
    return processor.validate_and_fix_geometry(geometry, unique_key)


def create_properties(row: pd.Series, 
                     df: pd.DataFrame, 
                     coord_column: str, 
                     is_oneway: bool, 
                     preserve_oneway_order: bool,
                     direction_code_column: Optional[str] = None) -> Dict[str, Any]:
    """
    プロパティ辞書を作成する関数
    
    Args:
        row: データ行
        df: データフレーム
        coord_column: 座標カラム名
        is_oneway: 一方通行データかどうか
        preserve_oneway_order: 一方通行の座標順序を保持するかどうか
        direction_code_column: 方向コードカラム名
        
    Returns:
        Dict[str, Any]: 生成されたプロパティ
    """
    processor = GeometryProcessor()
    return processor.create_properties(row, df, coord_column, is_oneway, preserve_oneway_order, direction_code_column)


def count_geometries(geometry_list: List[Union[Point, LineString, Polygon]]) -> Dict[str, int]:
    """
    ジオメトリのタイプ別カウントを行う関数
    
    Args:
        geometry_list: ジオメトリのリスト
        
    Returns:
        Dict[str, int]: タイプ別カウント
    """
    return GeometryProcessor.count_geometries(geometry_list)


def format_method_description(method: str, polygon_count: int) -> List[str]:
    """
    処理方法の説明文を生成する関数
    
    Args:
        method: 処理方法
        polygon_count: ポリゴン数
        
    Returns:
        List[str]: 適用された処理方法のリスト
    """
    return GeometryProcessor.format_method_description(method, polygon_count)
