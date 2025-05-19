"""
座標処理ユーティリティ - 緯度経度の解析と変換に関する機能を提供します。
"""

import re
import numpy as np
import pandas as pd
from typing import List, Tuple, Optional, Dict, Any


class CoordinateParser:
    """座標文字列を解析するクラス"""
    
    @staticmethod
    def parse_coordinates(coord_str: Any, 
                          debug_mode: bool = False, 
                          preserve_duplicates: bool = False) -> List[List[float]]:
        """
        緯度経度の文字列からリストを作成する関数
        セミコロンで区切られた座標ペアを処理する
        より厳格な解析と複数のフォーマットに対応
        
        Args:
            coord_str: 座標文字列
            debug_mode: デバッグ情報を表示するかどうか
            preserve_duplicates: 重複座標を維持するかどうか（一方通行データ用）
            
        Returns:
            List[List[float]]: 解析された座標のリスト [[lon1, lat1], [lon2, lat2], ...]
        """
        coords = []

        # 緯度経度のペアがない場合は空リストを返す
        if pd.isna(coord_str) or coord_str == "":
            return coords

        # 文字列型に変換
        coord_str = str(coord_str)

        # 座標文字列をセミコロンで分割
        coord_pairs = coord_str.split(';')
        
        # 各座標ペアを処理
        for pair in coord_pairs:
            if pair.strip() == "":
                continue
                
            # 複数のパターンで解析を試みる
            result = CoordinateParser._try_parse_coordinate_pair(pair, debug_mode)
            if result:
                coords.append(result)

        # 重複座標の処理
        if coords:
            return CoordinateParser._handle_duplicate_coordinates(
                coords, preserve_duplicates, debug_mode
            )
        
        return coords
    
    @staticmethod
    def _try_parse_coordinate_pair(pair: str, debug_mode: bool) -> Optional[List[float]]:
        """
        座標ペアを異なるパターンで解析する内部ヘルパー関数
        
        Args:
            pair: 座標ペア文字列
            debug_mode: デバッグ情報を表示するかどうか
            
        Returns:
            Optional[List[float]]: 解析された[経度, 緯度]、解析失敗時はNone
        """
        # パターン1: 浮動小数点数（139.6471 35.4436）
        float_pattern = r'(\d+\.\d+)'
        float_matches = re.findall(float_pattern, pair)

        if len(float_matches) >= 2:
            try:
                lon = float(float_matches[0])  # 経度
                lat = float(float_matches[1])  # 緯度
                return [lon, lat]
            except (ValueError, IndexError) as e:
                if debug_mode:
                    print(f"  警告: 浮動小数点パターン解析エラー: {pair} - {e}")

        # パターン2: スペースで区切られた数値
        try:
            parts = pair.strip().split()
            numeric_parts = []

            for part in parts:
                try:
                    # 数値変換を試みる
                    numeric_parts.append(float(part))
                except ValueError:
                    pass

            if len(numeric_parts) >= 2:
                lon = numeric_parts[0]  # 経度
                lat = numeric_parts[1]  # 緯度
                return [lon, lat]
        except Exception as e:
            if debug_mode:
                print(f"  警告: スペース区切りパターン解析エラー: {pair} - {e}")

        # パターン3: カンマで区切られた数値
        try:
            parts = pair.strip().split(',')
            numeric_parts = []

            for part in parts:
                try:
                    # 数値変換を試みる
                    numeric_parts.append(float(part.strip()))
                except ValueError:
                    pass

            if len(numeric_parts) >= 2:
                lon = numeric_parts[0]  # 経度
                lat = numeric_parts[1]  # 緯度
                return [lon, lat]
        except Exception as e:
            if debug_mode:
                print(f"  警告: カンマ区切りパターン解析エラー: {pair} - {e}")
                
        return None
        
    @staticmethod
    def _handle_duplicate_coordinates(
        coords: List[List[float]], 
        preserve_duplicates: bool, 
        debug_mode: bool
    ) -> List[List[float]]:
        """
        重複する座標を処理する内部ヘルパー関数
        
        Args:
            coords: 座標のリスト
            preserve_duplicates: 重複を維持するかどうか
            debug_mode: デバッグ情報を表示するかどうか
            
        Returns:
            List[List[float]]: 重複処理後の座標リスト
        """
        orig_len = len(coords)

        # 重複を維持する場合はそのまま返す
        if preserve_duplicates:
            if debug_mode and len(coords) > 0:
                print(f"  重複座標を維持: 座標数 {len(coords)}")
            return coords

        # 重複する座標を除去（同じ座標が続く場合）
        unique_coords = []
        for coord in coords:
            if not unique_coords or coord != unique_coords[-1]:
                unique_coords.append(coord)

        # 重複除去の結果を報告
        if len(unique_coords) < orig_len and debug_mode:
            print(f"  警告: 重複する座標 {orig_len - len(unique_coords)} 個を除去しました")
            if len(coords) <= 10:  # 座標数が少ない場合は表示
                print(f"  元の座標: {coords}")
                print(f"  重複除去後: {unique_coords}")

        return unique_coords


class CoordinateProcessor:
    """座標データを処理するクラス"""
    
    @staticmethod
    def fix_intersections(coords: List[List[float]]) -> List[List[float]]:
        """
        交差するラインを検出して修正する
        Graham scanアルゴリズムの一部を使って点を反時計回りに並べ替える
        
        Args:
            coords: 座標のリスト
            
        Returns:
            List[List[float]]: 修正後の座標リスト
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
    
    @staticmethod
    def validate_oneway_coordinates(
        coords: List[List[float]], 
        unique_key: str, 
        direction_code: Optional[str] = None, 
        debug_mode: bool = False
    ) -> List[List[float]]:
        """
        一方通行の座標順序を検証し、必要に応じて調整する関数
        
        【仕様に基づく処理】
        - 指定・禁止方向の別コード=1（禁止）の場合：
          座標順序: 進入禁止地点→中間点→一方通行開始地点（禁止方向に沿って）
          処理: そのまま使用（この順序が正しい）
        - 指定・禁止方向の別コード=2（指定）の場合：
          座標順序: 一方通行開始地点→中間点→進入禁止地点（通行可能方向に沿って）
          処理: 座標を逆順にして禁止方向に統一
          
        Args:
            coords: 座標のリスト
            unique_key: デバッグ用のユニークID
            direction_code: 指定・禁止方向の別コード
            debug_mode: デバッグ情報を表示するかどうか
            
        Returns:
            List[List[float]]: 検証・調整後の座標リスト
        """
        if len(coords) < 2:
            if debug_mode:
                print(f"  警告: 一方通行(ID:{unique_key})の座標が2点未満です")
            return coords

        # 方向コードの確認と処理
        if direction_code is not None:
            # 数値として処理を試みる
            direction_code_num = None
            try:
                direction_code_num = float(direction_code)
            except (ValueError, TypeError):
                pass

            # 文字列に変換して処理
            direction_code_str = str(direction_code).strip()

            # 数値の1または文字列の"1"または"1.0"をチェック（禁止方向）
            if direction_code_str == "1" or direction_code_str == "1.0" or direction_code_num == 1:
                # 禁止方向（コード1）
                # 座標順序: 進入禁止地点→一方通行開始地点（禁止方向に沿って）
                # この順序が正しいので、そのまま使用
                if debug_mode:
                    print(f"  一方通行(ID:{unique_key})の方向コードは'禁止'(1)")
                    print(f"  座標順序: 進入禁止地点→一方通行開始地点（禁止方向に沿って）")
                    print(f"  座標順序は正しいのでそのまま使用します")
                return coords

            # 数値の2または文字列の"2"または"2.0"をチェック（指定方向）
            elif direction_code_str == "2" or direction_code_str == "2.0" or direction_code_num == 2:
                # 指定方向（コード2）
                # 座標順序: 一方通行開始地点→進入禁止地点（通行可能方向に沿って）
                # 禁止方向に統一するため、座標を逆順にする
                if debug_mode:
                    print(f"  一方通行(ID:{unique_key})の方向コードは'指定'(2)")
                    print(f"  元の座標順序: 一方通行開始地点→進入禁止地点（通行可能方向に沿って）")
                    print(f"  座標を逆順にして禁止方向に統一します")
                    print(f"  元の座標: {coords}")

                # 座標を逆順にする
                reversed_coords = list(reversed(coords))
                
                if debug_mode:
                    print(f"  逆順後の座標: {reversed_coords}")
                    
                return reversed_coords
            else:
                if debug_mode:
                    print(f"  警告: 一方通行(ID:{unique_key})の方向コードが不明です: {direction_code}")
        else:
            if debug_mode:
                print(f"  警告: 一方通行(ID:{unique_key})の方向コードが指定されていません")

        if debug_mode:
            print(f"  座標数: {len(coords)}")
            if len(coords) > 0:
                print(f"  始点: {coords[0]}")
            if len(coords) > 1:
                print(f"  終点: {coords[-1]}")

        # 方向コードが不明な場合はそのまま返す
        return coords


# 下位互換性のためのユーティリティ関数
def parse_coordinates(coord_str: Any, 
                     debug_mode: bool = False, 
                     preserve_duplicates: bool = False) -> List[List[float]]:
    """
    緯度経度の文字列からリストを作成する関数
    セミコロンで区切られた座標ペアを処理する
    
    Args:
        coord_str: 座標文字列
        debug_mode: デバッグ情報を表示するかどうか
        preserve_duplicates: 重複座標を維持するかどうか（一方通行データ用）
        
    Returns:
        List[List[float]]: 解析された座標のリスト
    """
    return CoordinateParser.parse_coordinates(coord_str, debug_mode, preserve_duplicates)


def fix_intersections(coords: List[List[float]]) -> List[List[float]]:
    """
    交差するラインを検出して修正する
    
    Args:
        coords: 座標のリスト
        
    Returns:
        List[List[float]]: 修正後の座標リスト
    """
    return CoordinateProcessor.fix_intersections(coords)


def validate_oneway_coordinates(
    coords: List[List[float]], 
    unique_key: str, 
    direction_code: Optional[str] = None, 
    debug_mode: bool = False
) -> List[List[float]]:
    """
    一方通行の座標順序を検証し、必要に応じて調整する関数
    
    Args:
        coords: 座標のリスト
        unique_key: デバッグ用のユニークID
        direction_code: 指定・禁止方向の別コード
        debug_mode: デバッグ情報を表示するかどうか
        
    Returns:
        List[List[float]]: 検証・調整後の座標リスト
    """
    return CoordinateProcessor.validate_oneway_coordinates(coords, unique_key, direction_code, debug_mode)
