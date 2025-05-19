"""
列検出ユーティリティ - CSVデータから特殊カラムを検出するための機能を提供します。
"""

import re
from typing import Dict, Optional, List, Any, Callable
import pandas as pd


class ColumnDetector:
    """
    データフレームから特殊カラムを検出するクラス
    """
    
    def __init__(self, df: pd.DataFrame, debug_mode: bool = False):
        """
        初期化関数
        
        Args:
            df: 検査対象のデータフレーム
            debug_mode: デバッグ情報を表示するかどうか
        """
        self.df = df
        self.debug_mode = debug_mode
        self.column_info = {}
        
    def detect_all_columns(self) -> Dict[str, Optional[str]]:
        """
        データフレームから各種特殊カラムを検出する
        
        Returns:
            Dict[str, Optional[str]]: 検出した特殊カラム名の辞書
        """
        # 緯度経度が含まれるカラム名を探す
        coord_column = self.detect_coordinate_column()

        # ユニークキーのカラム名を探す
        unique_key_column = self.detect_unique_key_column()

        # 点・線・面コードのカラム名を探す
        geometry_type_column = self.detect_geometry_type_column()

        # 共通規制種別コードのカラム名を探す
        regulation_code_column = self.detect_regulation_code_column()

        # 指定・禁止方向の別コードのカラム名を探す
        direction_code_column = self.detect_direction_code_column()

        self.column_info = {
            'coord_column': coord_column,
            'unique_key_column': unique_key_column,
            'geometry_type_column': geometry_type_column,
            'regulation_code_column': regulation_code_column,
            'direction_code_column': direction_code_column
        }
        
        return self.column_info

    def detect_coordinate_column(self) -> Optional[str]:
        """
        緯度経度カラムを検出する関数
        
        Returns:
            Optional[str]: 検出された列名、見つからない場合はNone
        """
        coord_column = None
        coord_column_patterns = [
            '規制場所の経度緯度',
            '規制場所',
            '経度緯度',
            '緯度経度',
            'coordinates'
        ]

        # パターンマッチによる検出
        for pattern in coord_column_patterns:
            for col in self.df.columns:
                if pattern in col:
                    coord_column = col
                    if self.debug_mode:
                        print(f"緯度経度カラムを発見: '{coord_column}'")
                    return coord_column

        # カラムが見つからない場合は、内容で推測
        for col in self.df.columns:
            # サンプル値を取得
            sample_values = self.df[col].dropna().head(5).astype(str)

            # 緯度経度らしい値が含まれているか確認
            for val in sample_values:
                # 数値のペアとセミコロンが含まれているか確認
                if ';' in val and re.search(r'\d+\.\d+', val):
                    coord_column = col
                    if self.debug_mode:
                        print(f"緯度経度カラムを内容から推測: '{coord_column}'")
                    return coord_column

        return None
    
    def detect_unique_key_column(self) -> Optional[str]:
        """
        ユニークキーカラムを検出する関数
        
        Returns:
            Optional[str]: 検出された列名、見つからない場合はNone
        """
        key_column_patterns = [
            'ユニークキー',
            'ID',
            'id',
            'Key',
            'key'
        ]

        return self._detect_column_by_patterns(key_column_patterns, "ユニークキーカラム")

    def detect_geometry_type_column(self) -> Optional[str]:
        """
        点・線・面コードカラムを検出する関数
        
        Returns:
            Optional[str]: 検出された列名、見つからない場合はNone
        """
        geometry_type_patterns = [
            '点・線・面コード',
            '点線面コード'
        ]

        return self._detect_column_by_patterns(geometry_type_patterns, "点・線・面コードカラム")

    def detect_regulation_code_column(self) -> Optional[str]:
        """
        共通規制種別コードカラムを検出する関数
        
        Returns:
            Optional[str]: 検出された列名、見つからない場合はNone
        """
        regulation_code_patterns = [
            '共通規制種別コード',
        ]

        return self._detect_column_by_patterns(regulation_code_patterns, "共通規制種別コードカラム")

    def detect_direction_code_column(self) -> Optional[str]:
        """
        指定・禁止方向の別コードカラムを検出する関数
        
        Returns:
            Optional[str]: 検出された列名、見つからない場合はNone
        """
        direction_code_patterns = [
            '指定・禁止方向の別コード',
            '指定禁止方向別コード',
            '方向コード'
        ]

        # 大文字小文字を区別せずに検索
        for pattern in direction_code_patterns:
            for col in self.df.columns:
                if isinstance(col, str) and pattern.lower() in col.lower():
                    if self.debug_mode:
                        print(f"指定・禁止方向の別コードカラムを発見: '{col}'")
                    return col

        return None

    def _detect_column_by_patterns(self, patterns: List[str], column_description: str) -> Optional[str]:
        """
        パターンリストを使用して列を検出する内部ヘルパー関数
        
        Args:
            patterns: 検索パターンのリスト
            column_description: ログ出力用の列説明
            
        Returns:
            Optional[str]: 検出された列名、見つからない場合はNone
        """
        for pattern in patterns:
            for col in self.df.columns:
                if pattern in col:
                    if self.debug_mode:
                        print(f"{column_description}を発見: '{col}'")
                    return col
        
        return None

    def get_column_candidates(self, column_type: str) -> List[Dict[str, Any]]:
        """
        特定の列タイプの候補を取得する
        
        Args:
            column_type: 列タイプ（'regulation', 'coordinate', etc.）
            
        Returns:
            List[Dict[str, Any]]: 候補列の情報リスト
        """
        candidates = []
        
        if column_type == 'regulation':
            # 共通規制種別コードの候補を探す（数値のみの列）
            for i, col in enumerate(self.df.columns):
                # サンプル値を取得
                sample_values = self.df[col].dropna().head(5).astype(str)

                # 数値のみのカラムをチェック
                if all(val.isdigit() for val in sample_values if val.strip()):
                    candidates.append({
                        'index': i,
                        'name': col,
                        'samples': list(sample_values)
                    })
        
        elif column_type == 'coordinate':
            # 緯度経度カラムの候補を探す
            for i, col in enumerate(self.df.columns):
                sample_values = self.df[col].dropna().head(5).astype(str)
                
                # 緯度経度らしい値が含まれているか確認
                has_coords = any(';' in val and re.search(r'\d+\.\d+', val) for val in sample_values)
                if has_coords:
                    candidates.append({
                        'index': i,
                        'name': col,
                        'samples': list(sample_values)
                    })
        
        return candidates

    def handle_column_selection(self, 
                                column_type: str, 
                                input_handler: Callable[[List[Dict[str, Any]]], Optional[int]] = None
                               ) -> Optional[str]:
        """
        列選択を処理する拡張関数
        
        Args:
            column_type: 列タイプ（'regulation', 'coordinate', etc.）
            input_handler: ユーザー入力を処理するコールバック関数
            
        Returns:
            Optional[str]: 選択された列名、選択されなかった場合はNone
        """
        # 既に検出されている場合はその値を返す
        if column_type == 'regulation' and self.column_info.get('regulation_code_column'):
            return self.column_info.get('regulation_code_column')
        
        if column_type == 'coordinate' and self.column_info.get('coord_column'):
            return self.column_info.get('coord_column')
        
        # 候補を取得
        candidates = self.get_column_candidates(column_type)
        
        if not candidates:
            if self.debug_mode:
                print(f"{column_type}カラムの候補が見つかりませんでした。")
            return None
        
        # 入力ハンドラがない場合はデフォルトの処理
        if input_handler is None:
            if len(candidates) == 1:
                # 候補が1つだけなら自動選択
                selected_index = candidates[0]['index']
            else:
                # 対話的な選択は行わず、None を返す
                return None
        else:
            # 入力ハンドラを使用して選択
            selected_index = input_handler(candidates)
            
        # 選択された列を返す
        if selected_index is not None and 0 <= selected_index < len(self.df.columns):
            selected_column = self.df.columns[selected_index]
            if self.debug_mode:
                print(f"選択された{column_type}カラム: '{selected_column}'")
            return selected_column
        
        return None


# 単純なユーティリティ関数として従来の関数もサポート
def detect_columns(df: pd.DataFrame, debug_mode: bool = False) -> Dict[str, Optional[str]]:
    """
    データフレームから各種特殊カラムを検出する関数
    
    Args:
        df: 検査対象のデータフレーム
        debug_mode: デバッグ情報を表示するかどうか
        
    Returns:
        Dict[str, Optional[str]]: 検出した特殊カラム名の辞書
    """
    detector = ColumnDetector(df, debug_mode)
    return detector.detect_all_columns()


def handle_regulation_code_selection(df: pd.DataFrame, 
                                    regulation_code_column: Optional[str], 
                                    split_by_regulation: bool,
                                    input_handler: Callable[[List[Dict[str, Any]]], Optional[int]] = None
                                   ) -> tuple:
    """
    規制コードカラムがない場合の処理
    
    Args:
        df: データフレーム
        regulation_code_column: 既に検出された規制コードカラム
        split_by_regulation: 規制種別ごとに分割するかどうか
        input_handler: ユーザー入力を処理するコールバック関数
        
    Returns:
        tuple: (規制コードカラム, 分割フラグ)
    """
    if not regulation_code_column and split_by_regulation:
        detector = ColumnDetector(df)
        regulation_code_column = detector.handle_column_selection('regulation', input_handler)
        
        if not regulation_code_column:
            print("共通規制種別コードのカラムが見つかりませんでした。分割せずに処理を続行します。")
            split_by_regulation = False

    return regulation_code_column, split_by_regulation
