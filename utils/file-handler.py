"""
ファイル処理ユーティリティ - CSVファイルの読み込みと解析を行う機能を提供します。
"""

import sys
import io
import codecs
import re
import pandas as pd
from typing import List, Optional, Tuple, Dict, Any


class FileHandler:
    """ファイル読み込みと処理を行うクラス"""
    
    # サポートするエンコーディングのリスト
    SUPPORTED_ENCODINGS = ['shift_jis', 'cp932', 'euc-jp', 'utf-8']
    
    def __init__(self, debug_mode: bool = False):
        """
        初期化関数
        
        Args:
            debug_mode: デバッグ情報を表示するかどうか
        """
        self.debug_mode = debug_mode
    
    def read_csv_file(self, file_path: str) -> pd.DataFrame:
        """
        CSVファイルを読み込んでデータフレームとして返す
        
        Args:
            file_path: 入力ファイルのパス
            
        Returns:
            pd.DataFrame: 読み込まれたデータフレーム
            
        Raises:
            ValueError: ファイル読み込みに失敗した場合
        """
        try:
            # ファイルの内容を取得
            file_content = self._read_file_with_encoding(file_path)
            
            # CSVとして処理
            df = self._parse_csv_content(file_content)
            
            if self.debug_mode:
                print(f"CSVファイルを正常に読み込みました: {len(df)}行 x {len(df.columns)}列")
                
            return df
            
        except Exception as e:
            # 手動解析を試行
            if self.debug_mode:
                print(f"標準的なCSV解析に失敗しました: {e}")
                print("手動解析を試みます...")
                
            try:
                df = self._manually_parse_csv(file_path)
                return df
            except Exception as manual_error:
                error_msg = f"ファイルの読み込みと解析に失敗しました: {manual_error}"
                if self.debug_mode:
                    print(f"エラー: {error_msg}")
                raise ValueError(error_msg)
    
    def _read_file_with_encoding(self, file_path: str) -> str:
        """
        複数のエンコーディングを試して最適なものでファイルを読み込む
        
        Args:
            file_path: 入力ファイルのパス
            
        Returns:
            str: 読み込まれたファイル内容
            
        Raises:
            ValueError: どのエンコーディングでも読み込めなかった場合
        """
        for encoding in self.SUPPORTED_ENCODINGS:
            try:
                with codecs.open(file_path, 'r', encoding=encoding) as f:
                    file_content = f.read()
                if self.debug_mode:
                    print(f"ファイルを {encoding} エンコーディングで読み込みました")
                return file_content
            except UnicodeDecodeError:
                if self.debug_mode:
                    print(f"{encoding} での読み込みに失敗")
                continue
        
        raise ValueError("ファイルをどのエンコーディングでも読み込めませんでした")
    
    def _parse_csv_content(self, file_content: str) -> pd.DataFrame:
        """
        ファイル内容をCSVとして解析する
        
        Args:
            file_content: ファイル内容の文字列
            
        Returns:
            pd.DataFrame: 解析されたデータフレーム
            
        Raises:
            ValueError: CSV解析に失敗した場合
        """
        # ファイル内容を行に分割
        lines = file_content.splitlines()

        # 特別なヘッダー行や余分な行を除去してCSVデータを特定
        csv_lines = self._extract_csv_lines(lines)

        if not csv_lines:
            raise ValueError("CSVデータが見つかりませんでした")

        # メモリ上でCSVを解析
        csv_content = '\n'.join(csv_lines)
        df = pd.read_csv(io.StringIO(csv_content), low_memory=False)

        # カラム名をクリーンアップ
        df.columns = [col.strip('"').strip() for col in df.columns]

        return df
    
    def _extract_csv_lines(self, lines: List[str]) -> List[str]:
        """
        テキスト行からCSVとして解析できる行を抽出する
        
        Args:
            lines: テキスト行のリスト
            
        Returns:
            List[str]: CSVとして解析できる行のリスト
        """
        csv_lines = []
        header_found = False
        csv_data_started = False

        for line in lines:
            # ファイル名やヘッダーのような行をスキップ
            if line.startswith('==>') or line.startswith('<=='):
                continue

            # CSVデータの行かどうかを判断
            if '"' in line and ',' in line:
                if not header_found:
                    header_found = True
                csv_data_started = True
                csv_lines.append(line)
            elif csv_data_started and line.strip():
                # CSVデータが始まった後の非空行
                csv_lines.append(line)
                
        return csv_lines
    
    def _manually_parse_csv(self, file_path: str) -> pd.DataFrame:
        """
        標準的な解析に失敗した場合の手動CSV解析を試みる
        
        Args:
            file_path: 入力ファイルのパス
            
        Returns:
            pd.DataFrame: 手動解析されたデータフレーム
            
        Raises:
            ValueError: 手動解析にも失敗した場合
        """
        # エラー許容モードでファイルを読み込み
        with codecs.open(file_path, 'r', encoding='shift_jis', errors='ignore') as f:
            content = f.read()

        # CSVのような行を特定
        csv_pattern = r'"[^"]*"(,"[^"]*")+' # "文字列","文字列",...のパターン
        potential_csv_lines = []

        for line in content.splitlines():
            if re.match(csv_pattern, line) or line.count(',') > 5:  # CSVらしい行
                potential_csv_lines.append(line)

        if not potential_csv_lines:
            raise ValueError("CSVデータを手動でも特定できませんでした")
            
        # 最初の行をヘッダーとして扱う
        header = potential_csv_lines[0]
        data_lines = potential_csv_lines[1:]

        # カラム名を抽出
        headers = re.findall(r'"([^"]*)"', header)
        if not headers and ',' in header:
            headers = [col.strip('"').strip() for col in header.split(',')]

        # データ行を処理
        data = []
        for line in data_lines:
            values = re.findall(r'"([^"]*)"', line)
            if not values and ',' in line:
                values = [val.strip('"').strip() for val in line.split(',')]

            row_dict = {}
            for i, val in enumerate(values):
                if i < len(headers):
                    row_dict[headers[i]] = val

            data.append(row_dict)

        # データフレームを作成
        df = pd.DataFrame(data)
        
        if self.debug_mode:
            print(f"手動解析に成功しました: {len(df)}行 x {len(df.columns)}列")
            
        return df


# 下位互換性のためのユーティリティ関数
def process_raw_text_file(file_path: str, debug_mode: bool = False) -> pd.DataFrame:
    """
    テキスト形式のファイルを処理して、CSVとして解析します
    
    Args:
        file_path: 入力ファイルのパス
        debug_mode: デバッグ情報を表示するかどうか
        
    Returns:
        pd.DataFrame: 解析されたデータフレーム
        
    Raises:
        ValueError: ファイル読み込みに失敗した場合
    """
    handler = FileHandler(debug_mode)
    return handler.read_csv_file(file_path)
