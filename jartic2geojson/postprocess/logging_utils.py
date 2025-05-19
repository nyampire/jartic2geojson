"""
jartic2geojson/postprocess/logging_utils.py - ロギング関連のユーティリティ
"""

import os
import logging
from datetime import datetime
from typing import Optional

def setup_logging(log_dir: str, 
                 log_name: Optional[str] = None, 
                 verbose: bool = False) -> logging.Logger:
    """
    ロギングの設定を行う
    
    Args:
        log_dir: ログディレクトリ
        log_name: ロガー名（Noneの場合はモジュール名）
        verbose: 詳細なログを出力するかどうか
        
    Returns:
        logging.Logger: 設定されたロガーオブジェクト
    """
    # ログディレクトリを作成
    os.makedirs(log_dir, exist_ok=True)
    
    # ログレベルの設定
    level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # ロガーを取得
    logger = logging.getLogger(log_name or __name__)
    
    # ロガーレベルを設定
    logger.setLevel(level)
    
    # 既存のハンドラを削除（重複防止）
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # コンソールハンドラを追加
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setLevel(level)
    logger.addHandler(console_handler)
    
    # ファイルハンドラを追加
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if log_name:
        file_handler = logging.FileHandler(os.path.join(log_dir, f"{log_name}_{timestamp}.log"))
    else:
        file_handler = logging.FileHandler(os.path.join(log_dir, f"process_{timestamp}.log"))
    
    file_handler.setFormatter(logging.Formatter(log_format))
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    
    return logger

def setup_file_logger(log_file: str, 
                     logger_name: Optional[str] = None, 
                     verbose: bool = False) -> logging.Logger:
    """
    特定のファイル用ロガーの設定
    
    Args:
        log_file: ログファイルのパス
        logger_name: ロガー名
        verbose: 詳細なログを出力するかどうか
        
    Returns:
        logging.Logger: 設定されたロガーオブジェクト
    """
    # ログディレクトリを作成
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # ロガー名を設定
    if logger_name is None:
        logger_name = os.path.basename(log_file)
    
    # ロガーを取得
    logger = logging.getLogger(logger_name)
    
    # ロガーレベルを設定
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    
    # 既存のハンドラを削除（重複防止）
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # ファイルハンドラを追加
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    
    return logger

def close_logger(logger: logging.Logger):
    """
    ロガーのクリーンアップを行う
    
    Args:
        logger: クローズするロガーオブジェクト
    """
    # ハンドラをクローズして削除
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
