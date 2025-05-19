"""
jartic2geojson/postprocess/memory_manager.py - メモリ使用状況の監視と制御
"""

import gc
import logging
import psutil
from typing import Dict, Optional

class MemoryManager:
    """メモリ使用状況の監視と管理を行うクラス"""
    
    def __init__(self, limit_percent: float = 80.0, check_interval: int = 100, logger: Optional[logging.Logger] = None):
        """
        初期化
        
        Args:
            limit_percent: メモリ使用率の上限 (%)
            check_interval: チェック間隔（処理件数）
            logger: ロガーオブジェクト
        """
        self.limit_percent = limit_percent
        self.check_interval = check_interval
        self.counter = 0
        self.logger = logger or logging.getLogger(__name__)
    
    def check(self, logger: Optional[logging.Logger] = None, force_gc: bool = False) -> bool:
        """
        メモリ使用状況をチェックし、必要に応じてガベージコレクションを実行
        
        Args:
            logger: 使用するロガー（Noneの場合はデフォルトを使用）
            force_gc: 強制的にGCを実行するかどうか
            
        Returns:
            bool: GCが実行された場合はTrue
        """
        log = logger or self.logger
        self.counter += 1
        
        # 強制GCまたはチェック間隔に達した場合
        if force_gc or self.counter % self.check_interval == 0:
            memory = self.get_memory_usage()
            
            # メモリ使用率がリミットを超えているか強制GCの場合
            if memory['percent'] > self.limit_percent or force_gc:
                log.info(f"Memory usage: {memory['usage_mb']:.1f} MB ({memory['percent']:.1f}%), collecting garbage")
                gc.collect()
                
                # GC後のメモリ使用状況をログ
                after_memory = self.get_memory_usage()
                log.info(f"After GC: {after_memory['usage_mb']:.1f} MB ({after_memory['percent']:.1f}%)")
                
                return True
            
            # 定期的にメモリ使用状況をログ
            elif self.counter % (self.check_interval * 10) == 0:
                log.info(f"Memory usage: {memory['usage_mb']:.1f} MB ({memory['percent']:.1f}%)")
            
        return False
    
    @staticmethod
    def get_memory_usage() -> Dict[str, float]:
        """
        現在のメモリ使用状況を取得
        
        Returns:
            Dict[str, float]: メモリ使用状況の辞書
        """
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_usage_bytes = memory_info.rss  # Resident Set Size
        
        # システム全体のメモリ情報
        system_memory = psutil.virtual_memory()
        memory_percent = memory_usage_bytes / system_memory.total * 100
        
        return {
            'usage_bytes': memory_usage_bytes,
            'usage_mb': memory_usage_bytes / (1024 * 1024),
            'percent': memory_percent,
            'system_percent': system_memory.percent,
            'system_available_mb': system_memory.available / (1024 * 1024)
        }
    
    def log_memory_usage(self, logger: Optional[logging.Logger] = None, prefix: str = "") -> Dict[str, float]:
        """
        メモリ使用状況をログに出力
        
        Args:
            logger: 使用するロガー（Noneの場合はデフォルトを使用）
            prefix: ログメッセージの接頭辞
            
        Returns:
            Dict[str, float]: メモリ使用状況の辞書
        """
        log = logger or self.logger
        memory = self.get_memory_usage()
        
        log.info(f"{prefix}Memory usage: {memory['usage_mb']:.1f} MB ({memory['percent']:.1f}% of process), "
                f"System: {memory['system_percent']:.1f}% used, {memory['system_available_mb']:.1f} MB available")
        
        return memory
    
    @staticmethod
    def emergency_cleanup():
        """
        緊急時のメモリクリーンアップ
        GCを複数回実行し、可能な限りメモリを解放
        """
        # 複数回GCを実行
        for _ in range(3):
            gc.collect()
