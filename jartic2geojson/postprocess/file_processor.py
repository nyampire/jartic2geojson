"""
jartic2geojson/postprocess/file_processor.py - GeoJSONファイル処理機能
"""

import os
import logging
import time
import gc
import json
from typing import Dict, List, Optional, Any, Tuple
from shapely.geometry import shape, mapping
import fiona

from .core import (
    is_valid_geometry, fix_geometry, is_large_integer_field,
    adjust_schema_for_large_integers, create_default_pipeline
)
from .memory_manager import MemoryManager


class GeoJSONProcessor:
    """GeoJSONファイルを処理するクラス"""
    
    def __init__(self, config=None, logger=None):
        """
        初期化
        
        Args:
            config: 設定辞書
            logger: ロガーオブジェクト
        """
        self.config = config or {}
        self.logger = logger or logging.getLogger(__name__)
        
        # デフォルト設定
        self.chunk_size = self.config.get("chunk_size", 1000)
        self.memory_limit = self.config.get("memory_limit", 80.0)
        self.verbose = self.config.get("verbose", False)
        
        # メモリ管理機能
        self.memory_manager = MemoryManager(
            limit_percent=self.memory_limit,
            logger=self.logger
        )
        
        # 処理パイプライン
        self.pipeline = create_default_pipeline(self.logger)
    
    def process_file(self, input_file: str, output_file: str, log_file: str) -> Dict[str, Any]:
        """
        GeoJSONファイルを処理し、ジオメトリを修正する
        
        Args:
            input_file: 入力GeoJSONファイルパス
            output_file: 出力GeoJSONファイルパス
            log_file: ログファイルパス
            
        Returns:
            Dict[str, Any]: 処理結果の統計情報
        """
        # ファイル専用のロガーを設定
        file_logger = self._setup_file_logger(log_file)
        file_logger.info(f"Processing file: {input_file}")
        
        # 処理統計の初期化
        stats = {
            "total_features": 0,
            "invalid_features": 0,
            "fixed_features": 0,
            "unfixable_features": 0,
            "skipped_features": 0,
            "fix_methods": {}
        }
        
        try:
            # ファイル処理の開始時間
            start_time = time.time()
            
            # ファイル情報と各フィーチャの処理
            with fiona.open(input_file, 'r') as source:
                file_logger.info(f"File contains {len(source)} features")
                
                # スキーマの調整
                original_schema = source.schema
                adjusted_schema = adjust_schema_for_large_integers(original_schema)
                crs = source.crs
                
                # スキーマ情報のデバッグ出力
                if self.verbose:
                    file_logger.debug(f"Original schema: {json.dumps(original_schema)}")
                    file_logger.debug(f"Adjusted schema: {json.dumps(adjusted_schema)}")
                
                # 出力ディレクトリの作成
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # バッチ処理
                self._process_features_in_batches(
                    source, output_file, adjusted_schema, crs, 
                    stats, file_logger
                )
            
            # 処理時間の計算
            end_time = time.time()
            processing_time = end_time - start_time
            
            # 結果情報を出力
            self._log_processing_results(file_logger, stats, processing_time)
            
            return stats
            
        except Exception as e:
            file_logger.error(f"Failed to process {input_file}: {e}")
            import traceback
            file_logger.debug(traceback.format_exc())
            
            # エラー情報を統計に追加
            stats["error"] = str(e)
            return stats
            
        finally:
            # クリーンアップ
            gc.collect()
            
            # ファイルハンドラをクローズ
            for handler in file_logger.handlers[:]:
                handler.close()
                file_logger.removeHandler(handler)
    
    def _setup_file_logger(self, log_file: str) -> logging.Logger:
        """
        ファイル処理用のロガーを設定
        
        Args:
            log_file: ログファイルパス
            
        Returns:
            logging.Logger: 設定されたロガー
        """
        logger = logging.getLogger(os.path.basename(log_file))
        
        # ハンドラが既に存在する場合は削除
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # ファイルロガーを設定
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
        
        # ログレベルの設定
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        
        return logger
    
    def _process_features_in_batches(self, 
                                      source: fiona.Collection, 
                                      output_file: str,
                                      schema: Dict, 
                                      crs: Dict, 
                                      stats: Dict, 
                                      logger: logging.Logger):
        """
        フィーチャをバッチで処理
        
        Args:
            source: 入力ファイルオブジェクト
            output_file: 出力ファイルパス
            schema: スキーマ情報
            crs: 座標参照系情報
            stats: 統計情報の辞書
            logger: ロガー
        """
        with fiona.open(output_file, 'w',
                      driver='GeoJSON',
                      crs=crs,
                      schema=schema) as sink:
            
            feature_buffer = []
            chunk_counter = 0
            gc_trigger = 5  # GCを実行する間隔
            
            # 各フィーチャの処理
            for feature in source:
                stats["total_features"] += 1
                feature_id = feature.get('id', stats["total_features"])
                
                try:
                    # パイプラインでフィーチャを処理
                    processed_feature, metadata = self.pipeline.process(feature)
                    
                    # 処理結果の集計
                    geometry_metadata = metadata.get("geometry_fixing", {})
                    
                    if not geometry_metadata.get("is_valid", True):
                        stats["invalid_features"] += 1
                        
                        fix_method = geometry_metadata.get("fix_method")
                        if fix_method:
                            stats["fix_methods"][fix_method] = stats["fix_methods"].get(fix_method, 0) + 1
                        
                        if geometry_metadata.get("fixed", False):
                            stats["fixed_features"] += 1
                        else:
                            stats["unfixable_features"] += 1
                    
                    if geometry_metadata.get("skipped", False):
                        stats["skipped_features"] += 1
                    
                    # バッファに追加
                    feature_buffer.append(processed_feature)
                    
                    # メモリ使用率チェック
                    if stats["total_features"] % 100 == 0:
                        if self.memory_manager.check(logger=logger):
                            # メモリ使用量が高い場合は早めに書き込み
                            for f in feature_buffer:
                                sink.write(f)
                            feature_buffer = []
                    
                    # バッファがチャンクサイズに達したら書き込み
                    if len(feature_buffer) >= self.chunk_size:
                        for f in feature_buffer:
                            sink.write(f)
                        logger.info(f"Processed chunk of {len(feature_buffer)} features. Total: {stats['total_features']}")
                        feature_buffer = []
                        
                        # 定期的なGC実行
                        chunk_counter += 1
                        if chunk_counter % gc_trigger == 0:
                            self.memory_manager.check(logger=logger, force_gc=True)
                            
                except Exception as e:
                    logger.error(f"Error processing feature {feature_id}: {e}")
                    stats["skipped_features"] += 1
                    if self.verbose:
                        import traceback
                        logger.debug(traceback.format_exc())
            
            # 残りのフィーチャを書き込み
            for f in feature_buffer:
                sink.write(f)
            
            if feature_buffer:
                logger.info(f"Processed final chunk of {len(feature_buffer)} features.")
    
    def _log_processing_results(self, logger: logging.Logger, stats: Dict, processing_time: float):
        """
        処理結果をログに出力
        
        Args:
            logger: ロガー
            stats: 統計情報
            processing_time: 処理時間（秒）
        """
        logger.info(f"Processing completed in {processing_time:.2f} seconds")
        logger.info(f"Total features: {stats['total_features']}")
        logger.info(f"Invalid features: {stats['invalid_features']}")
        logger.info(f"Successfully fixed: {stats['fixed_features']}")
        logger.info(f"Unfixable features: {stats['unfixable_features']}")
        logger.info(f"Skipped features: {stats['skipped_features']}")
        logger.info(f"Fix methods used: {stats['fix_methods']}")


def process_geojson_file(input_file: str, output_file: str, log_file: str, 
                         config: Optional[Dict] = None) -> Dict[str, Any]:
    """
    GeoJSONファイルを処理する便利関数
    
    Args:
        input_file: 入力ファイルパス
        output_file: 出力ファイルパス
        log_file: ログファイルパス
        config: 設定辞書
        
    Returns:
        Dict[str, Any]: 処理結果
    """
    processor = GeoJSONProcessor(config)
    return processor.process_file(input_file, output_file, log_file)
