"""
jartic2geojson/postprocess/__init__.py - ジオメトリ修正機能のエントリーポイント
"""

from .core import is_valid_geometry, fix_geometry
from .file_processor import GeoJSONProcessor, process_geojson_file
from .memory_manager import MemoryManager
from .logging_utils import setup_logging
from .reporting import create_summary_report

# 公開API
__all__ = [
    'fix_geojson_files',
    'process_geojson',
    'setup_logging',
    'create_summary_report'
]

def fix_geojson_files(input_dir, output_dir, log_dir,
                      pattern="*.geojson", recursive=False,
                      chunk_size=1000, memory_limit=80.0,
                      verbose=False, config=None):
    """
    ディレクトリ内のGeoJSONファイルのジオメトリを修正する

    Args:
        input_dir: 入力ディレクトリ
        output_dir: 出力ディレクトリ
        log_dir: ログディレクトリ
        pattern: ファイル検索パターン
        recursive: サブディレクトリも処理するか
        chunk_size: 一度に処理するフィーチャ数
        memory_limit: メモリ使用率の上限 (%)
        verbose: 詳細なログを出力するか
        config: 追加の設定

    Returns:
        List[Dict]: 処理結果のリスト
    """
    from pathlib import Path
    import os
    import time

    # 設定を統合
    processor_config = {
        "chunk_size": chunk_size,
        "memory_limit": memory_limit,
        "verbose": verbose
    }
    if config:
        processor_config.update(config)

    # ロガーの設定
    logger = setup_logging(log_dir, "batch_fix", verbose)
    logger.info(f"Starting batch processing of GeoJSON files")

    # ディレクトリの確認と作成
    input_path = Path(input_dir)
    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        return []

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # ファイルの検索
    files = []
    if recursive:
        files = list(input_path.glob(f"**/{pattern}"))
    else:
        files = list(input_path.glob(pattern))

    if not files:
        logger.warning(f"No files found matching pattern '{pattern}' in {input_dir}")
        return []

    logger.info(f"Found {len(files)} GeoJSON files to process")

    # 処理結果を保存するリスト
    results = []

    # メモリマネージャーを初期化
    memory_manager = MemoryManager(limit_percent=memory_limit)

    # 処理開始時間
    start_time = time.time()

    # GeoJSONプロセッサを作成
    processor = GeoJSONProcessor(processor_config, logger)

    # 各ファイルを処理
    for i, file_path in enumerate(files, 1):
        # 相対パスを取得
        rel_path = file_path.relative_to(input_path)

        # 出力パスとログパスを構築
        output_path = Path(output_dir) / rel_path.with_name(f"{rel_path.stem}_fixed{rel_path.suffix}")
        log_path = Path(log_dir) / rel_path.with_suffix('.log')

        # 出力ディレクトリの作成
        output_path.parent.mkdir(exist_ok=True, parents=True)
        log_path.parent.mkdir(exist_ok=True, parents=True)

        logger.info(f"[{i}/{len(files)}] Processing: {file_path}")

        # ファイルの処理
        try:
            file_result = processor.process_file(
                str(file_path),
                str(output_path),
                str(log_path)
            )

            # 相対パスを設定
            file_result["file"] = str(rel_path)
            results.append(file_result)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            import traceback
            logger.debug(traceback.format_exc())

            # エラー情報を結果に追加
            results.append({
                "file": str(rel_path),
                "error": str(e),
                "total_features": 0,
                "invalid_features": 0,
                "fixed_features": 0,
                "unfixable_features": 0,
                "skipped_features": 0,
                "fix_methods": {}
            })

        # 進捗状況と予測残り時間
        elapsed = time.time() - start_time
        avg_time = elapsed / i
        remaining = avg_time * (len(files) - i)

        logger.info(f"Progress: {i}/{len(files)} ({i/len(files)*100:.1f}%) - ETA: {remaining/60:.1f} min")

        # メモリ状況をチェック
        memory_manager.check(logger=logger, force_gc=True)

    # 処理完了情報
    total_elapsed = time.time() - start_time
    logger.info(f"All files processed in {total_elapsed/60:.1f} minutes")

    # 処理結果のサマリーを作成
    try:
        create_summary_report(results, input_dir, output_dir, log_dir)
    except Exception as e:
        logger.error(f"Error creating summary report: {e}")

    return results

def process_geojson(input_file, output_file, log_file,
                    chunk_size=1000, memory_limit=80.0):
    """
    既存APIとの互換性を保つためのラッパー関数
    """
    processor = GeoJSONProcessor({
        "chunk_size": chunk_size,
        "memory_limit": memory_limit
    })

    result = processor.process_file(input_file, output_file, log_file)

    return (
        result["total_features"],
        result["invalid_features"],
        result["fixed_features"],
        result["unfixable_features"],
        result["fix_methods"],
        result["skipped_features"]
    )
