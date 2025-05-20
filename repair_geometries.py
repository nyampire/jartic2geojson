#!/usr/bin/env python3
"""
repair_geometries.py - GEOSを使用してGeoJSONのジオメトリを修正するコマンドラインツール
"""

import argparse
import sys
import os
import logging
from pathlib import Path
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor
import time

# スクリプトのあるディレクトリをPYTHONPATHに追加
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

# 修正モジュールをインポート
try:
    from jartic2geojson.postprocess import (
        fix_geojson_files, process_geojson,
        setup_logging
    )
except ImportError as e:
    print(f"エラー: モジュールのインポートに失敗しました: {e}")
    print("jartic2geojsonパッケージが正しく配置されているか確認してください。")
    print("必要なディレクトリ構造:")
    print("jartic2geojson/")
    print("└── postprocess/")
    print("    ├── __init__.py")
    print("    ├── core.py")
    print("    ├── file_processor.py")
    print("    ├── memory_manager.py")
    print("    ├── logging_utils.py")
    print("    └── reporting.py")
    sys.exit(1)

def parse_arguments():
    """コマンドライン引数をパースする"""
    parser = argparse.ArgumentParser(description='GEOSを使用してGeoJSONのジオメトリを修正するスクリプト')
    parser.add_argument('-i', '--input', default='./output_data',
                        help='入力ディレクトリ (デフォルト: ./output_data)')
    parser.add_argument('-o', '--output', default='./fixed_geojson',
                        help='出力ディレクトリ (デフォルト: ./fixed_geojson)')
    parser.add_argument('-l', '--log', default='./geometry_logs',
                        help='ログディレクトリ (デフォルト: ./geometry_logs)')
    parser.add_argument('-r', '--recursive', action='store_true',
                        help='ディレクトリを再帰的に処理する')
    parser.add_argument('-p', '--pattern', default='*.geojson',
                        help='処理するファイルのパターン (デフォルト: *.geojson)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='詳細なログを出力する')
    parser.add_argument('--test', action='store_true',
                        help='テストモード: 処理を実行せず、対象ファイルのリストを表示')
    parser.add_argument('--threads', type=int, default=1,
                        help='並列処理のスレッド数 (デフォルト: 1, 0=CPUコア数)')
    parser.add_argument('--chunk-size', type=int, default=1000,
                        help='一度に処理するフィーチャの数 (デフォルト: 1000)')
    parser.add_argument('--memory-limit', type=float, default=80.0,
                        help='メモリ使用率の上限 (%、デフォルト: 80.0)')

    return parser.parse_args()

def main():
    """メイン関数"""
    args = parse_arguments()
    
    # 入出力ディレクトリの設定
    input_dir = args.input
    output_dir = args.output
    log_dir = args.log
    
    # ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    
    # ロガーの設定
    logger = setup_logging(log_dir, "geometry_repair", args.verbose)
    logger.info(f"GeoJSON ジオメトリ修正ツール")
    logger.info(f"入力ディレクトリ: {input_dir}")
    logger.info(f"出力ディレクトリ: {output_dir}")
    logger.info(f"ログディレクトリ: {log_dir}")
    
    # テストモードの場合
    if args.test:
        logger.info(f"テストモード: 処理対象ファイルのリストを表示します")
        
        input_path = Path(input_dir)
        if args.recursive:
            files = list(input_path.glob(f"**/{args.pattern}"))
        else:
            files = list(input_path.glob(args.pattern))
            
        logger.info(f"処理対象ファイル数: {len(files)}")
        for i, file_path in enumerate(files, 1):
            logger.info(f"[{i}] {file_path}")
        
        return
    
    # スレッド数の設定
    if args.threads == 0:
        num_threads = cpu_count()
    else:
        num_threads = max(1, args.threads)
    
    logger.info(f"使用スレッド数: {num_threads}")
    logger.info(f"チャンクサイズ: {args.chunk_size}")
    logger.info(f"メモリ使用率上限: {args.memory_limit}%")
    
    # 処理設定を準備
    config = {
        "chunk_size": args.chunk_size,
        "memory_limit": args.memory_limit,
        "verbose": args.verbose
    }
    
    # 並列処理を使用する場合
    if num_threads > 1:
        logger.info(f"並列処理モードで実行します")
        process_in_parallel(
            input_dir, output_dir, log_dir, 
            args.pattern, args.recursive, 
            num_threads, config, logger
        )
    else:
        # 単一スレッド処理（標準モード）
        logger.info(f"単一スレッドモードで処理を開始します")
        
        # fix_geojson_files関数を使用して一括処理
        results = fix_geojson_files(
            input_dir=input_dir,
            output_dir=output_dir,
            log_dir=log_dir,
            pattern=args.pattern,
            recursive=args.recursive,
            chunk_size=args.chunk_size,
            memory_limit=args.memory_limit,
            verbose=args.verbose,
            config=config
        )
        
        if not results:
            logger.warning("処理完了: 処理対象ファイルがありませんでした")
        else:
            logger.info(f"処理完了: {len(results)}ファイルを処理しました")

def process_in_parallel(input_dir, output_dir, log_dir, pattern, 
                       recursive, num_threads, config, logger):
    """
    並列処理を行う関数
    
    Args:
        input_dir: 入力ディレクトリ
        output_dir: 出力ディレクトリ
        log_dir: ログディレクトリ
        pattern: ファイルパターン
        recursive: 再帰的処理フラグ
        num_threads: スレッド数
        config: 処理設定
        logger: ロガーオブジェクト
    """
    from jartic2geojson.postprocess.memory_manager import MemoryManager
    from jartic2geojson.postprocess.reporting import create_summary_report
    
    # 入力ファイルの検索
    input_path = Path(input_dir)
    if recursive:
        files = list(input_path.glob(f"**/{pattern}"))
    else:
        files = list(input_path.glob(pattern))
        
    if not files:
        logger.warning(f"処理対象ファイルが見つかりません")
        return
        
    logger.info(f"処理対象ファイル数: {len(files)}")
    
    # 結果を格納するリスト
    results = []
    
    # 開始時間を記録
    start_time = time.time()
    
    # 各ファイルを処理する関数
    def process_file(file_path):
        """
        単一ファイルを処理する関数（並列処理用）
        
        Args:
            file_path: 処理対象のファイルパス
            
        Returns:
            Dict: 処理結果
        """
        rel_path = file_path.relative_to(input_path)
        output_path = Path(output_dir) / rel_path.with_name(f"{rel_path.stem}_fixed{rel_path.suffix}")
        log_path = Path(log_dir) / rel_path.with_suffix('.log')
        
        # 出力ディレクトリを作成
        os.makedirs(output_path.parent, exist_ok=True)
        os.makedirs(log_path.parent, exist_ok=True)
        
        file_logger = logging.getLogger(f"file_{os.path.basename(file_path)}")
        file_logger.info(f"Processing file: {file_path}")
        
        try:
            # process_geojsonの戻り値をディクショナリに変換
            total, invalid, fixed, unfixable, methods, skipped = process_geojson(
                str(file_path), 
                str(output_path), 
                str(log_path),
                chunk_size=config['chunk_size'],
                memory_limit=config['memory_limit']
            )
            
            result = {
                "file": str(rel_path),
                "total_features": total,
                "invalid_features": invalid,
                "fixed_features": fixed,
                "unfixable_features": unfixable,
                "skipped_features": skipped,
                "fix_methods": methods
            }
            
            file_logger.info(f"Completed processing: {total} features, {invalid} invalid, {fixed} fixed")
            return result
            
        except Exception as e:
            error_msg = f"Error processing {file_path}: {e}"
            file_logger.error(error_msg)
            import traceback
            file_logger.debug(traceback.format_exc())
            
            return {
                "file": str(rel_path),
                "error": str(e),
                "total_features": 0,
                "invalid_features": 0,
                "fixed_features": 0,
                "unfixable_features": 0,
                "skipped_features": 0,
                "fix_methods": {}
            }
    
    # ファイル数に基づいてバッチサイズを計算
    batch_size = max(1, min(100, len(files) // num_threads))
    
    # メモリマネージャー
    memory_manager = MemoryManager(limit_percent=config['memory_limit'])
    
    # バッチ処理で並列実行
    for batch_start in range(0, len(files), batch_size):
        batch_end = min(batch_start + batch_size, len(files))
        batch_files = files[batch_start:batch_end]
        
        logger.info(f"バッチ処理: {batch_start//batch_size + 1}/{(len(files) + batch_size - 1)//batch_size} - {len(batch_files)}ファイル")
        
        # 並列処理の実行
        with ProcessPoolExecutor(max_workers=num_threads) as executor:
            batch_results = list(executor.map(process_file, batch_files))
            results.extend(batch_results)
        
        # 進捗情報
        elapsed = time.time() - start_time
        processed = batch_end
        avg_time = elapsed / processed if processed > 0 else 0
        remaining = avg_time * (len(files) - processed)
        
        logger.info(f"進捗: {processed}/{len(files)} ({processed/len(files)*100:.1f}%) - 残り時間: {remaining/60:.1f}分")
        
        # メモリ使用状況をチェック
        memory_manager.log_memory_usage(logger, "バッチ処理後: ")
        memory_manager.check(logger=logger, force_gc=True)
    
    # 処理完了
    total_elapsed = time.time() - start_time
    logger.info(f"全ファイル処理完了: 所要時間 {total_elapsed/60:.1f}分")
    
    # サマリーレポート作成
    if results:
        try:
            json_summary, text_summary = create_summary_report(results, input_dir, output_dir, log_dir)
            logger.info(f"サマリーレポート作成完了:\n  JSON: {json_summary}\n  テキスト: {text_summary}")
        except Exception as e:
            logger.error(f"サマリーレポート作成中にエラー: {e}")
            import traceback
            logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()
