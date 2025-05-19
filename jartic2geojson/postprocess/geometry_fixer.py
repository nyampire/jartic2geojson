"""
ジオメトリ修正モジュール - GEOSを使用してGeoJSONのジオメトリを修正する機能を提供
"""

import os
import json
import logging
import time
import gc
import psutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Union

from shapely.geometry import shape, mapping
from shapely.validation import make_valid
import shapely.geos
import fiona

# ロギング設定関数
def setup_logging(log_dir: str, log_name: str = None, verbose: bool = False) -> logging.Logger:
    """ロギングの設定"""
    os.makedirs(log_dir, exist_ok=True)

    level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'

    logger = logging.getLogger(log_name or __name__)
    logger.setLevel(level)
    
    # ハンドラが既に存在する場合は削除
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 標準出力ハンドラ
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)

    # ファイルハンドラを追加
    if log_name:
        file_name = f"{log_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    else:
        file_name = f"geos_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
    file_handler = logging.FileHandler(os.path.join(log_dir, file_name))
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)

    return logger

# メモリ使用状況取得関数
def get_memory_usage() -> Dict[str, float]:
    """現在のメモリ使用率を取得する"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_usage_bytes = memory_info.rss  # Resident Set Size

    # システム全体のメモリ情報
    system_memory = psutil.virtual_memory()
    memory_percent = process.memory_percent()

    return {
        'usage_bytes': memory_usage_bytes,
        'usage_mb': memory_usage_bytes / (1024 * 1024),
        'percent': memory_percent,
        'system_percent': system_memory.percent,
        'system_available_mb': system_memory.available / (1024 * 1024)
    }

def log_memory_usage(logger: logging.Logger, prefix: str = "") -> None:
    """メモリ使用状況をログに出力する"""
    memory = get_memory_usage()
    logger.info(f"{prefix}Memory usage: {memory['usage_mb']:.1f} MB ({memory['percent']:.1f}% of system), "
                f"System: {memory['system_percent']:.1f}% used, {memory['system_available_mb']:.1f} MB available")

# ジオメトリ修正関連関数
def is_valid_geometry(geom) -> bool:
    """ジオメトリが有効かどうかを確認する"""
    return geom.is_valid

def validate_and_fix_geometry(geom) -> Tuple[Any, str]:
    """GEOSを使用してジオメトリを検証し修正する"""
    if is_valid_geometry(geom):
        return geom, "Already valid"

    # ステップ1: makeValidを使用（GEOS 3.8以上で利用可能）
    try:
        fixed_geom = make_valid(geom)
        if is_valid_geometry(fixed_geom):
            return fixed_geom, "Fixed with make_valid"
    except Exception as e:
        logging.warning(f"make_valid failed: {e}")

    # ステップ2: バッファを0にしてジオメトリを修正
    try:
        buffered_geom = geom.buffer(0)
        if is_valid_geometry(buffered_geom):
            return buffered_geom, "Fixed with buffer(0)"
    except Exception as e:
        logging.warning(f"buffer(0) failed: {e}")

    # ステップ3: 小さなバッファを適用してから元に戻す
    try:
        buffer_out = geom.buffer(0.0000001)
        buffer_in = buffer_out.buffer(-0.0000001)
        if is_valid_geometry(buffer_in):
            return buffer_in, "Fixed with double buffer"
    except Exception as e:
        logging.warning(f"double buffer failed: {e}")

    # ステップ4: 単純化を試みる
    try:
        simplified = geom.simplify(0.0000001, preserve_topology=True)
        if is_valid_geometry(simplified):
            return simplified, "Fixed with simplify"
    except Exception as e:
        logging.warning(f"simplify failed: {e}")

    # ステップ5: エンベロープ（境界ボックス）を使用（最後の手段）
    try:
        envelope = geom.envelope
        if is_valid_geometry(envelope):
            return envelope, "Fixed with envelope (fallback)"
    except Exception as e:
        logging.warning(f"envelope failed: {e}")

    # 修正できなかった場合、元のジオメトリを返す
    return geom, "Unable to fix"

def is_large_integer_field(field_name: str) -> bool:
    """大きな整数値を持つフィールドかどうかを判断する"""
    # 大きな整数を持つ可能性のあるフィールド名のリスト
    large_integer_fields = [
        '除外車両コード', '対象車両コード'
    ]
    return any(field in field_name for field in large_integer_fields)

def adjust_schema_for_large_integers(original_schema: Dict) -> Dict:
    """大きな整数を持つフィールドのスキーマを調整する"""
    adjusted_schema = original_schema.copy()
    # プロパティの型を調整
    properties = adjusted_schema['properties']
    for field_name, field_type in properties.items():
        if is_large_integer_field(field_name) and field_type == 'int':
            # 大きな整数を文字列として扱う
            properties[field_name] = 'str'
    return adjusted_schema

# メインのジオメトリ修正関数
def process_geojson(input_file: str, 
                    output_file: str, 
                    log_file: str, 
                    chunk_size: int = 1000, 
                    memory_limit: float = 80.0) -> Tuple[int, int, int, int, Dict[str, int], int]:
    """
    GeoJSONファイルを処理し、有効なジオメトリを持つファイルを出力する

    Args:
        input_file: 入力GeoJSONファイルパス
        output_file: 出力GeoJSONファイルパス
        log_file: ログファイルパス
        chunk_size: 一度に処理するフィーチャの数
        memory_limit: メモリ使用率の上限 (%)

    Returns:
        Tuple[int, int, int, int, Dict[str, int], int]: 
        (総フィーチャ数, 無効フィーチャ数, 修正済フィーチャ数, 修正不能フィーチャ数, 修正方法統計, スキップフィーチャ数)
    """
    file_logger = logging.getLogger(os.path.basename(input_file))
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    file_logger.addHandler(file_handler)
    file_logger.setLevel(logging.INFO)

    file_logger.info(f"Processing file: {input_file}")
    file_logger.info(f"GEOS Version: {shapely.geos.geos_version}")
    log_memory_usage(file_logger, "Initial ")

    # 集計変数の初期化
    total_features = 0
    invalid_features = 0
    fixed_features = 0
    unfixable_features = 0
    skipped_features = 0
    fix_methods = {}

    try:
        # ファイル情報の取得
        with fiona.open(input_file, 'r') as source:
            file_logger.info(f"File contains {len(source)} features")
            original_schema = source.schema
            adjusted_schema = adjust_schema_for_large_integers(original_schema)
            crs = source.crs

        # 出力ディレクトリ作成
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # 処理メインループ
        with fiona.open(input_file, 'r') as source:
            with fiona.open(output_file, 'w',
                          driver='GeoJSON',
                          crs=crs,
                          schema=adjusted_schema) as sink:

                feature_buffer = []
                chunk_counter = 0
                gc_trigger = 5  # GCを実行する間隔

                # 各フィーチャの処理
                for feature in source:
                    total_features += 1
                    feature_id = feature.get('id', total_features)

                    try:
                        # 大きな整数フィールドの処理
                        for field_name, field_value in feature['properties'].items():
                            if is_large_integer_field(field_name) and field_value is not None:
                                if isinstance(field_value, (int, float)) and field_value > 2147483647:
                                    feature['properties'][field_name] = str(field_value)

                        # ジオメトリの検証と修正
                        geom = shape(feature['geometry'])
                        if not is_valid_geometry(geom):
                            invalid_features += 1
                            file_logger.info(f"Feature {feature_id} is invalid")

                            fixed_geom, method = validate_and_fix_geometry(geom)
                            fix_methods[method] = fix_methods.get(method, 0) + 1

                            if is_valid_geometry(fixed_geom):
                                fixed_features += 1
                                file_logger.info(f"Feature {feature_id} fixed using {method}")
                                feature['geometry'] = mapping(fixed_geom)
                            else:
                                unfixable_features += 1
                                file_logger.warning(f"Feature {feature_id} could not be fixed")

                        # バッファに追加
                        feature_buffer.append(feature)

                        # メモリ使用率チェック
                        if total_features % 100 == 0:
                            memory = get_memory_usage()
                            if memory['percent'] > memory_limit:
                                file_logger.warning(f"Memory usage high ({memory['percent']:.1f}%), writing buffer early")
                                for f in feature_buffer:
                                    sink.write(f)
                                feature_buffer = []
                                gc.collect()

                        # バッファがチャンクサイズに達したら書き込み
                        if len(feature_buffer) >= chunk_size:
                            for f in feature_buffer:
                                sink.write(f)
                            file_logger.info(f"Processed chunk of {len(feature_buffer)} features. Total: {total_features}")
                            feature_buffer = []

                            # 定期的なGC実行
                            chunk_counter += 1
                            if chunk_counter % gc_trigger == 0:
                                gc.collect()
                                log_memory_usage(file_logger)

                    except Exception as e:
                        file_logger.error(f"Error processing feature {feature_id}: {e}")
                        skipped_features += 1

                # 残りのフィーチャを書き込み
                for f in feature_buffer:
                    sink.write(f)

        # 結果のログ出力
        file_logger.info(f"Processing completed for {input_file}")
        file_logger.info(f"Total features: {total_features}")
        file_logger.info(f"Invalid features: {invalid_features}")
        file_logger.info(f"Successfully fixed: {fixed_features}")
        file_logger.info(f"Unfixable features: {unfixable_features}")
        file_logger.info(f"Skipped features: {skipped_features}")
        file_logger.info(f"Fix methods used: {fix_methods}")

        return total_features, invalid_features, fixed_features, unfixable_features, fix_methods, skipped_features

    except Exception as e:
        file_logger.error(f"Failed to process {input_file}: {e}")
        return 0, 0, 0, 0, {}, 0
    finally:
        # クリーンアップ
        gc.collect()
        for handler in file_logger.handlers[:]:
            handler.close()
            file_logger.removeHandler(handler)

# 複数ファイル処理関数
def fix_geojson_files(input_dir: str, 
                     output_dir: str, 
                     log_dir: str, 
                     pattern: str = "*.geojson", 
                     recursive: bool = False, 
                     chunk_size: int = 1000, 
                     memory_limit: float = 80.0, 
                     verbose: bool = False) -> List[Dict]:
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

    Returns:
        List[Dict]: 処理結果のリスト
    """
    # ロガーの設定
    logger = setup_logging(log_dir, "batch_fix", verbose)
    logger.info(f"Starting batch processing of GeoJSON files")
    logger.info(f"GEOS Version: {shapely.geos.geos_version}")
    
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
    
    # 処理開始時間
    start_time = time.time()
    
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
        total, invalid, fixed, unfixable, methods, skipped = process_geojson(
            str(file_path),
            str(output_path),
            str(log_path),
            chunk_size=chunk_size,
            memory_limit=memory_limit
        )
        
        # 結果を保存
        file_result = {
            "file": str(rel_path),
            "total_features": total,
            "invalid_features": invalid,
            "fixed_features": fixed,
            "unfixable_features": unfixable,
            "skipped_features": skipped,
            "fix_methods": methods
        }
        results.append(file_result)
        
        # 進捗状況と予測残り時間
        elapsed = time.time() - start_time
        avg_time = elapsed / i
        remaining = avg_time * (len(files) - i)
        
        logger.info(f"Progress: {i}/{len(files)} ({i/len(files)*100:.1f}%) - ETA: {remaining/60:.1f} min")
        
        # メモリ状況をログ
        log_memory_usage(logger)
    
    # 処理完了情報
    total_elapsed = time.time() - start_time
    logger.info(f"All files processed in {total_elapsed/60:.1f} minutes")
    
    # 処理結果のサマリーを作成
    create_summary_report(results, input_dir, output_dir, log_dir)
    
    return results

# サマリーレポート作成関数
def create_summary_report(results: List[Dict], input_dir: str, output_dir: str, log_dir: str) -> Tuple[str, str]:
    """処理結果のサマリーレポートを作成"""
    logger = logging.getLogger(__name__)

    # 統計集計
    total_files = len(results)
    total_features = sum(r["total_features"] for r in results)
    total_invalid = sum(r["invalid_features"] for r in results)
    total_fixed = sum(r["fixed_features"] for r in results)
    total_unfixable = sum(r["unfixable_features"] for r in results)
    total_skipped = sum(r.get("skipped_features", 0) for r in results)

    # 修正メソッドの集計
    all_methods = {}
    for r in results:
        for method, count in r.get("fix_methods", {}).items():
            all_methods[method] = all_methods.get(method, 0) + count

    # JSONサマリー保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = os.path.join(log_dir, f"summary_{timestamp}.json")

    summary = {
        "timestamp": timestamp,
        "geos_version": shapely.geos.geos_version_string,
        "input_directory": str(input_dir),
        "output_directory": str(output_dir),
        "total_files": total_files,
        "total_features": total_features,
        "total_invalid_features": total_invalid,
        "total_fixed_features": total_fixed,
        "total_unfixable_features": total_unfixable,
        "total_skipped_features": total_skipped,
        "fix_success_rate": (total_fixed / total_invalid * 100) if total_invalid > 0 else 100,
        "memory_info": get_memory_usage(),
        "fix_methods": all_methods,
        "file_results": results
    }

    os.makedirs(os.path.dirname(summary_file), exist_ok=True)
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # テキストサマリー保存
    text_summary_file = os.path.join(log_dir, f"summary_{timestamp}.txt")

    with open(text_summary_file, 'w', encoding='utf-8') as f:
        f.write("GeoJSON Geometry Repair Summary\n")
        f.write("=============================\n\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"GEOS Version: {shapely.geos.geos_version_string}\n\n")

        f.write("Overall Statistics:\n")
        f.write(f"  Input Directory: {input_dir}\n")
        f.write(f"  Output Directory: {output_dir}\n")
        f.write(f"  Total Files Processed: {total_files}\n")
        f.write(f"  Total Features: {total_features}\n")
        f.write(f"  Invalid Features: {total_invalid}\n")
        f.write(f"  Successfully Fixed: {total_fixed}\n")
        f.write(f"  Unfixable Features: {total_unfixable}\n")
        f.write(f"  Features with Special Handling: {total_skipped}\n")

        if total_invalid > 0:
            f.write(f"  Success Rate: {total_fixed / total_invalid * 100:.2f}%\n\n")
        else:
            f.write("  Success Rate: 100% (no invalid features)\n\n")

        f.write("Fix Methods Used:\n")
        for method, count in sorted(all_methods.items(), key=lambda x: x[1], reverse=True):
            if total_invalid > 0:
                percentage = count / total_invalid * 100
                f.write(f"  {method}: {count} ({percentage:.2f}%)\n")
            else:
                f.write(f"  {method}: {count}\n")

        f.write("\nFiles with Unfixable Geometries:\n")
        for r in sorted(results, key=lambda x: x["unfixable_features"], reverse=True):
            if r["unfixable_features"] > 0:
                f.write(f"  {r['file']}: {r['unfixable_features']} unfixable out of {r['invalid_features']} invalid\n")

    logger.info(f"Summary reports created:\n  JSON: {summary_file}\n  Text: {text_summary_file}")

    return summary_file, text_summary_file
