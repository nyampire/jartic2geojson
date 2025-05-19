"""
jartic2geojson/postprocess/reporting.py - 処理結果のレポート生成機能
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any
import shapely.geos

from .memory_manager import MemoryManager

def create_summary_report(results: List[Dict], 
                         input_dir: str, 
                         output_dir: str, 
                         log_dir: str) -> Tuple[str, str]:
    """
    処理結果のサマリーレポートを作成
    
    Args:
        results: 処理結果のリスト
        input_dir: 入力ディレクトリパス
        output_dir: 出力ディレクトリパス
        log_dir: ログディレクトリパス
        
    Returns:
        Tuple[str, str]: (JSONレポートパス, テキストレポートパス)
    """
    logger = logging.getLogger(__name__)

    # 統計集計
    total_files = len(results)
    total_features = sum(r.get("total_features", 0) for r in results)
    total_invalid = sum(r.get("invalid_features", 0) for r in results)
    total_fixed = sum(r.get("fixed_features", 0) for r in results)
    total_unfixable = sum(r.get("unfixable_features", 0) for r in results)
    total_skipped = sum(r.get("skipped_features", 0) for r in results)
    
    # エラーのあったファイル数
    error_files = sum(1 for r in results if "error" in r)

    # 修正メソッドの集計
    all_methods = {}
    for r in results:
        for method, count in r.get("fix_methods", {}).items():
            all_methods[method] = all_methods.get(method, 0) + count

    # 地域別集計（ファイルパスの最初の部分から推測）
    region_stats = {}
    for r in results:
        file_path = r.get("file", "")
        parts = file_path.split(os.sep)
        region = parts[0] if parts else "unknown"
        
        if region not in region_stats:
            region_stats[region] = {
                "files": 0,
                "total_features": 0,
                "invalid_features": 0,
                "fixed_features": 0,
                "unfixable_features": 0,
                "skipped_features": 0,
                "error_files": 0
            }
            
        stats = region_stats[region]
        stats["files"] += 1
        stats["total_features"] += r.get("total_features", 0)
        stats["invalid_features"] += r.get("invalid_features", 0)
        stats["fixed_features"] += r.get("fixed_features", 0)
        stats["unfixable_features"] += r.get("unfixable_features", 0)
        stats["skipped_features"] += r.get("skipped_features", 0)
        stats["error_files"] += 1 if "error" in r else 0

    # JSONサマリー保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = os.path.join(log_dir, f"summary_{timestamp}.json")
    
    # 出力ディレクトリを確保
    os.makedirs(os.path.dirname(summary_file), exist_ok=True)

    # 成功率の計算
    success_rate = (total_fixed / total_invalid * 100) if total_invalid > 0 else 100.0

    # サマリー情報を構築
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
        "error_files": error_files,
        "fix_success_rate": success_rate,
        "memory_info": MemoryManager.get_memory_usage(),
        "fix_methods": all_methods,
        "region_statistics": region_stats,
        "file_results": results
    }

    # JSONファイルに書き出し
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # テキストサマリー保存
    text_summary_file = os.path.join(log_dir, f"summary_{timestamp}.txt")

    with open(text_summary_file, 'w', encoding='utf-8') as f:
        # ヘッダー
        f.write("GeoJSON Geometry Repair Summary\n")
        f.write("=============================\n\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"GEOS Version: {shapely.geos.geos_version_string}\n\n")

        # 全体統計
        f.write("Overall Statistics:\n")
        f.write(f"  Input Directory: {input_dir}\n")
        f.write(f"  Output Directory: {output_dir}\n")
        f.write(f"  Total Files Processed: {total_files}\n")
        f.write(f"  Files with Errors: {error_files}\n")
        f.write(f"  Total Features: {total_features}\n")
        f.write(f"  Invalid Features: {total_invalid}\n")
        f.write(f"  Successfully Fixed: {total_fixed}\n")
        f.write(f"  Unfixable Features: {total_unfixable}\n")
        f.write(f"  Features with Special Handling: {total_skipped}\n")

        # 成功率
        if total_invalid > 0:
            f.write(f"  Success Rate: {success_rate:.2f}%\n\n")
        else:
            f.write("  Success Rate: 100% (no invalid features)\n\n")

        # 修正方法の統計
        f.write("Fix Methods Used:\n")
        for method, count in sorted(all_methods.items(), key=lambda x: x[1], reverse=True):
            if total_invalid > 0:
                method_percent = count / total_invalid * 100
                f.write(f"  {method}: {count} ({method_percent:.2f}%)\n")
            else:
                f.write(f"  {method}: {count}\n")

        # 地域別統計
        f.write("\nRegion Statistics:\n")
        for region, stats in sorted(region_stats.items()):
            invalid = stats["invalid_features"]
            fixed = stats["fixed_features"]
            success_rate = (fixed / invalid * 100) if invalid > 0 else 100.0

            f.write(f"  {region}:\n")
            f.write(f"    Files: {stats['files']}\n")
            f.write(f"    Features: {stats['total_features']}\n")
            f.write(f"    Invalid: {invalid}\n")
            f.write(f"    Fixed: {fixed}\n")
            f.write(f"    Unfixable: {stats['unfixable_features']}\n")
            f.write(f"    Special Handling: {stats['skipped_features']}\n")
            f.write(f"    Error Files: {stats['error_files']}\n")
            f.write(f"    Success Rate: {success_rate:.2f}%\n\n")

        # 修正できなかったフィーチャがあるファイル
        f.write("\nFiles with Unfixable Geometries:\n")
        unfixable_files = [r for r in results if r.get("unfixable_features", 0) > 0]
        unfixable_files.sort(key=lambda x: x.get("unfixable_features", 0), reverse=True)
        
        for r in unfixable_files:
            f.write(f"  {r['file']}: {r['unfixable_features']} unfixable out of {r['invalid_features']} invalid\n")
            
        # エラーが発生したファイル
        f.write("\nFiles with Errors:\n")
        for r in results:
            if "error" in r:
                f.write(f"  {r['file']}: {r['error']}\n")

    logger.info(f"Summary reports created:\n  JSON: {summary_file}\n  Text: {text_summary_file}")

    return summary_file, text_summary_file

def generate_file_report(stats: Dict[str, Any], 
                        input_file: str, 
                        output_file: str, 
                        log_file: str) -> str:
    """
    単一ファイルの処理結果レポートを生成
    
    Args:
        stats: 処理統計情報
        input_file: 入力ファイルパス
        output_file: 出力ファイルパス
        log_file: ログファイルパス
        
    Returns:
        str: レポートファイルパス
    """
    report_file = os.path.splitext(log_file)[0] + "_report.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"GeoJSON Geometry Repair Report for {os.path.basename(input_file)}\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Input file: {input_file}\n")
        f.write(f"Output file: {output_file}\n")
        f.write(f"Log file: {log_file}\n\n")
        
        f.write("Processing Statistics:\n")
        f.write(f"  Total Features: {stats.get('total_features', 0)}\n")
        f.write(f"  Invalid Features: {stats.get('invalid_features', 0)}\n")
        f.write(f"  Successfully Fixed: {stats.get('fixed_features', 0)}\n")
        f.write(f"  Unfixable Features: {stats.get('unfixable_features', 0)}\n")
        f.write(f"  Skipped Features: {stats.get('skipped_features', 0)}\n")
        
        # 成功率の計算
        invalid = stats.get('invalid_features', 0)
        fixed = stats.get('fixed_features', 0)
        if invalid > 0:
            success_rate = fixed / invalid * 100
            f.write(f"  Success Rate: {success_rate:.2f}%\n\n")
        else:
            f.write("  Success Rate: 100% (no invalid features)\n\n")
        
        # 修正方法の統計
        f.write("Fix Methods Used:\n")
        for method, count in sorted(stats.get('fix_methods', {}).items(), key=lambda x: x[1], reverse=True):
            if invalid > 0:
                method_percent = count / invalid * 100
                f.write(f"  {method}: {count} ({method_percent:.2f}%)\n")
            else:
                f.write(f"  {method}: {count}\n")
        
        # エラーがあれば表示
        if 'error' in stats:
            f.write("\nErrors:\n")
            f.write(f"  {stats['error']}\n")
    
    return report_file
