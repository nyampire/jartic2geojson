#!/bin/bash

# 入力ディレクトリと出力ディレクトリを定義
INPUT_DIR="./data"
OUTPUT_BASE_DIR="./output_data"
FIXED_OUTPUT_DIR="./fixed_geojson"
GEOMETRY_LOG_DIR="./geometry_logs"

# 引数の解析
USE_FIX_FLAG=false
USE_SEPARATE_FIX=false
THREADS=1

while getopts "fst:h" opt; do
  case $opt in
    f) USE_FIX_FLAG=true ;;    # convert_jartic.pyの--fix-geometryフラグを使用
    s) USE_SEPARATE_FIX=true ;; # 別途repair_geometries.pyを実行
    t) THREADS=$OPTARG ;;      # 並列スレッド数
    h)
      echo "使用方法: $0 [-f] [-s] [-t threads] [-h]"
      echo "  -f: convert_jartic.pyの--fix-geometryフラグを使用"
      echo "  -s: 別途repair_geometries.pyを実行"
      echo "  -t threads: ジオメトリ修正の並列スレッド数 (デフォルト: 1)"
      echo "  -h: このヘルプを表示"
      exit 0
      ;;
    \?)
      echo "無効なオプション: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

# 必要なディレクトリを作成
mkdir -p "$OUTPUT_BASE_DIR"
mkdir -p "$FIXED_OUTPUT_DIR"
mkdir -p "$GEOMETRY_LOG_DIR"

# ステップ1: 全CSVファイルを処理してGeoJSONに変換
echo "===== ステップ1: CSVからGeoJSONへの変換 ====="
for csv_file in ${INPUT_DIR}/*.csv; do
    # ファイル名（拡張子なし）を取得
    filename=$(basename -- "$csv_file")
    basename="${filename%.*}"

    # 出力ディレクトリを作成
    output_dir="${OUTPUT_BASE_DIR}/${basename}"
    mkdir -p "$output_dir"

    echo "Processing: $csv_file -> $output_dir"

    # convert_jartic.pyコマンドの基本部分
    cmd="python convert_jartic.py \"$csv_file\" --split_by_regulation -o \"$output_dir\""

    # --fix-geometryフラグを追加（有効な場合）
    if $USE_FIX_FLAG; then
        cmd="$cmd --fix-geometry"
    fi

    # コマンドを実行
    eval $cmd

    # 処理結果のチェック
    if [ $? -eq 0 ]; then
        echo "✓ Successfully processed: $basename"
    else
        echo "✗ Failed to process: $basename"
    fi
done

# ステップ2: 必要に応じて別々にジオメトリ修正を実行
if $USE_SEPARATE_FIX; then
    echo -e "\n===== ステップ2: GeoJSONのジオメトリ修正 ====="

    # repair_geometries.pyを実行
    echo "Running repair_geometries.py on $OUTPUT_BASE_DIR"
    python repair_geometries.py -i "$OUTPUT_BASE_DIR" -o "$FIXED_OUTPUT_DIR" -l "$GEOMETRY_LOG_DIR" -r --threads $THREADS

    # 処理結果のチェック
    if [ $? -eq 0 ]; then
        echo "✓ Successfully fixed GeoJSON geometries"
    else
        echo "✗ Failed to fix GeoJSON geometries"
    fi
fi

echo -e "\nAll processing completed"
echo "Original GeoJSON files: $OUTPUT_BASE_DIR"

# ジオメトリ修正が実行された場合は出力ディレクトリを表示
if $USE_FIX_FLAG; then
    echo "Fixed GeoJSON files (internal): $OUTPUT_BASE_DIR/fixed"
    echo "Geometry logs (internal): $OUTPUT_BASE_DIR/logs"
fi

if $USE_SEPARATE_FIX; then
    echo "Fixed GeoJSON files (separate): $FIXED_OUTPUT_DIR"
    echo "Geometry logs (separate): $GEOMETRY_LOG_DIR"
fi
