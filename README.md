# JARTIC2GeoJSON

交通規制情報CSVファイルをGeoJSONに変換し、ジオメトリ修正機能を提供するツールセット

## 概要

このプロジェクトは、JARTICが公開する[交通規制情報](https://www.jartic.or.jp/service/opendata/)のCSVオープンデータを、地理情報システムで利用しやすいGeoJSON形式に変換します。さらに、GeoJSONファイルに含まれる無効なジオメトリを検出し修正する機能も提供します。一方通行などの特殊なデータの方向性情報を正確に保持する機能も備えています。

利用する交通規制情報データの仕様については、[交通規制情報（拡張版標準フォーマット）説明書 Ver: k_2.1](https://www.jartic.or.jp/d/opendata/typeD_kisei_73_k_2.1.pdf)を参照しています。

## 特徴

- **変換機能**
  - 交通規制データに特化したGeoJSON変換
  - 一方通行データの座標順序の正確な処理
  - 複数のエンコーディングに対応したCSV読み込み
  - 自動列検出機能
  - 規制種別ごとのファイル分割オプション
  - ポリゴンデータの交差処理のカスタマイズ

- **ジオメトリ修正機能**
  - 無効なジオメトリの自動検出と修正
  - 複数の修正手法の適用（make_valid, buffer, simplify など）
  - 大規模ファイルのチャンク処理
  - メモリ使用量の最適化
  - 並列処理による高速化
  - 詳細なレポート生成

## インストール

### 前提条件

- Python 3.7以上
- 以下のPythonパッケージ:
  - geopandas
  - shapely
  - pandas
  - numpy
  - scipy
  - fiona (ジオメトリ修正機能用)
  - psutil (ジオメトリ修正機能用)

### 方法1: パッケージとしてインストール

```bash
git clone https://github.com/yourusername/jartic2geojson.git
cd jartic2geojson
pip install -e .
```

これにより `jartic2geojson` コマンドがシステムに登録されます。

### 方法2: インストールせずに直接実行

```bash
git clone https://github.com/yourusername/jartic2geojson.git
cd jartic2geojson
pip install -r requirements.txt  # 必要な依存パッケージをインストール
chmod +x convert_jartic.py       # Linuxの場合、実行権限を付与
chmod +x repair_geometries.py    # Linuxの場合、実行権限を付与
chmod +x batch_convert.sh        # Linuxの場合、実行権限を付与
```

## ディレクトリ構造

```
jartic2geojson/
├── convert_jartic.py        # CSVからGeoJSONへの変換スクリプト
├── repair_geometries.py     # ジオメトリ修正ツール
├── batch_convert.sh         # バッチ処理用シェルスクリプト
├── jartic2geojson/
│   ├── __init__.py
│   ├── cli.py               # CLIエントリポイント
│   ├── config.py            # 設定管理
│   ├── core/
│   │   ├── __init__.py
│   │   └── converter.py     # 変換メイン処理
│   ├── postprocess/         # ジオメトリ修正モジュール
│   │   ├── __init__.py
│   │   ├── core.py          # 修正アルゴリズム
│   │   ├── file_processor.py # ファイル処理
│   │   ├── memory_manager.py # メモリ管理
│   │   ├── logging_utils.py  # ロギング
│   │   └── reporting.py      # レポート生成
│   └── utils/
│       ├── __init__.py
│       ├── column_detector.py   # 列検出
│       ├── coordinate_utils.py  # 座標処理
│       ├── file_handler.py      # ファイル処理
│       └── geometry_processor.py # ジオメトリ処理
├── data/                     # 入力CSVファイル
├── output_data/              # 変換後のGeoJSONファイル
├── fixed_geojson/            # 修正後のGeoJSONファイル
└── geometry_logs/            # ジオメトリ修正ログ
```

## 使用方法

### 1. CSVからGeoJSONへの変換

#### コマンドライン

```bash
python convert_jartic.py ./data/交通規制データ.csv -o ./output_data/出力先 -s
```

オプション:
- `-s, --split_by_regulation`: 共通規制種別コード別にファイルを分割
- `-d, --debug`: デバッグモードを有効化（詳細情報を表示）
- `-m {convex_hull,fix_intersections}`: 交差ラインの処理方法を選択
- `-p, --preserve_oneway_order`: 一方通行の座標順序を厳密に保持（デフォルト: True）
- `-f, --fix-geometry`: 変換後にジオメトリ修正を実行

#### Pythonからの利用

```python
from jartic2geojson.core.converter import convert_csv_to_geojson

output_files = convert_csv_to_geojson(
    input_file="input.csv",
    output_dir="output",
    method="convex_hull",
    debug=False,
    split_by_regulation=True,
    preserve_oneway_order=True
)

print(f"出力ファイル: {output_files}")
```

### 2. ジオメトリ修正

#### コマンドライン

```bash
python repair_geometries.py -i ./output_data -o ./fixed_geojson -l ./geometry_logs -r
```

オプション:
- `-i, --input`: 入力ディレクトリ
- `-o, --output`: 出力ディレクトリ
- `-l, --log`: ログディレクトリ
- `-r, --recursive`: サブディレクトリを再帰的に処理
- `-v, --verbose`: 詳細なログを出力
- `--threads`: 並列処理のスレッド数（デフォルト: 1、0=CPUコア数）
- `--chunk-size`: 一度に処理するフィーチャの数（デフォルト: 1000）
- `--memory-limit`: メモリ使用率の上限（%、デフォルト: 80.0）

#### Pythonからの利用

```python
from jartic2geojson.postprocess import fix_geojson_files

results = fix_geojson_files(
    input_dir="./output_data",
    output_dir="./fixed_geojson",
    log_dir="./geometry_logs",
    recursive=True,
    chunk_size=1000,
    memory_limit=80.0,
    verbose=False
)
```

### 3. バッチ処理

複数のCSVファイルを一括処理するシェルスクリプトを提供しています。

```bash
# 実行権限を付与
chmod +x batch_convert.sh

# 基本的な変換（ジオメトリ修正なし）
./batch_convert.sh

# convert_jartic.pyの組み込み修正機能を使用
./batch_convert.sh -f

# 別途repair_geometries.pyを使用（並列処理4スレッド）
./batch_convert.sh -s -t 4

# 両方の修正方法を実行（比較のために）
./batch_convert.sh -f -s -t 4
```

バッチ処理オプション:
- `-f`: `convert_jartic.py`の`--fix-geometry`フラグを使用（内部修正）
- `-s`: 別途`repair_geometries.py`を実行（外部修正）
- `-t <threads>`: ジオメトリ修正の並列スレッド数
- `-h`: ヘルプを表示

## 入力CSVの形式

プログラムは以下のカラムを自動検出します:

1. 緯度経度カラム: `規制場所の経度緯度`, `規制場所`, `経度緯度`, `緯度経度`, `coordinates` のいずれかの名前を含むカラム
2. ユニークキーカラム: `ユニークキー`, `ID`, `id`, `Key`, `key` のいずれかの名前を含むカラム
3. 点・線・面コードカラム: `点・線・面コード`, `点線面コード` のいずれかの名前を含むカラム
4. 共通規制種別コードカラム: `共通規制種別コード` の名前を含むカラム
5. 指定・禁止方向の別コードカラム: `指定・禁止方向の別コード`, `指定禁止方向別コード`, `方向コード` のいずれかの名前を含むカラム

## 一方通行データの処理

一方通行データ（共通規制種別コード=11）は特別な処理が適用されます:

- 方向コード=1（禁止）の場合: 座標順序は「進入禁止地点→一方通行開始地点」（禁止方向）を示します。
- 方向コード=2（指定）の場合: 元の座標順序は「一方通行開始地点→進入禁止地点」（通行可能方向）ですが、処理時に逆順にして禁止方向に統一されます。

## ジオメトリ修正の方法

無効なジオメトリを修正するために、以下の方法が順番に適用されます：

1. `make_valid`: GEOS 3.8以上で利用可能なmakeValid関数を使用
2. `buffer(0)`: バッファを0にしてジオメトリを修正
3. `double buffer`: 小さなバッファを適用してから元に戻す
4. `simplify`: 単純化を適用
5. `envelope`: 最後の手段として、ジオメトリのエンベロープ（境界ボックス）を使用

## パフォーマンスとメモリ管理

- 大規模なGeoJSONファイルを処理する場合は、`--chunk-size`と`--memory-limit`オプションを調整
- 複数のファイルを処理する場合は、`--threads`オプションで並列処理を有効化
- メモリ使用率が高くなる場合は、バッチサイズを小さくして処理

## ライセンス

MIT

## 貢献

プルリクエストを歓迎します。大きな変更を行う場合は、まず問題を開いて、変更したい内容について議論してください。

## 生成AIの利用

このスクリプトは、Claude Sonnet 3.7を利用して生成されています。
