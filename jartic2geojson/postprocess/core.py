"""
jartic2geojson/postprocess/core.py - ジオメトリ修正の核となる機能
"""

from typing import Tuple, Any, Dict, List
from shapely.geometry import shape, mapping
from shapely.validation import make_valid
import logging

def is_valid_geometry(geom) -> bool:
    """
    ジオメトリが有効かどうかを確認する
    
    Args:
        geom: 検証するジオメトリオブジェクト
        
    Returns:
        bool: 有効な場合はTrue
    """
    return geom.is_valid

def fix_geometry(geom) -> Tuple[Any, str]:
    """
    GEOSを使用してジオメトリを検証し修正する
    
    Args:
        geom: 修正対象のジオメトリ
        
    Returns:
        Tuple[Any, str]: (修正されたジオメトリ, 使用された修正方法)
    """
    if is_valid_geometry(geom):
        return geom, "Already valid"

    # 修正方法のリスト - 順に試行する
    methods = [
        # 関数オブジェクト, メソッド名
        (lambda g: make_valid(g), "make_valid"),
        (lambda g: g.buffer(0), "buffer(0)"),
        (lambda g: g.buffer(0.0000001).buffer(-0.0000001), "double buffer"),
        (lambda g: g.simplify(0.0000001, preserve_topology=True), "simplify"),
        (lambda g: g.envelope, "envelope (fallback)")
    ]
    
    # 各修正方法を順に試行
    logger = logging.getLogger(__name__)
    
    for fix_func, method_name in methods:
        try:
            fixed_geom = fix_func(geom)
            if is_valid_geometry(fixed_geom):
                return fixed_geom, f"Fixed with {method_name}"
        except Exception as e:
            logger.debug(f"Method {method_name} failed: {e}")
            continue

    # 修正できなかった場合、元のジオメトリを返す
    return geom, "Unable to fix"

def is_large_integer_field(field_name: str) -> bool:
    """
    大きな整数値を持つフィールドかどうかを判断する
    
    Args:
        field_name: フィールド名
        
    Returns:
        bool: 大きな整数フィールドの場合はTrue
    """
    # 大きな整数を持つ可能性のあるフィールド名のリスト
    large_integer_fields = [
        '除外車両コード', '対象車両コード'
    ]
    return any(field in field_name for field in large_integer_fields)

def adjust_schema_for_large_integers(original_schema: Dict) -> Dict:
    """
    大きな整数を持つフィールドのスキーマを調整する
    
    Args:
        original_schema: オリジナルのスキーマ
        
    Returns:
        Dict: 調整後のスキーマ
    """
    adjusted_schema = original_schema.copy()
    
    # プロパティの型を調整
    properties = adjusted_schema['properties']
    for field_name, field_type in properties.items():
        if is_large_integer_field(field_name) and field_type == 'int':
            # 大きな整数を文字列として扱う
            properties[field_name] = 'str'
            
    return adjusted_schema

def process_feature(feature: Dict, logger=None) -> Tuple[Dict, Dict]:
    """
    単一のGeoJSONフィーチャを処理する
    
    Args:
        feature: 処理対象のGeoJSONフィーチャ
        logger: ロガーオブジェクト
        
    Returns:
        Tuple[Dict, Dict]: (処理後のフィーチャ, 処理メタデータ)
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    feature_id = feature.get('id', 'unknown')
    
    # 結果メタデータの初期化
    metadata = {
        "is_valid": True,
        "fixed": False,
        "fix_method": None,
        "skipped": False
    }
    
    try:
        # 大きな整数フィールドの処理
        for field_name, field_value in feature['properties'].items():
            if is_large_integer_field(field_name) and field_value is not None:
                if isinstance(field_value, (int, float)) and field_value > 2147483647:
                    feature['properties'][field_name] = str(field_value)
                    logger.debug(f"Converted large integer field {field_name}: {field_value} to string")

        # ジオメトリの検証と修正
        geom = shape(feature['geometry'])
        
        if not is_valid_geometry(geom):
            metadata["is_valid"] = False
            logger.info(f"Feature {feature_id} is invalid")

            fixed_geom, method = fix_geometry(geom)
            metadata["fix_method"] = method

            if is_valid_geometry(fixed_geom):
                metadata["fixed"] = True
                logger.info(f"Feature {feature_id} fixed using {method}")
                feature['geometry'] = mapping(fixed_geom)
            else:
                logger.warning(f"Feature {feature_id} could not be fixed")
                
    except Exception as e:
        logger.error(f"Error processing feature {feature_id}: {e}")
        metadata["skipped"] = True
        metadata["error"] = str(e)
        
    return feature, metadata

class GeometryFixingPipeline:
    """
    ジオメトリ修正パイプラインを表すクラス
    処理ステップを順次適用する
    """
    
    def __init__(self, logger=None):
        """初期化"""
        self.steps = []
        self.logger = logger or logging.getLogger(__name__)
        
    def add_step(self, step_func, name=None):
        """
        パイプラインに処理ステップを追加
        
        Args:
            step_func: 処理関数 (feature, metadata) -> (feature, metadata)
            name: ステップ名（デバッグ用）
        """
        self.steps.append((step_func, name or f"step_{len(self.steps)+1}"))
        return self
        
    def process(self, feature):
        """
        フィーチャに対してパイプラインを実行
        
        Args:
            feature: 処理対象のGeoJSONフィーチャ
            
        Returns:
            Tuple[Dict, Dict]: (処理後のフィーチャ, 処理メタデータ)
        """
        metadata = {}
        
        try:
            for step_func, step_name in self.steps:
                feature, step_metadata = step_func(feature)
                metadata[step_name] = step_metadata
                
                # エラーがあれば中断
                if step_metadata.get("error"):
                    self.logger.warning(f"Pipeline stopped at step {step_name}: {step_metadata['error']}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            metadata["error"] = str(e)
            
        return feature, metadata

# デフォルトパイプラインステップ
def create_default_pipeline(logger=None):
    """
    デフォルトのジオメトリ修正パイプラインを作成
    
    Args:
        logger: ロガーオブジェクト
        
    Returns:
        GeometryFixingPipeline: 設定済みパイプライン
    """
    pipeline = GeometryFixingPipeline(logger)
    
    # フィーチャ処理ステップを追加
    pipeline.add_step(
        lambda feature: process_feature(feature, logger),
        "geometry_fixing"
    )
    
    return pipeline
