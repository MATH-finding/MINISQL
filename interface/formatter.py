"""
查询结果格式化器
"""

from typing import Dict, Any, List


def format_query_result(result: Dict[str, Any]):
    """格式化并打印查询结果"""
    if not result.get("success", True):
        print(f"❌ 错误: {result.get('error', '未知错误')}")
        return

    result_type = result.get("type", "UNKNOWN")

    if result_type == "SELECT":
        _format_select_result(result)
    elif result_type in ["CREATE_TABLE", "INSERT"]:
        print(f"✅ {result.get('message', '操作成功')}")
    else:
        print(f"✅ {result.get('message', '操作完成')}")


def _format_select_result(result: Dict[str, Any]):
    """格式化SELECT查询结果"""
    data = result.get("data", [])

    if not data:
        print("查询结果为空")
        return

    # 获取列名
    columns = list(data[0].keys()) if data else []

    if not columns:
        print("没有数据可显示")
        return

    # 计算每列的最大宽度
    col_widths = {}
    for col in columns:
        # 列名宽度
        col_widths[col] = len(str(col))
        # 数据宽度
        for row in data:
            value_len = len(str(row.get(col, "")))
            col_widths[col] = max(col_widths[col], value_len)

    # 打印表头
    header = " | ".join(f"{col:<{col_widths[col]}}" for col in columns)
    print(header)
    print("-" * len(header))

    # 打印数据行
    for row in data:
        row_str = " | ".join(
            f"{str(row.get(col, '')):<{col_widths[col]}}" for col in columns
        )
        print(row_str)

    print(f"\n共 {len(data)} 行")


def format_table_info(table_info: Dict[str, Any]):
    """格式化表信息"""
    if "error" in table_info:
        print(f"❌ {table_info['error']}")
        return

    print(f"\n表: {table_info['table_name']}")
    print("=" * 50)

    print("列定义:")
    for col in table_info["columns"]:
        constraints = []
        if col["primary_key"]:
            constraints.append("PRIMARY KEY")
        if not col["nullable"]:
            constraints.append("NOT NULL")

        type_str = col["type"]
        if col["max_length"]:
            type_str += f"({col['max_length']})"

        constraint_str = " " + ", ".join(constraints) if constraints else ""
        print(f"  {col['name']:<20} {type_str:<15}{constraint_str}")

    print(f"\n记录数: {table_info['record_count']}")
    print(f"使用页面: {len(table_info['pages'])} 个")


def format_database_stats(stats: Dict[str, Any]):
    """格式化数据库统计信息"""
    print("\n数据库统计信息")
    print("=" * 30)
    print(f"数据库文件: {stats['database_file']}")
    print(f"文件大小: {stats['file_size_pages']} 页")
    print(f"表数量: {stats['tables_count']}")

    cache_stats = stats["cache_stats"]
    print(f"\n缓存统计:")
    print(f"  缓存命中: {cache_stats['cache_hits']}")
    print(f"  缓存未命中: {cache_stats['cache_misses']}")
    print(f"  命中率: {cache_stats['hit_rate']:.2%}")
    print(f"  缓存页面: {cache_stats['cached_pages']}/{cache_stats['cache_size']}")
