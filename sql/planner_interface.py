# sql/planner_interface.py
"""
执行计划生成器接口
"""

from typing import Dict, Any, Optional
from .planner import ExecutionPlanner, PlanFormatter
from .ast_nodes import Statement
from catalog import SystemCatalog


class PlanGeneratorInterface:
    """执行计划生成器接口"""

    def __init__(self, catalog: SystemCatalog, index_manager=None):
        self.planner = ExecutionPlanner(catalog, index_manager)

    def generate_execution_plan(self, ast: Statement, output_format: str = "tree") -> Dict[str, Any]:
        """
        生成执行计划

        Args:
            ast: 抽象语法树
            output_format: 输出格式 ("tree", "json", "sexp")

        Returns:
            包含执行计划的字典
        """
        try:
            # 生成执行计划
            plan = self.planner.generate_plan(ast)

            # 格式化输出
            if output_format.lower() == "json":
                plan_output = PlanFormatter.format_as_json(plan)
            elif output_format.lower() == "sexp":
                plan_output = PlanFormatter.format_as_sexp(plan)
            else:  # 默认为tree
                plan_output = PlanFormatter.format_as_tree(plan)

            return {
                "success": True,
                "plan": plan_output,
                "format": output_format,
                "estimated_cost": plan.estimated_cost,
                "estimated_rows": plan.estimated_rows,
                "message": f"执行计划生成成功，估计成本: {plan.estimated_cost:.2f}"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"执行计划生成失败: {str(e)}"
            }

    def explain_query(self, sql: str, output_format: str = "tree") -> Dict[str, Any]:
        """
        解释SQL查询的执行计划

        Args:
            sql: SQL语句
            output_format: 输出格式

        Returns:
            执行计划结果
        """
        try:
            from .lexer import SQLLexer
            from .parser import SQLParser

            # 词法分析
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()

            # 语法分析
            parser = SQLParser(tokens)
            ast = parser.parse()

            # 生成执行计划
            return self.generate_execution_plan(ast, output_format)

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"SQL解析失败: {str(e)}"
            }
