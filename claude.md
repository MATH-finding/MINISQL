下面我想为我的 MiniDBMS 一次性实现触发器（TRIGGER）功能的详细方案和代码落点说明，涵盖我指定的所有核心文件，如果你觉得写在system_catalog.py更合适那么就按你的想法来，保证低耦合、可扩展、Web/Shell端均可用。
1. 触发器功能设计
支持 CREATE TRIGGER、DROP TRIGGER。
支持 BEFORE/AFTER + INSERT/UPDATE/DELETE。
触发器体为一条或多条 SQL 语句（可简化为单条）。
触发器与表绑定，按事件自动执行。
触发器元数据存储于 catalog（如 system_catalog.py）。
Web/Shell端均可创建/删除/查看触发器。
2. 代码实现分工
2.1 sql/lexer.py
新增关键字：TRIGGER、BEFORE、AFTER、FOR、EACH、ROW、EVENT。
2.2 sql/ast_nodes.py
新增节点：
CreateTriggerStatement
DropTriggerStatement
字段包括：trigger_name, timing, event, table_name, statement/body
2.3 sql/parser.py
新增 _parse_create_trigger、_parse_drop_trigger。
在主分发入口支持 CREATE/DROP TRIGGER。
解析语法如：
Apply to system_catal...
2.4 sql/semantic.py
触发器定义/删除只需基本校验（如表是否存在、事件是否合法）。
触发器执行时不做语义分析（直接在 executor 执行）。
2.5 sql/executor.py
新增 _execute_create_trigger、_execute_drop_trigger。
在表的 INSERT/UPDATE/DELETE 操作前后，自动查找并执行相关触发器。
触发器体可用 executor 递归执行。
2.6 catalog/system_catalog.py
存储触发器元数据（如 dict: table_name -> [trigger_def,...]）。
提供 create_trigger/drop_trigger/list_triggers/get_triggers_for_event 等方法。
2.7 interface/web_api.py
新增 RESTful API：
POST /api/triggers 创建触发器
DELETE /api/triggers/<trigger_name> 删除触发器
GET /api/triggers 列出所有触发器
触发器相关操作需鉴权。
2.8 interface/shell.py
新增命令：.triggers 查看触发器，支持 SQL 方式创建/删除。
在帮助文档中补充触发器用法。
3. 代码落点与实现细节
3.1 sql/lexer.py
在 TokenType 和 KEYWORDS 中添加：
Apply to system_catal...
3.2 sql/ast_nodes.py
新增：
Apply to system_catal...
3.3 sql/parser.py
在主分发入口 parse() 增加：
Apply to system_catal...
新增解析方法 _parse_create_trigger、_parse_drop_trigger，解析上述语法。
3.4 sql/semantic.py
在 analyze() 增加对 CreateTriggerStatement/DropTriggerStatement 的分支，做基本校验。
3.5 sql/executor.py
在 execute() 增加对 CreateTriggerStatement/DropTriggerStatement 的分支。
在 execute_insert/_execute_update/_execute_delete 前后，查找并执行相关触发器（通过 catalog）。
触发器体用 executor 递归执行。
3.6 catalog/system_catalog.py
新增触发器元数据结构和管理方法。
3.7 interface/web_api.py
新增 /api/triggers 相关路由，调用 SimpleDatabase 的触发器管理方法。
3.8 interface/shell.py
新增 .triggers 命令，支持 SQL 方式创建/删除触发器，帮助文档补充。
4. 低耦合实现建议
触发器元数据全部集中在 system_catalog.py，executor 只通过接口查找和执行。
parser/ast_nodes/semantic/executor 各自只做本职，互不侵入。
Web/Shell 端均通过统一的 SimpleDatabase/SQLExecutor API 操作，无需重复实现。