# MiniSQL 数据库系统

## 新特性 - B+树索引

拓展：1. 查询优化器 2. 并发控制 3. 高级索引 4. 数据压缩 5. 分区表 6.可视化

### 索引操作
- `CREATE INDEX index_name ON table_name (column_name)` - 创建普通索引
- `CREATE UNIQUE INDEX index_name ON table_name (column_name)` - 创建唯一索引
- `DROP INDEX index_name` - 删除索引

### 性能优化
- SELECT查询会自动使用索引加速
- INSERT/UPDATE操作会自动维护索引
- 支持等值查询和范围查询优化

### 使用示例
```sql
-- 创建表
CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(100), age INTEGER);

-- 创建索引
CREATE INDEX idx_user_age ON users (age);
CREATE UNIQUE INDEX idx_user_email ON users (email);

-- 查询会自动使用索引
SELECT * FROM users WHERE age = 25;
SELECT * FROM users WHERE email = 'user@example.com';

📋 功能特性
✅ 已实现功能
存储管理: 4KB固定页面大小，支持页面分配和回收

缓存管理: LRU缓存策略，提高I/O性能

数据类型: INTEGER, VARCHAR, FLOAT, BOOLEAN, CHAR
        DECIMAL, DATE, TIME, DATETIME, BIGINT
        TINYINT, TEXT

SQL支持: CREATE TABLE, INSERT, SELECT

约束支持: PRIMARY KEY, NOT NULL

条件查询: WHERE子句，支持AND/OR逻辑运算

事务管理: 事务回滚

系统目录: 元数据管理，持久化表结构

交互式Shell: 友好的命令行接口

🚧 计划功能
UPDATE, DELETE语句

索引支持（B+树）

事务管理

连接查询（JOIN）

聚合函数（COUNT, SUM, AVG等）

更多数据类型支持

🏗️ 系统架构
scss
复制
┌─────────────────────────────────────┐
│          SQL Shell (交互层)          │
├─────────────────────────────────────┤
│         执行引擎 (Executor)          │
├─────────────────────────────────────┤
│       SQL解析器 (Parser/Lexer)       │
├─────────────────────────────────────┤
│      表管理器 (Table Manager)        │
├─────────────────────────────────────┤
│     系统目录 (System Catalog)        │
├─────────────────────────────────────┤
│     记录管理器 (Record Manager)       │
├─────────────────────────────────────┤
│     缓存管理器 (Buffer Manager)       │
├─────────────────────────────────────┤
│      页管理器 (Page Manager)         │
├─────────────────────────────────────┤
│          磁盘存储 (Disk)             │
└─────────────────────────────────────┘
📁 目录结构
graphql
复制
simple_database/
├── main.py                    # 主程序入口
├── requirements.txt           # 依赖列表
├── README.md                 # 项目说明
├── test_index_parsing.py      # 索引解析测试文件
├── storage/                   # 存储层
│   ├── __init__.py
│   ├── page_manager.py       # 页面管理
│   ├── buffer_manager.py     # 缓存管理
│   ├── record_manager.py     # 记录管理
│   └── btree.py             # B+树索引实现 (新增)
├── catalog/                  # 系统目录层
│   ├── __init__.py
│   ├── column_definition.py  # 列定义
│   ├── table_schema.py       # 表结构定义
│   ├── system_catalog.py     # 系统目录管理
│   └── index_manager.py      # 索引管理器 (新增)
├── table/                    # 表管理层
│   ├── __init__.py
│   └── table_manager.py     # 表管理器
├── sql/                      # SQL处理层
│   ├── __init__.py
│   ├── lexer.py             # 词法分析
│   ├── parser.py            # 语法分析
│   ├── ast_nodes.py         # AST节点定义
│   └── executor.py          # SQL执行器
└── interface/                # 接口层
    ├── __init__.py
    ├── database.py          # 数据库主接口
    ├── shell.py             # 交互式Shell
    └── formatter.py         # 结果格式化
└── logging/                  
    ├── __init__.py
    ├── logger.py            # 核心日志器
    └── log_manager.py       # 日志管理器

🔧 技术细节
存储引擎
页面大小: 4KB固定大小页面
存储格式: 二进制格式，支持变长记录
缓存策略: LRU替换算法
持久化: 所有数据和元数据持久存储到磁盘

SQL引擎
词法分析: 手工编写的词法分析器
语法分析: 递归下降解析器
执行方式: 解释执行，支持条件下推

数据类型系统
INTEGER: 32位有符号整数

VARCHAR(n): 变长字符串，UTF-8编码

FLOAT: IEEE 754单精度浮点数

BOOLEAN: 布尔值（0/1）

🎯 使用场景
这个项目适合以下场景：

数据库系统学习: 理解数据库内部实现原理
教学演示: 展示数据库各个组件如何协同工作
原型开发: 作为简单应用的嵌入式数据库
技术面试: 展示系统设计和编程能力

🤝 贡献指南
欢迎提交 Issue 和 Pull Request！

开发环境设置
bash
复制
# 只需要Python 3.7+，无额外依赖
python --version  # 