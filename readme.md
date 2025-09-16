# MiniSQL

一个功能完整的关系型数据库管理系统，采用Python实现，支持标准SQL语法、事务管理、B+树索引、智能补全等企业级特性。

## 项目特性

### 核心功能

* **完整SQL支持** - DDL、DML、DCL语句全覆盖
* **ACID事务** - 支持四种隔离级别和完整的事务管理
* **B+树索引** - 高效的数据检索和范围查询
* **智能补全** - SQL关键字、表名、列名自动补全
* **用户权限管理** - 完整的认证授权体系
* **视图和触发器** - 支持虚拟表和事件驱动逻辑

### 架构特色

* **分层架构** - SQL处理、存储引擎、目录管理、接口层清晰分离
* **LRU缓存** - 智能页面缓存管理，提升查询性能
* **语义分析** - 深度的SQL语义检查和类型推断
* **多接口支持** - 命令行界面和现代化Web界面

## 快速开始

### 环境要求

* Python 3.7+
* 依赖库：Flask>=2.0.0, prompt\_toolkit

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行方式

#### 1. 命令行交互模式

```bash
python main.py shell
```

启动交互式SQL命令行，支持：

* 智能SQL补全和语法高亮
* 多行SQL输入
* 事务状态实时显示
* 内置演示系统

#### 2. Web管理界面

```bash
python main.py web
```

启动Web服务器（默认端口5000），提供：

* 现代化的数据库管理界面
* 实时SQL编辑器和执行
* 表数据浏览和管理
* 事务和会话管理
* 响应式设计，支持移动端

访问地址：[http://localhost:5000](http://localhost:5000)

#### 3. 其他运行模式

```bash
python main.py demo    # 运行功能演示
python main.py test    # 运行测试用例
```

## 系统架构

```
miniSQL/
├── main.py                    # 主程序入口
├── requirements.txt           # 依赖列表
├── README.md                 # 项目说明
├── test_index_parsing.py      # 索引解析测试文件
├── storage/                   # 存储层
│   ├── __init__.py
│   ├── page_manager.py       # 页面管理
│   ├── buffer_manager.py     # 缓存管理
│   ├── record_manager.py     # 记录管理
│   └── btree.py             # B+树索引实现
├── catalog/                  # 系统目录层
│   ├── __init__.py
│   ├── data_types.py         # 列定义
│   ├── schema.py             # 表结构定义
│   ├── system_catalog.py     # 系统目录管理
│   └── index_manager.py      # 索引管理器
├── table/                    # 表管理层
│   ├── __init__.py
│   └── table_manager.py     # 表管理器
├── sql/                      # SQL处理层
│   ├── __init__.py
│   ├── lexer.py             # 词法分析
│   ├── parser.py            # 语法分析
│   ├── ast_nodes.py         # AST节点定义
│   ├── executor.py          # SQL执行器
│   ├── semantic.py          # 语义分析
│   ├── transaction_state.py # 事务管理
│   ├── planner.py           # 执行计划生成器
│   ├── plan_nodes.py        # 执行计划节点
│   └── execution_engine.py  # 执行引擎
├── interface/                # 接口层
│   ├── __init__.py
│   ├── database.py          # 数据库主接口
│   ├── shell.py             # 交互式Shell
│   ├── formatter.py         # 结果格式化
│   ├── planner_interface.py # 执行计划接口
│   └── web_api.py           # Flask Web API
└── db_logging/               # 日志系统
    ├── __init__.py
    ├── logger.py            # 核心日志器
    └── log_manager.py       # 日志管理器
```

## 支持的SQL语法

### 数据定义语言 (DDL)

```sql
-- 创建表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INTEGER CHECK(age >= 0),
    created_at DATETIME DEFAULT NOW()
);

-- 创建索引
CREATE INDEX idx_user_name ON users(name);
CREATE UNIQUE INDEX idx_user_email ON users(email);

-- 创建视图
CREATE VIEW active_users AS 
SELECT * FROM users WHERE age >= 18;

-- 删除表/视图/索引
DROP TABLE users;
DROP VIEW active_users;
DROP INDEX idx_user_name;
```

### 数据操纵语言 (DML)

```sql
-- 插入数据
INSERT INTO users (name, email, age) 
VALUES ('张三', 'zhangsan@email.com', 25);

-- 查询数据
SELECT u.name, u.email, COUNT(*) as order_count
FROM users u 
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.age > 20
GROUP BY u.id, u.name, u.email
HAVING COUNT(*) > 5
ORDER BY order_count DESC
LIMIT 10;

-- 更新数据
UPDATE users SET age = 26 WHERE name = '张三';

-- 删除数据
DELETE FROM users WHERE age < 18;
TRUNCATE TABLE users;  -- 快速清空表数据
```

### 数据控制语言 (DCL)

```sql
-- 用户管理
CREATE USER 'test_user' IDENTIFIED BY 'password';
DROP USER 'test_user';

-- 权限管理
GRANT SELECT, INSERT ON users TO 'test_user';
REVOKE INSERT ON users FROM 'test_user';

-- 查看权限
SHOW PRIVILEGES test_user;
```

### 事务控制

```sql
-- 事务操作
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- 设置隔离级别和自动提交
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET AUTOCOMMIT = 0;

-- 查看事务状态
SHOW TRANSACTION STATUS;
SHOW AUTOCOMMIT;
SHOW ISOLATION LEVEL;
```

## Shell命令帮助

### 系统命令

```bash
tables                         # 列出所有表
describe <table>               # 查看表结构
show <table>                   # 查看表数据
indexes [table_name]           # 查看索引信息
views                          # 列出所有视图
triggers                       # 列出所有触发器
stats                          # 显示数据库统计信息
users                          # 列出所有用户
whoami                         # 显示当前用户
```

### 会话管理

```bash
\session list                  # 列出所有会话
\session new                   # 新建会话
\session use <id>              # 切换会话
\session info                  # 显示当前会话信息
```

### 演示命令

```bash
demo views                     # 运行视图演示
demo constraints               # 运行约束演示
demo transactions              # 运行事务演示
```

### 日志命令

```bash
log level <LEVEL>              # 设置日志级别
log stats                      # 显示日志统计
cache stats                    # 显示缓存统计
```

## 支持的数据类型

| 类型           | 描述    | 示例                      |
| ------------ | ----- | ----------------------- |
| INTEGER      | 32位整数 | `123`                   |
| BIGINT       | 64位整数 | `123456789012345`       |
| TINYINT      | 8位整数  | `255`                   |
| FLOAT        | 浮点数   | `3.14159`               |
| DECIMAL(p,s) | 精确小数  | `DECIMAL(10,2)`         |
| BOOLEAN      | 布尔值   | `TRUE`, `FALSE`         |
| CHAR(n)      | 定长字符串 | `CHAR(10)`              |
| VARCHAR(n)   | 变长字符串 | `VARCHAR(255)`          |
| TEXT         | 长文本   | 大段文本内容                  |
| DATE         | 日期    | `'2023-12-25'`          |
| TIME         | 时间    | `'14:30:00'`            |
| DATETIME     | 日期时间  | `'2023-12-25 14:30:00'` |

## 智能补全特性

### SQL补全功能

* **关键字补全** - SELECT, INSERT, UPDATE, DELETE等
* **表名补全** - 自动提示已存在的表名
* **列名补全** - 基于表结构的列名提示
* **函数名补全** - COUNT, SUM, AVG等聚合函数

### 使用示例

```sql
-- 输入 "SEL" 后按Tab，自动补全为 "SELECT"
-- 输入 "FROM us" 后按Tab，自动补全为 "FROM users"
-- 在WHERE子句中会提示相应表的列名
```

## 事务管理特性

### 支持的隔离级别

* **READ UNCOMMITTED** - 读未提交（最低隔离级别）
* **READ COMMITTED** - 读已提交（默认隔离级别）
* **REPEATABLE READ** - 可重复读（快照隔离）
* **SERIALIZABLE** - 串行化（最高隔离级别）

### 事务控制命令

```sql
-- 基本事务操作
BEGIN | START TRANSACTION;
COMMIT;
ROLLBACK;

-- 自动提交控制
SET AUTOCOMMIT = 0|1;
SHOW AUTOCOMMIT;

-- 隔离级别设置
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SHOW ISOLATION LEVEL;

-- 事务状态查看
SHOW TRANSACTION STATUS;
```

## Web界面特性

### 现代化设计

* 响应式布局，适配桌面和移动设备
* 现代配色方案和字体设计
* 直观的标签页式管理界面

### 实时交互

* SQL编辑器支持语法高亮
* Ctrl+Enter快速执行SQL
* 实时显示执行结果和错误信息
* 事务状态和会话信息实时更新

### 完整功能

* 表数据浏览和编辑
* 索引和视图管理
* 用户权限管理
* 查询历史记录
* 数据库统计信息

## 性能特性

* **B+树索引** - O(log n)的查询复杂度
* **LRU缓存** - 智能页面缓存，减少磁盘I/O
* **查询优化** - 基于成本的执行计划生成
* **批量操作** - 支持批量插入和更新
* **分页查询** - 高效的大数据集处理

## 开发与扩展

### 添加新的数据类型

在`catalog/data_types.py`中扩展DataType枚举和相应的验证逻辑。

### 扩展SQL语法

1. 在`sql/lexer.py`中添加新的Token类型
2. 在`sql/parser.py`中扩展语法规则
3. 在`sql/executor.py`中实现执行逻辑

### 自定义存储格式

继承`storage/record_manager.py`中的RecordManager类，实现自定义的序列化逻辑。

## 许可证

本项目采用MIT许可证 - 查看LICENSE文件了解详情。

---

**MiniSQL** - 让数据库开发变得简单而强大！
