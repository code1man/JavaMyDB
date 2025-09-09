# MyDB - 小型数据库系统

## 项目概述
MyDB是一个用C++11编写的教学用小型数据库系统，支持基本的SQL操作和持久化存储。

## 模块架构

### 1. 主程序模块 (main.cpp)
- 程序入口点
- 初始化各组件并启动REPL循环

### 2. 命令行接口模块 (cli.h/cli.cpp)
- 提供交互式命令行界面
- 处理用户输入和结果输出
- 实现REPL (Read-Eval-Print Loop)循环

### 3. 解析器模块 (parser.h/parser.cpp)
- **词法分析**：将SQL字符串转换为Token流
- **语法分析**：使用LL(1)解析器构建抽象语法树(AST)
- **语义分析**：验证SQL语义正确性

### 4. 执行器模块 (executor.h/executor.cpp)
- 接收解析后的AST
- 调用存储引擎执行具体操作
- 返回执行结果

### 5. 存储引擎模块 (storage_engine.h/storage_engine.cpp)
- **文件管理**：处理数据文件的持久化存储
- **缓存管理**：实现缓冲池和页面替换算法
- **索引管理**：维护B+树索引结构
- **事务管理**：保证ACID特性（基础版）

### 6. 工具模块 (utils.h/utils.cpp)
- 通用工具函数
- 内存管理辅助函数
- 调试和日志功能

## 支持的SQL语法(后续会拓展)
- `CREATE DATABASE <db_name>`
- `CREATE TABLE <table_name> (...)`
- `INSERT INTO <table_name> VALUES (...)`
- `SELECT ... FROM ... WHERE ...`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`

## 存储格式
采用自定义二进制格式，优化读写性能：
- 文件头包含元数据和偏移量信息
- 表结构区存储列定义信息
- 数据区使用固定长度+变长记录存储
- 索引区维护B+树结构

## 开发计划
### **阶段一：SQL 编译器实现（LL(1) 文法）**
#### **目标**
实现 SQL 语法解析器，支持从 SQL 字符串到四元式的转换，含 First/Follow 集计算、预测分析表构建、错误处理及四元式输出。

#### **关键任务**
1. **词法分析器（Lexer）开发**
- 定义 Token 类型（关键字、标识符、常量、运算符、分隔符）。
- 实现字符串扫描逻辑，将 SQL 输入拆分为 Token 流（如 `SELECT id FROM users;` → `[SELECT, id, FROM, users, ;]`）。
- 处理大小写不敏感（如 `SELECT` 与 `select` 视为同一关键字）。

2. **LL(1) 文法定义与符号集计算**
    - 定义支持的 SQL 子集文法（示例）：
   ``` 
   SELECT_STMT → SELECT COL_LIST FROM TABLE_NAME [WHERE CONDITION]
   
   COL_LIST → COL_NAME (',' COL_NAME)*
   
   CONDITION → COL_NAME COMP_OP VALUE
   
   COMP_OP → '=' | '>' | '!=' | '>='
   ```
    - 计算文法的 **First 集**（每个非终结符能推导的首符号）和 **Follow 集**（每个非终结符后可能出现的符号）。

3. **预测分析表构建与语法栈动作设计**
    - 基于 First/Follow 集生成无冲突的预测分析表（行：非终结符；列：终结符+`$`）。
    - 设计语法栈动作（如遇到 `SELECT` 则压入 `SELECT_STMT`，匹配 `COL_LIST` 后弹出）。

4. **四元式输出与错误处理**
    - 定义四元式格式 `[步骤, 语法栈, 输入串, 表达式]`（示例：`[1, [SELECT_STMT], [SELECT, id, FROM, users], SELECT(id) FROM(users)]`）。
    - 检测语法错误（如缺少 `FROM` 关键字、括号不匹配），输出具体错误位置及提示（如 `Error: Line 3, Expected 'FROM' after 'SELECT'`）。

#### **预期成果**
- 能正确解析定义的 SQL 子集，输出四元式中间结果。
- 对语法错误输入（如 `SELECT id FROM`）给出明确错误提示。

---
### **阶段二：存储系统开发（页式存储+B+树+缓存）**
#### **目标**
实现数据持久化存储、高效缓存及索引加速，支持页式管理、LRU/FIFO 缓存策略及 B+ 树索引。

#### **关键任务**
1. **页式存储设计**
    - 定义页结构（如 4KB 固定大小）：包含页头（元数据：页 ID、类型、校验和）和数据区（行记录或索引节点）。
    - 实现 `FileManager` 类，支持页的磁盘读写（`read_page(page_id)`/`write_page(page_id)`）、页分配（`allocate_page()`）与释放（`free_page(page_id)`）。
    - 设计表空间文件（`.dat`）格式：文件头（数据库元数据）→ 表结构区（列定义）→ 数据区（行记录按页存储）。

2. **缓存机制（LRU/FIFO）实现**
    - 实现 `BufferPool` 类，内存缓存数据页（默认缓存大小可配置，如 100 页）。
    - 支持 LRU（最近最少使用）或 FIFO（先进先出）替换策略，统计缓存命中率（`hit_count`/`total_access`）。
    - 日志记录缓存操作（如 `Cache Hit: Page 5`/`Cache Miss: Load Page 10 from Disk`）。

3. **B+ 树索引集成**
    - 实现 B+ 树数据结构（节点含键值、子节点指针、叶子节点链表）。
    - 支持索引操作：插入（`insert_index(key, row_id)`）、查询（`search_index(key)`）、范围查询（`range_query(start, end)`）。
    - 与存储引擎集成：在 `INSERT`/`UPDATE` 语句执行时，同步更新 B+ 树索引。

4. **系统目录（元数据管理）**
    - 设计系统表（如 `sys_tables`/`sys_columns`/`sys_indexes`），存储表名、列类型、索引关联列等元数据。
    - 实现元数据查询接口（如 `get_table_schema(table_name)` 返回列定义）。

#### **预期成果**
- 数据可持久化存储至 `.sql` 文件，重启后通过 `FileManager` 恢复。
- 缓存机制有效减少磁盘 I/O（命中率 ≥ 80%）。
- B+ 树索引加速 `WHERE` 条件查询（等值查询时间复杂度 O(log n)）。

---

### **阶段三：数据库引擎与集成**
#### **目标**
整合各模块，实现 SQL 语句的执行流程，支持高效查询与数据操作。

#### **关键任务**
1. **执行器（Executor）设计**
- 接收解析后的 AST 或四元式，调用存储引擎执行具体操作（如 `CREATE TABLE` 调用 `FileManager` 创建表结构）。
- 实现 SQL 语句的执行逻辑（如 `INSERT` 插入数据到数据区，`SELECT` 通过 B+ 树索引加速查询）。

2. **模块联调与接口优化**
- 验证 CLI、Parser、StorageEngine、Executor 之间的接口兼容性（如 Token 流传递、AST 解析、页读写同步）。
- 优化内存管理（如避免内存泄漏，合理分配缓存空间）。

3. **性能优化与稳定性测试**
- 对比 LRU 与 FIFO 缓存策略在不同负载下的命中率，选择最优策略。
- 测试大数据量下的查询性能（如 10 万条记录的 `SELECT` 查询响应时间）。
- 处理磁盘 I/O 错误（如文件损坏时提示 `Database corrupted, please restore from backup`）。

#### **预期成果**
- 系统支持完整 SQL 工作流（`CREATE TABLE→INSERT→SELECT→DELETE`）。
- 查询性能达标（如 `SELECT` 查询响应时间 ≤ 10ms）。
- 系统稳定性验证（连续运行 24 小时无崩溃）。

---

### **阶段四：配置文件与存储总结**
#### **目标**
支持灵活配置，明确数据存储模型与索引机制。

#### **关键任务**
1. **配置文件实现**
- 设计配置文件格式（类似 `my.ini`，键值对形式）：
  ```
  ; 数据库存储配置
  [storage]
  page_size=4096               ; 数据页大小（字节）
  buffer_pool_size=100         ; 缓冲池容量（页数）
  data_file_path=./mydb.dat    ; 表空间文件路径
  index_file_path=./mydb.idx   ; 索引文件路径（可选）
  
  ; 缓存策略配置
  [cache]
  policy=lru                   ; 替换策略（lru/fifo）
  lru_hot_threshold=10         ; LRU热点页阈值（访问次数）
  
  ; 日志配置
  [log]
  level=info                   ; 日志级别（debug/info/warn/error）
  path=./db.log                ; 日志文件路径
  max_size=10 * 1024 * 1024        ; 日志文件最大大小（10MB）
  ```
- 实现 `ConfigManager` 类，启动时加载配置（默认路径 `mydb.ini`），支持运行时参数覆盖。

2. **存储总结文档**
- 整理数据存储模型：固定大小页（4KB）→ 表空间文件（`.dat`）→ 行记录按页存储。
- 说明索引结构：B+ 树节点页（存储键值+子节点指针）与叶子节点页（存储键值+行 ID 链表）。
- 解释缓冲池作用：缓存热点页，减少磁盘随机读，提升查询性能。

#### **预期成果**
- 配置文件生效（如修改 `buffer_pool_size` 后缓存大小动态调整）。
- 存储总结文档清晰描述数据存储与索引机制。

---

## 学习重点
- 数据库内核设计与实现
- C++面向对象编程
- 文件系统和存储管理
- 索引结构与算法
- SQL语言处理