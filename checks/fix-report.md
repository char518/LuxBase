# LuxBase 修复报告（开源基线）

与 `checks/issue-ledger.md` 一一对应；验证手段：`cargo test --lib`（82 项）、`npm run validate`、
disposable MySQL/PostgreSQL/SQLite/Redis/Elasticsearch 实测与 mock 后端浏览器 QA。

### P1-01 — Redis 连接测试结果提示被弹窗遮挡
toast 层级提升至 300（高于 modal 200），并在连接弹窗内联展示测试结果。

### P1-02 — 跨 DB 读取键值报 `type 'none'`
KV trait 全链路携带 db 参数（get/set/del/ttl/key_type/dbsize），`none` 返回明确的键不存在提示。

### P1-03 — Redis DB 缺少键数量显示
解析 INFO keyspace 并新增 redis_dbsize 精确计数。

### P1-04 — 万级键 DB 因 SCAN 空批无法打开
SCAN COUNT 仅作提示，空批自动续扫（上限 64 轮），失败不锁死加载态、支持重试。

### P1-05 — 二进制键名/值 UTF-8 乱码
SCAN 与值按字节解析 + lossy 展示，keyBase64/rawBase64 贯穿读写导出，提供编码检测与可读片段。

### P1-06 — 键信息长键名溢出覆盖工具栏
头部允许换行、按钮不换行，长键 ellipsis，信息网格 minmax(0,1fr)。

### P1-07 — Redis 自定义 CA / mTLS 未提供配置入口
当前版本不支持（Redis 自定义 CA/mTLS），在能力矩阵中明示，不静默降级。

### P1-08 — 超大 Redis 值的全量编辑未开放
当前版本不支持（超大值全量编辑），提供分片预览与复制作为替代。

### P2-01 — Elasticsearch 协议接入
新增 Search 驱动与 manager（basic/API Key/TLS/CA）、索引/别名/文档/REST 命令与工作区；真实 ES 7.7.1 全链路验证。

### P2-02 — ES 文档表格序号、列宽拖拽与 JSON 高亮
文档表满宽 + 序号列 + 通用列宽拖拽持久化，_source 与 mapping/结果 JSON 语法高亮。

### P2-03 — ES REST 控制台请求/响应区拖拽与自适应
拖拽条 fixed 定位避免裁剪，请求/响应 flex 填满窗口，状态行上移。

### P2-04 — MySQL 对象列表缺少容量列
对象统计返回数据/索引字节并在对象列表展示容量。

### P2-05 — 表筛选不支持 AND/OR 混合分组
buildFilterWhere 支持分组条件混合拼接，生成参数化 WHERE。

### P2-06 — 结果页导出 CSV/JSONL
查询与表数据结果均可导出，默认文件名含表名与时间戳。

### P2-07 — 查询结果双击编辑（含表别名 SQL）
单表检测支持别名/db.table/末尾分号，双击等待主键元数据就绪；JOIN/UNION/子查询保持只读。

### P2-08 — 批量修改需按行合并 UPDATE 并单事务提交
新增 sql_apply_row_edits：每行多列合并一条 UPDATE，单事务执行、失败整批回滚，审计记录；真实 MySQL 回滚用例通过。

### P2-09 — BLOB 列展示与 HEX 编辑保存
BLOB 弹窗提供 HEX dump/文本预览/HEX 编辑保存，保存目标修正为 blobTarget。

### P2-10 — 表格列宽拖拽与持久化
通用 initGenericColumnResize/applyGenericColWidths 覆盖对象、审计、传输、计划等表格并同步表宽。

### P2-11 — 默认查询页不自动打开且标签可关闭
工作区标签按需创建，Redis/ES 常驻标签可关闭且会话保持。

### P2-12 — 连接选择页布局偏下
连接选择空态改为顶部对齐（objects-empty-top），其它空态保持居中。

### P2-13 — 连接批量导入/导出（口令密封信封）
AES-256-GCM + salt/SHA256(passphrase) 信封，导出预览、导入预检与应用分步执行。

### P2-14 — LuxBase 品牌统一（logo、协议徽标、署名）
L+棱镜+基座非蓝 logo、协议图形徽标、DMG 注入 charl 署名与安装说明；源码内 Arcbase 标识统一为 LuxBase，legacy 迁移保留旧标识。

### P2-15 — 传输 keyset 分页与批量事务提交
单主键 keyset 避免 OFFSET 深分页，16 页一批事务提交并修复末批未 flush；100k SQLite 实测 224ms。

### P2-16 — 亿级专项：COUNT 进度/速率/ETA、索引停用重建、检查开关
每表 COUNT 驱动表级进度条与 ETA；可选停用二级索引（完成后重建）与禁用唯一/外键检查；索引重建有 100k 断言。

### P2-17 — 同步语句分组
insert 500 行/批多行 VALUES，delete OR 合并并对 PostgreSQL 占位符重编号，applied 计数修正。

### P2-18 — MCP stdio server（只读默认、写开关、全量审计）
luxbase-mcp 独立二进制：7 个只读工具 + --allow-writes 解锁 execute；密码不出保险库，agent_query/agent_execute 全量审计；stdio 与真实 MySQL 用例通过。

### P2-19 — Monaco 占位提示与光标位置对齐
按 layout.contentLeft 与首行坐标定位 editor-empty-hint。

### P2-20 — 连接导入/导出入口移至文件菜单
原生文件菜单新增导入连接/导出连接，与批量管理界面一致。
