<div align="center">

<img src="icon-source.png" width="120" alt="LuxBase logo" />

# LuxBase

**Navicat 的免费开源平替。**
一个窗口管好 MySQL · PostgreSQL · SQLite · Redis · Elasticsearch —— 还内置 MCP server，让 AI 助手也能查你的库。

[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/badge/release-v2.2.0-brightgreen.svg)](https://github.com/char518/LuxBase/releases)
[![Platform](https://img.shields.io/badge/platform-macOS%20·%20Universal-lightgrey.svg)](#安装macos)
[![Tauri 2](https://img.shields.io/badge/Tauri-2-24C8DB.svg?logo=tauri&logoColor=white)](https://tauri.app)

[English](README.md) | **简体中文**

![LuxBase 主界面](shots/baseline.png)

</div>

LuxBase 是我自己写、自己天天在用的数据库客户端。起因很简单：Navicat 好用但贵，而我平时还要同时碰 Redis 和 ES，来回切工具很烦。于是干脆写一个工具全管了——查询、改数据、导出、传输、同步，都在一个窗口里。它已经当了我的主力工具一阵子，也许对你也有用。

## 和 Navicat 对照一下

| 你平时在 Navicat 里做的事 | LuxBase 对应 |
| --- | --- |
| 连 MySQL / PostgreSQL / SQLite | 一样支持，还多了 Redis 和 Elasticsearch |
| 表数据浏览、筛选、编辑 | 双击单元格直接改；多处修改一个事务提交、失败全回滚 |
| SQL 查询、执行历史、执行计划 | Monaco 编辑器 + 执行历史 + 执行计划 |
| 结果导出 | CSV / JSON Lines，文件名带表名和时间 |
| 数据传输 / 同步 / 结构比较 | 都有；keyset 分页 + 批量提交，实时速率和 ETA |
| 连接管理、换机迁移 | 批量导入导出，口令加密打包 |
| 价格 | 免费，GPL-3.0 开源 |

**比 Navicat 多出来的**：Redis / ES 一体化管理、MCP 让 AI 助手查库、代码完全开放可以自己改。

## 协议覆盖

| | MySQL | PostgreSQL | SQLite | Redis | Elasticsearch |
| --- | :-: | :-: | :-: | :-: | :-: |
| 数据浏览与编辑 | ✓ | ✓ | ✓ | ✓ | ✓ |
| 查询控制台 | SQL | SQL | SQL | 原始命令 | Query DSL / REST |
| 传输与同步 | ✓ | ✓ | ✓ | — | — |
| 结构比较 | ✓ | ✓ | ✓ | — | — |

## 好用的地方

- **顺手的 SQL 工作台** —— Monaco 编辑器（就是 VS Code 那颗），多标签、筛选支持 AND/OR 混着来、结果一键导 CSV / JSONL，列宽随便拖。
- **改数据不用写 UPDATE** —— 结果表格双击就改，行数列数随便暂存，确认后**一个事务提交，出错全部回滚**。BLOB 也有 HEX / 文本预览和编辑。
- **Redis 很完整** —— 按 DB 浏览键空间，二进制键值自动识别（UTF-8 / GBK / hex / base64），按类型编辑值，内嵌命令控制台，键多的库也不卡。
- **ES 也能管** —— 索引、mapping、别名、文档增删改、Query DSL、REST 控制台。
- **大表传输专门优化过** —— keyset 分页 + 批量事务提交，进度、速度、预计剩余时间都看得到；可以选"先停索引、传完重建"，千万级以上的表会舒服很多。
- **安全上比较较真** —— 密码 AES-256-GCM 加密存本机，不上传任何服务器；危险操作二次确认；所有操作进只追加的审计日志。
- **连接可以带走** —— 批量导入导出、口令加密打包，换电脑不费劲。

## 让 AI 助手帮你查库（MCP）

安装包里带了 `luxbase-mcp`，一个本地 stdio MCP server。配好之后，Claude、Qoder 这类 AI 助手就能直接查你在 LuxBase 里存好的连接——**密码始终留在本地保险库，不会暴露给 AI**。

```json
{
  "mcpServers": {
    "luxbase": {
      "command": "/Applications/LuxBase.app/Contents/MacOS/luxbase-mcp",
      "args": []
    }
  }
}
```

默认只读：表结构、单条 SELECT（默认 100 行、最多 1000 行）、Redis 扫描、ES 查询。把 `"args"` 改成 `["--allow-writes"]` 会多一个 `execute` 工具（单条写语句）。AI 的每一条查询和写入，都会记进 LuxBase 的审计日志。

## 安装（macOS）

1. 到 [Releases](https://github.com/char518/LuxBase/releases) 下载 `LuxBase_2.2.0_universal.dmg`，把 LuxBase.app 拖进 Applications。一个 universal 包同时支持 Intel 和 Apple Silicon（macOS 12+）。
2. 包是自签名、未公证的。如果提示"无法验证开发者"：右键 → 打开，或者终端跑
   ```bash
   xattr -dr com.apple.quarantine /Applications/LuxBase.app
   ```
   想要完全掌控？也可以[从源码构建](#从源码构建)。
3. 新建连接就能用了。所有数据都在本机 `~/Library/Application Support/com.luxbase.app/`。

DMG 里附了一份中英双语安装说明。

## 从源码构建

需要 Rust（stable）和 Node.js 20+：

```bash
npm install
npm run validate                      # 静态检查和审计门禁
node scripts/build-macos-app.js --dmg # 打出 app 和 DMG（含 luxbase-mcp）
```

跑测试：

```bash
cd src-tauri && cargo test --lib
```

还有一组连真实数据库的测试，**只在你明确指定的临时实例上跑**，建的表都带 `luxbase_qa_` 前缀：

```bash
LUXBASE_LIVE_MYSQL="127.0.0.1|3306|user|password|testdb" cargo test --lib
LUXBASE_LIVE_POSTGRES="127.0.0.1|5432|user|password|testdb" cargo test --lib
LUXBASE_LIVE_REDIS="127.0.0.1|6379|password" cargo test --lib
```

## 说实话，现在的边界

- Redis 的自定义 CA / mTLS 还没做配置入口；超大值能预览和复制，但没开放全量编辑。
- 大表优化实测到 10 万行级别，亿级真实库还没机会压过——有条件的朋友试试告诉我结果。
- 目前只发了 macOS 的包（Intel 与 Apple Silicon 通用的 universal 二进制）。Windows / Linux 理论上 Tauri 都能出，只是我还没精力验证。

## 参与贡献

碰到问题或者想要什么功能？[开 issue](https://github.com/char518/LuxBase/issues) 就行，我看到会回。也欢迎提 PR。

## License

[GPL-3.0](LICENSE) © 2026 charl。代码随便用，但有一条规矩：基于本项目修改或衍生的作品，也必须以 GPL-3.0 公开源码，不能闭源白嫖。LuxBase 名称和 logo 不在授权范围内，衍生作品请换成自己的品牌。