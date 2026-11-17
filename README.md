# LuxBase

**简体中文 | [English](README.en.md)**

一个我自己在用的数据库客户端，Navicat 风格，但完全免费开源。
用 Tauri 2 + Rust + React/Monaco 写的，支持 **MySQL、PostgreSQL、SQLite、Redis、Elasticsearch**，还顺手加了个 MCP server，让 AI 助手也能直接查你的库（密码不会给它）。

![LuxBase 主界面](shots/baseline.png)

## 为什么做这个

Navicat 好用是好用，但贵，而且我平时还要同时碰 Redis 和 ES，来回切工具很烦。
所以干脆自己写一个：一个窗口管好几种库，查询、改数据、导数据、传输同步都在里面。
现在它已经是我的日常工具了，发出来给有同样需求的人试试。

## 好用的地方

- **查询写起来顺手**：Monaco 编辑器（就是 VS Code 那颗），多标签、筛选支持 AND/OR 混着来、结果一键导 CSV / JSONL，列宽随便拖。
- **改数据不用写 UPDATE**：结果表格里双击就改，可以一口气改好几行好几列，确认后**一个事务提交，出错全部回滚**。BLOB 也有 HEX / 文本预览和编辑。
- **Redis 很完整**：按 DB 浏览键空间，二进制键值自动识别（UTF-8 / GBK / hex / base64），还有内嵌命令控制台。键多的库也不会卡死。
- **ES 也能管**：索引、mapping、别名、文档增删改、Query DSL、REST 控制台，基本够用。
- **大表传输专门优化过**：keyset 分页 + 批量事务提交，进度、速度、预计剩余时间都看得到；可以选"先停索引、传完重建"，千万级以上的表会舒服很多。
- **安全上比较较真**：密码 AES-256-GCM 加密存本机，不上传任何服务器；删库删表这类操作都要二次确认；所有操作进只追加的审计日志。
- **连接可以带走**：批量导入导出，口令加密打包，换电脑很方便。

## 让 AI 助手帮你查库（MCP）

安装包里带了一个 `luxbase-mcp`，是个本地 MCP server。
配好之后，Claude、Qoder 这类 AI 助手就能直接查你在 LuxBase 里存好的连接——**密码始终留在本地保险库，不会暴露给 AI**。

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

默认只读（查表结构、跑 SELECT、扫 Redis key、查 ES），想让它写数据就把 `args` 改成 `["--allow-writes"]`，会多一个 `execute` 工具（单条写语句）。
AI 的每一条查询和写入，也都会记进 LuxBase 的审计日志。

## 安装（macOS，Apple Silicon）

1. 下载 `LuxBase_<版本>_aarch64.dmg`，把 LuxBase.app 拖进 Applications。
2. 第一次打开如果提示"无法验证开发者"：右键 → 打开，或者终端跑一句
   `xattr -dr com.apple.quarantine /Applications/LuxBase.app`。
   （包是自签名的，没做 Apple 公证，介意的话可以自己从源码构建。）
3. 新建连接就能用了。你的连接和密码都在本机 `~/Library/Application Support/com.luxbase.app/`。

DMG 里也附了一份中文安装说明。

## 从源码跑起来

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

- Redis 的自定义 CA / mTLS 还没做配置入口，超大值的全量编辑也没开放（能预览和复制）。
- 大表优化我实测到 10 万行级别，亿级真实库还没机会压过，欢迎有条件的朋友试试告诉我。
- 目前只打了 macOS Apple Silicon 的包，Windows / Linux 理论上 Tauri 都能出，但我还没精力验证。

碰到问题或者想要什么功能，直接开 issue 就行，我看到会回。

## License

[MIT](LICENSE) © 2026 charl。随便用，改得顺手的话也欢迎 PR。
