use tauri::menu::{AboutMetadata, Menu, MenuBuilder, MenuItemBuilder, PredefinedMenuItem, SubmenuBuilder};
use tauri::{AppHandle, Manager, Runtime};

// Menu ids are UI command names; the webview runs them through `window.__luxbaseCommand`.
fn item<R: Runtime>(
    app: &AppHandle<R>,
    command: &str,
    label: &str,
    accelerator: Option<&str>,
) -> tauri::Result<tauri::menu::MenuItem<R>> {
    let builder = MenuItemBuilder::with_id(command, label);
    match accelerator {
        Some(accelerator) => builder.accelerator(accelerator).build(app),
        None => builder.build(app),
    }
}

pub fn build<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<Menu<R>> {
    let app_menu = SubmenuBuilder::new(app, "LuxBase")
        .item(&PredefinedMenuItem::about(
            app,
            Some("关于 LuxBase"),
            Some(AboutMetadata {
                name: Some("LuxBase".into()),
                version: Some(env!("CARGO_PKG_VERSION").into()),
                ..Default::default()
            }),
        )?)
        .separator()
        .item(&item(app, "view-settings", "设置…", Some("CmdOrCtrl+,"))?)
        .separator()
        .item(&PredefinedMenuItem::services(app, Some("服务"))?)
        .separator()
        .item(&PredefinedMenuItem::hide(app, Some("隐藏 LuxBase"))?)
        .item(&PredefinedMenuItem::hide_others(app, Some("隐藏其他"))?)
        .item(&PredefinedMenuItem::show_all(app, Some("全部显示"))?)
        .separator()
        .item(&PredefinedMenuItem::quit(app, Some("退出 LuxBase"))?)
        .build()?;

    let file = SubmenuBuilder::new(app, "文件")
        .item(&item(app, "new-connection", "新建连接…", Some("CmdOrCtrl+N"))?)
        .item(&item(app, "new-query", "新建查询", Some("CmdOrCtrl+T"))?)
        .separator()
        .item(&item(app, "save", "保存查询", Some("CmdOrCtrl+S"))?)
        .separator()
        .item(&item(app, "import", "导入数据…", None)?)
        .item(&item(app, "export", "导出数据…", None)?)
        .separator()
        .item(&item(app, "connections-import", "导入连接…", None)?)
        .item(&item(app, "connections-export", "导出连接…", None)?)
        .separator()
        .item(&item(app, "close-tab", "关闭标签页", Some("CmdOrCtrl+W"))?)
        .item(&item(app, "window-close", "关闭窗口", Some("CmdOrCtrl+Shift+W"))?)
        .build()?;

    let edit = SubmenuBuilder::new(app, "编辑")
        .item(&PredefinedMenuItem::undo(app, Some("撤销"))?)
        .item(&PredefinedMenuItem::redo(app, Some("重做"))?)
        .separator()
        .item(&PredefinedMenuItem::cut(app, Some("剪切"))?)
        .item(&PredefinedMenuItem::copy(app, Some("拷贝"))?)
        .item(&PredefinedMenuItem::paste(app, Some("粘贴"))?)
        .item(&PredefinedMenuItem::select_all(app, Some("全选"))?)
        .separator()
        .item(&item(app, "focus-search", "查找连接或对象", Some("CmdOrCtrl+K"))?)
        .build()?;

    let appearance = SubmenuBuilder::new(app, "外观")
        .item(&item(app, "theme-system", "跟随系统", None)?)
        .item(&item(app, "theme-light", "浅色", None)?)
        .item(&item(app, "theme-dark", "深色", None)?)
        .build()?;

    let view = SubmenuBuilder::new(app, "查看")
        .item(&item(app, "view-objects", "对象", Some("CmdOrCtrl+1"))?)
        .item(&item(app, "view-console", "命令控制台", Some("CmdOrCtrl+2"))?)
        .item(&item(app, "view-compare", "结构比较", Some("CmdOrCtrl+3"))?)
        .item(&item(app, "view-audit", "审计日志", Some("CmdOrCtrl+4"))?)
        .item(&item(app, "view-transfer", "数据传输", Some("CmdOrCtrl+5"))?)
        .item(&item(app, "view-sync", "数据同步", Some("CmdOrCtrl+6"))?)
        .separator()
        .item(&item(app, "toggle-sidebar", "显示/隐藏连接面板", Some("CmdOrCtrl+B"))?)
        .item(&item(app, "toggle-inspector", "显示/隐藏信息面板", Some("CmdOrCtrl+Alt+I"))?)
        .item(&appearance)
        .separator()
        .item(&PredefinedMenuItem::fullscreen(app, Some("进入全屏幕"))?)
        .build()?;

    let query = SubmenuBuilder::new(app, "查询")
        .item(&item(app, "run", "运行", Some("CmdOrCtrl+Enter"))?)
        .item(&item(app, "stop", "停止", Some("CmdOrCtrl+."))?)
        .item(&item(app, "explain", "解释", None)?)
        .separator()
        .item(&item(app, "format", "美化 SQL", None)?)
        .item(&item(app, "transaction", "在事务中运行", None)?)
        .build()?;

    let tools = SubmenuBuilder::new(app, "工具")
        .item(&item(app, "view-transfer", "数据传输", None)?)
        .item(&item(app, "view-sync", "数据同步", None)?)
        .item(&item(app, "view-compare", "结构比较", None)?)
        .item(&item(app, "users", "用户与权限", None)?)
        .separator()
        .item(&item(app, "diagnostics", "连接诊断", None)?)
        .item(&item(app, "security-center", "安全中心", None)?)
        .build()?;

    let window = SubmenuBuilder::new(app, "窗口")
        .item(&PredefinedMenuItem::minimize(app, Some("最小化"))?)
        .item(&PredefinedMenuItem::maximize(app, Some("缩放"))?)
        .build()?;

    let help = SubmenuBuilder::new(app, "帮助")
        .item(&item(app, "shortcuts", "快捷键", Some("CmdOrCtrl+Shift+/"))?)
        .build()?;

    MenuBuilder::new(app)
        .items(&[&app_menu, &file, &edit, &view, &query, &tools, &window, &help])
        .build()
}

pub fn dispatch<R: Runtime>(app: &AppHandle<R>, command: &str) {
    // Only ids built above reach here; they are plain kebab-case, so quoting with {:?} is safe JS.
    if !command.chars().all(|c| c.is_ascii_lowercase() || c == '-') {
        return;
    }
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.eval(format!(
            "window.__luxbaseCommand && window.__luxbaseCommand({command:?})"
        ));
    }
}
