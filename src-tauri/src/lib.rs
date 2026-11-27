mod app_state;
mod commands;
mod drivers;
pub mod error;
mod jobs;
mod menu;
pub mod mcp;
#[cfg(test)]
mod live_tests;
mod models;
mod rowsync;
mod security;
mod ssh;
mod storage;
mod transfer;

use app_state::AppState;
use security::SecretStore;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use storage::LocalStore;
use tauri::Manager;

static SCREENSHOT_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();

pub fn screenshot_path() -> Option<&'static PathBuf> {
    SCREENSHOT_PATH
        .get_or_init(parse_screenshot_argument)
        .as_ref()
}

fn parse_screenshot_argument() -> Option<PathBuf> {
    let args = std::env::args().collect::<Vec<_>>();
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--screenshot" {
            return args.get(index + 1).map(PathBuf::from);
        }
        if let Some(path) = args[index].strip_prefix("--screenshot=") {
            if !path.is_empty() {
                return Some(PathBuf::from(path));
            }
        }
        index += 1;
    }
    None
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "luxbase_studio=info".into()),
        )
        .init();

    let screenshot_mode = screenshot_path().is_some();
    tracing::info!(screenshot_mode, "starting LuxBase");
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_os::init())
        .menu(|app| menu::build(app))
        .on_menu_event(|app, event| menu::dispatch(app, event.id().as_ref()))
        .setup(move |app| {
            let data_dir = app
                .path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            if storage::migrate_legacy_data_dir(&data_dir).unwrap_or(false) {
                tracing::info!("copied the legacy Arcbase Studio data folder into the LuxBase data folder; the legacy folder was left untouched");
            }
            let database_path = data_dir.join("luxbase.sqlite3");
            let secrets = SecretStore::new(data_dir.join("vault.key"));
            let store = LocalStore::open(database_path, secrets)
                .map_err(|error| Box::<dyn std::error::Error>::from(error.message))?;
            app.manage(Arc::new(AppState::new(store)));

            if let Some(window) = app.get_webview_window("main") {
                if screenshot_mode {
                    let _ = window.set_focusable(false);
                    #[cfg(target_os = "windows")]
                    {
                        let _ = window.set_always_on_bottom(true);
                    }
                } else {
                    let _ = window.set_focusable(true);
                    let _ = window.set_focus();
                }
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::app_bootstrap,
            commands::save_connection,
            commands::get_connection_config,
            commands::test_connection,
            commands::test_saved_connection,
            commands::delete_connection,
            commands::export_connections,
            commands::import_connections_preview,
            commands::import_connections_apply,
            commands::save_query_draft,
            commands::list_query_drafts,
            commands::rename_query_draft,
            commands::delete_query_draft,
            commands::sql_execute,
            commands::sql_execute_batch,
            commands::sql_run_script,
            commands::sql_apply_ddl,
            commands::sql_fetch,
            commands::sql_update_cell,
            commands::sql_apply_row_edits,
            commands::sql_insert_row,
            commands::sql_delete_rows,
            commands::sql_export_query,
            commands::sql_export_result,
            commands::sql_table_preview,
            commands::sql_table_ddl,
            commands::sql_object_stats,
            commands::sql_drop_table,
            commands::sql_cancel,
            commands::sql_metadata,
            commands::sql_pool_metrics,
            commands::list_identities,
            commands::redis_scan,
            commands::redis_get,
            commands::redis_set,
            commands::redis_create,
            commands::redis_delete,
            commands::redis_info,
            commands::redis_dbsize,
            commands::redis_command,
            commands::redis_cancel,
            commands::es_info,
            commands::es_indices,
            commands::es_create_index,
            commands::es_delete_index,
            commands::es_mapping,
            commands::es_aliases,
            commands::es_alias_modify,
            commands::es_search,
            commands::es_get_document,
            commands::es_put_document,
            commands::es_delete_document,
            commands::es_rest,
            commands::es_cancel,
            commands::vector_info,
            commands::vector_collections,
            commands::vector_describe,
            commands::vector_create_collection,
            commands::vector_drop_collection,
            commands::vector_search,
            commands::vector_query,
            commands::vector_get,
            commands::vector_upsert,
            commands::vector_delete,
            commands::vector_rest,
            commands::vector_cancel,
            commands::export_data,
            commands::import_data,
            transfer::transfer_start,
            transfer::transfer_status,
            transfer::transfer_cancel,
            rowsync::sync_plan_start,
            rowsync::sync_plan_status,
            rowsync::sync_plan_cancel,
            rowsync::sync_get_plan,
            rowsync::sync_apply,
            commands::recent_audit,
            commands::recent_history,
            commands::verify_audit_chain,
            commands::import_legacy_keychain_secrets,
            commands::save_diagnostics_report,
            commands::disconnect_connection,
            commands::shutdown_connections,
            commands::minimize_window,
            commands::toggle_maximize_window,
            commands::close_window,
            commands::set_window_appearance,
            commands::screenshot_target,
            commands::save_screenshot,
            commands::exit_after_screenshot
        ])
        .build(tauri::generate_context!())
        .expect("failed to build LuxBase")
        .run(|app_handle, event| {
            if let tauri::RunEvent::ExitRequested { .. } = event {
                let state = app_handle.state::<Arc<AppState>>();
                tauri::async_runtime::block_on(state.shutdown());
            }
        });
}
