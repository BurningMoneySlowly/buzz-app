//! Usage counts for plugins whose manifest declares `host.usage`. Buzz counts
//! each event name a plugin records, per plugin and revision, and a minute
//! after the first count hands them all to the command the build names in
//! `BUZZ_BUILD_PLUGIN_USAGE_COMMAND`, as JSON on its stdin. Counts are kept in
//! memory only: a failed command or quitting the app drops them. A build
//! without the command records nothing.
use crate::host_command::{
    load_login_shell_environment, login_shell_environment, run_command, DEADLINE,
};
use crate::{with_manager, PluginManager};
use buzzodz_plugins::{HostCommand, MAX_HOST_COMMAND_INPUT_BYTES};
use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::Duration;

const WINDOW: Duration = Duration::from_secs(60);
/// Distinct events one plugin may count in a window, and all plugins together,
/// which keeps the command's input well under its 64 KiB limit.
const MAX_PLUGIN_EVENTS: usize = 32;
const MAX_EVENTS: usize = 128;

static SINK: LazyLock<Option<HostCommand>> =
    LazyLock::new(|| sink(option_env!("BUZZ_BUILD_PLUGIN_USAGE_COMMAND")));
static PENDING: Mutex<Counts> = Mutex::new(Counts(BTreeMap::new()));

/// The build's command, a JSON array of the program and its arguments, which
/// the build script has already checked.
fn sink(configured: Option<&str>) -> Option<HostCommand> {
    let mut argv: Vec<String> = serde_json::from_str(configured.filter(|v| !v.is_empty())?).ok()?;
    if argv.is_empty() {
        return None;
    }
    let program = argv.remove(0);
    Some(HostCommand {
        id: "usage".into(),
        program,
        args: argv,
        max_input_bytes: Some(MAX_HOST_COMMAND_INPUT_BYTES),
        max_output_bytes: None,
        artifact: None,
    })
}

fn valid_event(event: &str) -> bool {
    let mut bytes = event.bytes();
    bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && event.len() <= 40
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Counts keyed by plugin id, revision and event.
#[derive(Default)]
struct Counts(BTreeMap<(String, String, String), u64>);

impl Counts {
    /// Counts one event, unless it would be a new one past the window's limits.
    fn add(&mut self, plugin: &str, revision: &str, event: &str) {
        let key = (plugin.to_owned(), revision.to_owned(), event.to_owned());
        if let Some(count) = self.0.get_mut(&key) {
            *count = count.saturating_add(1);
        } else if self.0.len() < MAX_EVENTS
            && self.0.keys().filter(|(id, _, _)| id == plugin).count() < MAX_PLUGIN_EVENTS
        {
            self.0.insert(key, 1);
        }
    }

    fn payload(&self) -> String {
        let events = self
            .0
            .iter()
            .map(|((plugin, revision, event), count)| {
                serde_json::json!({
                    "plugin": plugin,
                    "revision": revision,
                    "event": event,
                    "count": count,
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({ "version": 1, "events": events }).to_string()
    }
}

/// Runs `sink` with the counts on stdin; true when it exits successfully.
async fn deliver(sink: &HostCommand, counts: &Counts) -> bool {
    load_login_shell_environment().await;
    run_command(
        sink,
        login_shell_environment(),
        Some(&counts.payload()),
        DEADLINE,
    )
    .await
    .is_some()
}

#[tauri::command]
pub(crate) async fn plugin_host_record_usage(
    manager: tauri::State<'_, PluginManager>,
    id: String,
    revision: String,
    event: String,
) -> Result<(), String> {
    let Some(sink) = SINK.as_ref() else {
        return Ok(());
    };
    if !valid_event(&event) {
        return Err("Usage event names are 1–40 lowercase letters, digits and underscores, starting with a letter".into());
    }
    // A revision's grants never change, so an event already counted this window
    // was checked when it was first counted.
    let key = (id.clone(), revision.clone(), event.clone());
    let counted = PENDING
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .0
        .contains_key(&key);
    if !counted {
        let (id, revision) = (id.clone(), revision.clone());
        let declared = with_manager(manager, move |manager| {
            Ok(manager.host_grants(&id, &revision)?.usage)
        })
        .await?;
        if !declared {
            return Err("Usage is not declared".into());
        }
    }
    let started = {
        let mut pending = PENDING.lock().unwrap_or_else(PoisonError::into_inner);
        let was_empty = pending.0.is_empty();
        pending.add(&id, &revision, &event);
        was_empty
    };
    if started {
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(WINDOW).await;
            let counts =
                std::mem::take(&mut *PENDING.lock().unwrap_or_else(PoisonError::into_inner));
            if !deliver(sink, &counts).await {
                eprintln!("The plugin usage command failed; its counts were dropped");
            }
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_builds_command() {
        let command = sink(Some(r#"["example-usage","record","--json"]"#)).unwrap();
        assert_eq!(command.program, "example-usage");
        assert_eq!(command.args, ["record", "--json"]);
        assert!(sink(None).is_none());
        assert!(sink(Some("")).is_none());
        assert!(sink(Some("[]")).is_none());
    }

    #[test]
    fn accepts_only_short_snake_case_names() {
        for event in ["a", "panel_opened", "step2", &"a".repeat(40)] {
            assert!(valid_event(event), "{event}");
        }
        for event in [
            "",
            "Panel",
            "2step",
            "_opened",
            "panel-opened",
            "panel opened",
            "é",
            &"a".repeat(41),
        ] {
            assert!(!valid_event(event), "{event}");
        }
    }

    #[test]
    fn counts_each_event_per_plugin_and_revision_within_limits() {
        let mut counts = Counts::default();
        counts.add("example.notes", "r1", "opened");
        counts.add("example.notes", "r1", "opened");
        counts.add("example.notes", "r2", "opened");
        counts.add("example.tasks", "r1", "opened");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&counts.payload()).unwrap(),
            serde_json::json!({ "version": 1, "events": [
                { "plugin": "example.notes", "revision": "r1", "event": "opened", "count": 2 },
                { "plugin": "example.notes", "revision": "r2", "event": "opened", "count": 1 },
                { "plugin": "example.tasks", "revision": "r1", "event": "opened", "count": 1 },
            ]})
        );

        // One plugin cannot crowd out the others, nor all of them the command's input.
        let mut counts = Counts::default();
        for n in 0..MAX_PLUGIN_EVENTS + 1 {
            counts.add("example.noisy", "r1", &format!("event_{n}"));
        }
        counts.add("example.noisy", "r1", "event_0");
        assert_eq!(counts.0.len(), MAX_PLUGIN_EVENTS);
        assert_eq!(
            counts.0[&("example.noisy".into(), "r1".into(), "event_0".into())],
            2
        );
        for plugin in 0..MAX_EVENTS {
            counts.add(&format!("example.p{plugin}"), "r1", "opened");
        }
        assert_eq!(counts.0.len(), MAX_EVENTS);
        // The longest ids, revisions and names.
        let mut full = Counts::default();
        for n in 0..MAX_EVENTS {
            let (plugin, event) = (
                format!("{}{n:03}", "a".repeat(77)),
                format!("{}{n:03}", "e".repeat(37)),
            );
            full.add(&plugin, &"f".repeat(64), &event);
        }
        assert!(full.payload().len() as u64 <= MAX_HOST_COMMAND_INPUT_BYTES);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hands_the_counts_to_the_command_on_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("usage.json");
        let command = HostCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), format!("cat > '{}'", out.display())],
            ..sink(Some(r#"["unused"]"#)).unwrap()
        };
        let mut counts = Counts::default();
        counts.add("example.notes", "r1", "opened");
        assert!(deliver(&command, &counts).await);
        assert_eq!(std::fs::read_to_string(&out).unwrap(), counts.payload());

        let failing = HostCommand {
            args: vec!["-c".into(), "exit 1".into()],
            ..command
        };
        assert!(!deliver(&failing, &counts).await);
    }
}
