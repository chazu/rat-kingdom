//! Interactive `rk factory dashboard` terminal UI.
//!
//! This is a read-only ratatui projection over the same typed daemon snapshot,
//! inbox, and replay RPCs used by the JSON and plain-text modes.

use crate::factory_cmds::{
    FactoryDashboardArgs, fetch_dashboard, resolve_dashboard_repo,
};
use anyhow::Result;
use ratatui::Frame;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout as TuiLayout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::Value;
use std::time::{Duration, Instant};

const TABS: [Tab; 7] = [
    Tab::Overview,
    Tab::Agents,
    Tab::Workflows,
    Tab::Tickets,
    Tab::Inbox,
    Tab::Approvals,
    Tab::Events,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Overview,
    Agents,
    Workflows,
    Tickets,
    Inbox,
    Approvals,
    Events,
}

impl Tab {
    fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Agents => "Agents",
            Self::Workflows => "Workflows",
            Self::Tickets => "Tickets",
            Self::Inbox => "Inbox",
            Self::Approvals => "Approvals",
            Self::Events => "Events",
        }
    }
}

struct App {
    repo: String,
    snapshot: Value,
    events: Value,
    tab: usize,
    offset: usize,
    refreshed_at: Instant,
    refresh_error: Option<String>,
}

impl App {
    fn from_values(repo: String, snapshot: Value, events: Value) -> Self {
        Self {
            repo,
            snapshot,
            events,
            tab: 0,
            offset: 0,
            refreshed_at: Instant::now(),
            refresh_error: None,
        }
    }

    fn selected_tab(&self) -> Tab {
        TABS[self.tab]
    }

    fn next_tab(&mut self) {
        self.tab = (self.tab + 1) % TABS.len();
        self.offset = 0;
    }

    fn previous_tab(&mut self) {
        self.tab = self.tab.checked_sub(1).unwrap_or(TABS.len() - 1);
        self.offset = 0;
    }

    fn scroll_down(&mut self, amount: usize) {
        self.offset = self
            .offset
            .saturating_add(amount)
            .min(self.selected_row_count().saturating_sub(1));
    }

    fn scroll_up(&mut self, amount: usize) {
        self.offset = self.offset.saturating_sub(amount);
    }

    fn selected_row_count(&self) -> usize {
        let snapshot = &self.snapshot["snapshot"];
        match self.selected_tab() {
            Tab::Overview => 0,
            Tab::Agents => array(snapshot, "agents").len(),
            Tab::Workflows => array(snapshot, "workflows").len(),
            Tab::Tickets => array(snapshot, "tickets").len(),
            Tab::Inbox => array(snapshot, "inbox").len(),
            Tab::Approvals => array(&snapshot["approvals"], "proposals").len(),
            Tab::Events => array(&self.events, "events").len(),
        }
    }

    fn update(&mut self, snapshot: Value, events: Value) {
        self.snapshot = snapshot;
        self.events = events;
        self.refreshed_at = Instant::now();
        self.refresh_error = None;
        self.offset = self
            .offset
            .min(self.selected_row_count().saturating_sub(1));
    }
}

pub async fn open(layout: &Layout, mut args: FactoryDashboardArgs) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    if let Some(repo) = args.repo.as_deref() {
        args.repo = Some(resolve_dashboard_repo(&mut client, repo).await?);
    }
    let repo = args
        .repo
        .clone()
        .unwrap_or_else(|| "all repositories".to_string());
    let (snapshot, events) = fetch_dashboard(&mut client, &args).await?;
    let mut app = App::from_values(repo, snapshot, events);
    let mut terminal = ratatui::init();
    let result = run(
        &mut terminal,
        &mut client,
        &args,
        &mut app,
        args.interval.max(1),
    )
    .await;
    ratatui::restore();
    result
}

async fn run(
    terminal: &mut ratatui::DefaultTerminal,
    client: &mut Client,
    args: &FactoryDashboardArgs,
    app: &mut App,
    interval_secs: u64,
) -> Result<()> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;
        if event::poll(Duration::from_millis(150))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            return Ok(());
                        }
                        KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => app.next_tab(),
                        KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
                            app.previous_tab();
                        }
                        KeyCode::Down | KeyCode::Char('j') => app.scroll_down(1),
                        KeyCode::Up | KeyCode::Char('k') => app.scroll_up(1),
                        KeyCode::PageDown => app.scroll_down(10),
                        KeyCode::PageUp => app.scroll_up(10),
                        KeyCode::Home => app.offset = 0,
                        KeyCode::Char('r') => refresh(client, args, app).await,
                        _ => {}
                    }
                }
            }
        }
        if app.refreshed_at.elapsed() >= Duration::from_secs(interval_secs) {
            refresh(client, args, app).await;
        }
    }
}

async fn refresh(client: &mut Client, args: &FactoryDashboardArgs, app: &mut App) {
    match fetch_dashboard(client, args).await {
        Ok((snapshot, events)) => app.update(snapshot, events),
        Err(error) => {
            app.refresh_error = Some(error.to_string());
            app.refreshed_at = Instant::now();
        }
    }
}

fn draw(frame: &mut Frame, app: &App) {
    let [header, tabs, body, footer] = TuiLayout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(8),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, header, app);
    draw_tabs(frame, tabs, app);
    match app.selected_tab() {
        Tab::Overview => draw_overview(frame, body, app),
        Tab::Agents => draw_agents(frame, body, app),
        Tab::Workflows => draw_workflows(frame, body, app),
        Tab::Tickets => draw_tickets(frame, body, app),
        Tab::Inbox => draw_inbox(frame, body, app),
        Tab::Approvals => draw_approvals(frame, body, app),
        Tab::Events => draw_events(frame, body, app),
    }
    draw_footer(frame, footer, app);
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let snapshot = &app.snapshot["snapshot"];
    let cursor = app.snapshot["cursor"].as_u64().unwrap_or(0);
    let resync = snapshot["repo_resync"]["required"]
        .as_bool()
        .unwrap_or(false);
    let title_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let state = if let Some(error) = &app.refresh_error {
        Span::styled(format!("REFRESH ERROR: {error}"), Style::default().fg(Color::Red))
    } else if resync {
        Span::styled("RESYNC REQUIRED", Style::default().fg(Color::Yellow))
    } else {
        Span::styled("LIVE", Style::default().fg(Color::Green))
    };
    let line = Line::from(vec![
        Span::styled(" RK FACTORY ", title_style),
        Span::raw(format!("{}  ·  cursor {cursor}  ·  ", app.repo)),
        state,
    ]);
    let counts = Line::from(format!(
        " agents {}  workflows {}  tickets {}  inbox {}  approvals {} ",
        array(snapshot, "agents").len(),
        array(snapshot, "workflows").len(),
        array(snapshot, "tickets").len(),
        array(snapshot, "inbox").len(),
        array(&snapshot["approvals"], "proposals").len(),
    ));
    frame.render_widget(
        Paragraph::new(vec![line, counts])
            .block(Block::default().borders(Borders::BOTTOM)),
        area,
    );
}

fn draw_tabs(frame: &mut Frame, area: Rect, app: &App) {
    let titles = TABS
        .iter()
        .map(|tab| Line::from(tab.title()))
        .collect::<Vec<_>>();
    let tabs = Tabs::new(titles)
        .select(app.tab)
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider("  ")
        .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(tabs, area);
}

fn draw_overview(frame: &mut Frame, area: Rect, app: &App) {
    let snapshot = &app.snapshot["snapshot"];
    let [metrics, panels] = TuiLayout::vertical([Constraint::Length(4), Constraint::Min(4)])
        .areas(area);
    let [inbox, activity] =
        TuiLayout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(panels);

    let agents = array(snapshot, "agents");
    let live = agents
        .iter()
        .filter(|agent| matches!(agent["state"].as_str(), Some("running" | "spawning")))
        .count();
    let tickets = array(snapshot, "tickets");
    let open_tickets = tickets
        .iter()
        .filter(|ticket| ticket["payload"]["status"].as_str() == Some("open"))
        .count();
    let fleet = &snapshot["budget"]["fleet"];
    let spent = fleet["spent_usd"].as_f64().unwrap_or(0.0);
    let remaining = fleet["remaining_usd"].as_f64().unwrap_or(0.0);
    let metrics_line = Line::from(vec![
        metric("LIVE AGENTS", live.to_string(), Color::Green),
        Span::raw("     "),
        metric("OPEN TICKETS", open_tickets.to_string(), Color::Yellow),
        Span::raw("     "),
        metric("HUMAN ACTIONS", array(snapshot, "inbox").len().to_string(), Color::Magenta),
        Span::raw("     "),
        metric("SPEND", format!("${spent:.2} / ${remaining:.2} left"), Color::Cyan),
    ]);
    frame.render_widget(
        Paragraph::new(metrics_line).block(
            Block::default()
                .borders(Borders::ALL)
                .title("factory health"),
        ),
        metrics,
    );

    render_data_table(
        frame,
        inbox,
        array(snapshot, "inbox"),
        0,
        TableSpec {
            title: "human inbox",
            headings: &["KIND", "SUBJECT", "ACTION"],
            paths: &["kind", "subject", "action"],
            widths: [
                Constraint::Length(17),
                Constraint::Length(18),
                Constraint::Min(20),
            ],
        },
    );
    let events = array(&app.events, "events");
    if events.is_empty() {
        render_data_table(
            frame,
            activity,
            agents,
            0,
            TableSpec {
                title: "active agents",
                headings: &["NAME", "STATE", "TASK"],
                paths: &["name", "state", "task"],
                widths: [
                    Constraint::Length(16),
                    Constraint::Length(10),
                    Constraint::Min(20),
                ],
            },
        );
    } else {
        render_data_table(
            frame,
            activity,
            events,
            0,
            TableSpec {
                title: "recent events",
                headings: &["CURSOR", "KIND", "SUMMARY"],
                paths: &["cursor", "kind", "summary"],
                widths: [
                    Constraint::Length(9),
                    Constraint::Length(18),
                    Constraint::Min(20),
                ],
            },
        );
    }
}

fn draw_agents(frame: &mut Frame, area: Rect, app: &App) {
    render_data_table(
        frame,
        area,
        array(&app.snapshot["snapshot"], "agents"),
        app.offset,
        TableSpec {
            title: "agents",
            headings: &["NAME", "STATE", "ROLE", "TASK", "UPDATED"],
            paths: &["name", "state", "role", "task", "updated_at"],
            widths: [
                Constraint::Length(16),
                Constraint::Length(10),
                Constraint::Length(12),
                Constraint::Min(24),
                Constraint::Length(24),
            ],
        },
    );
}

fn draw_workflows(frame: &mut Frame, area: Rect, app: &App) {
    render_data_table(
        frame,
        area,
        array(&app.snapshot["snapshot"], "workflows"),
        app.offset,
        TableSpec {
            title: "workflow runs",
            headings: &["ID", "WORKFLOW", "STATUS", "STEP", "AWAITING", "STARTED"],
            paths: &["id", "workflow", "status", "current_step", "awaiting", "started_at"],
            widths: [
                Constraint::Length(18),
                Constraint::Length(20),
                Constraint::Length(10),
                Constraint::Length(7),
                Constraint::Min(16),
                Constraint::Length(24),
            ],
        },
    );
}

fn draw_tickets(frame: &mut Frame, area: Rect, app: &App) {
    render_data_table(
        frame,
        area,
        array(&app.snapshot["snapshot"], "tickets"),
        app.offset,
        TableSpec {
            title: "tickets",
            headings: &["ID", "STATUS", "TITLE", "UPDATED"],
            paths: &["identity", "payload.status", "payload.title", "payload.updated_at"],
            widths: [
                Constraint::Length(30),
                Constraint::Length(10),
                Constraint::Min(32),
                Constraint::Length(25),
            ],
        },
    );
}

fn draw_inbox(frame: &mut Frame, area: Rect, app: &App) {
    render_data_table(
        frame,
        area,
        array(&app.snapshot["snapshot"], "inbox"),
        app.offset,
        TableSpec {
            title: "human inbox",
            headings: &["KIND", "SUBJECT", "DETAIL", "ACTION"],
            paths: &["kind", "subject", "detail", "action"],
            widths: [
                Constraint::Length(18),
                Constraint::Length(18),
                Constraint::Min(28),
                Constraint::Length(30),
            ],
        },
    );
}

fn draw_approvals(frame: &mut Frame, area: Rect, app: &App) {
    render_data_table(
        frame,
        area,
        array(&app.snapshot["snapshot"]["approvals"], "proposals"),
        app.offset,
        TableSpec {
            title: "pending approvals",
            headings: &["ID", "KIND", "STATUS", "RISK", "DIGEST", "EXPIRES"],
            paths: &["id", "kind", "status", "risk", "digest", "expires_at"],
            widths: [
                Constraint::Length(20),
                Constraint::Length(24),
                Constraint::Length(11),
                Constraint::Length(9),
                Constraint::Min(18),
                Constraint::Length(24),
            ],
        },
    );
}

fn draw_events(frame: &mut Frame, area: Rect, app: &App) {
    render_data_table(
        frame,
        area,
        array(&app.events, "events"),
        app.offset,
        TableSpec {
            title: "factory events",
            headings: &["CURSOR", "KIND", "SUBJECT", "SUMMARY", "TIME"],
            paths: &["cursor", "kind", "subject", "summary", "occurred_at"],
            widths: [
                Constraint::Length(9),
                Constraint::Length(20),
                Constraint::Length(18),
                Constraint::Min(28),
                Constraint::Length(24),
            ],
        },
    );
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &App) {
    let age = app.refreshed_at.elapsed().as_secs();
    let line = Line::from(vec![
        key("q"),
        Span::raw(" quit   "),
        key("tab/←/→"),
        Span::raw(" panels   "),
        key("j/k"),
        Span::raw(" scroll   "),
        key("r"),
        Span::raw(format!(" refresh   updated {age}s ago")),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

struct TableSpec<'a, const N: usize> {
    title: &'a str,
    headings: &'a [&'a str; N],
    paths: &'a [&'a str; N],
    widths: [Constraint; N],
}

fn render_data_table<const N: usize>(
    frame: &mut Frame,
    area: Rect,
    values: &[Value],
    offset: usize,
    spec: TableSpec<'_, N>,
) {
    let visible = area.height.saturating_sub(3) as usize;
    let rows = values
        .iter()
        .skip(offset)
        .take(visible)
        .map(|value| {
            Row::new(
                spec.paths
                    .iter()
                    .map(|path| Cell::from(value_text(value_at(value, path))))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let start = if values.is_empty() { 0 } else { offset + 1 };
    let end = (offset + rows.len()).min(values.len());
    let title = format!(
        "{} ({}/{}, rows {start}-{end})",
        spec.title,
        rows.len(),
        values.len()
    );
    let table = Table::new(rows, spec.widths)
        .header(
            Row::new(spec.headings.iter().copied())
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .column_spacing(1)
        .block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(table, area);
}

fn array<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value[key].as_array().map(Vec::as_slice).unwrap_or(&[])
}

fn value_at<'a>(value: &'a Value, path: &str) -> &'a Value {
    path.split('.').fold(value, |current, key| &current[key])
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        Value::String(value) => single_line(value),
        other => single_line(&other.to_string()),
    }
}

fn single_line(value: &str) -> String {
    value.replace(['\n', '\r', '\t'], " ")
}

fn metric(label: &'static str, value: String, color: Color) -> Span<'static> {
    Span::styled(
        format!(" {label} {value} "),
        Style::default()
            .fg(Color::Black)
            .bg(color)
            .add_modifier(Modifier::BOLD),
    )
}

fn key(value: &'static str) -> Span<'static> {
    Span::styled(
        value,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

#[cfg(test)]
mod tests {
    use super::{App, Tab, draw};
    use ratatui::{Terminal, backend::TestBackend};
    use serde_json::json;

    #[test]
    fn factory_dashboard_renders_an_interactive_terminal_shell() {
        let backend = TestBackend::new(120, 32);
        let mut terminal = Terminal::new(backend).unwrap();
        let app = App::from_values(
            "rat-kingdom".into(),
            json!({
                "cursor": 42,
                "snapshot": {
                    "agents": [{"name": "Nibbles", "state": "running", "task": "build dashboard", "repo_name": "rat-kingdom"}],
                    "workflows": [],
                    "tickets": [],
                    "inbox": [{"kind": "need", "subject": "review", "scope": "rat-kingdom", "detail": "human review", "action": "rk approve"}],
                    "budget": {"fleet": {"spent_usd": 1.25, "remaining_usd": 48.75}},
                    "approvals": {"proposals": [], "grants": []},
                    "repo_resync": {"required": false}
                }
            }),
            json!({"events": [], "truncated": false}),
        );

        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(screen.contains("RK FACTORY"), "{screen}");
        assert!(screen.contains(Tab::Overview.title()), "{screen}");
        assert!(screen.contains("Nibbles"), "{screen}");
        assert!(screen.contains("q quit"), "{screen}");
    }
}
