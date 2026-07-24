//! `rk top` — live ratatui fleet dashboard. Deliberately thin: it polls the
//! read-side RPCs the CLI already uses (`status`, `agent.list`,
//! `budget.rollup`, `workflow.list`, `inbox.list`) on an interval and renders
//! them in one screen; per-agent detail stays with `rk log` / herdr.

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout as TuiLayout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Frame;
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

struct Snapshot {
    status: Value,
    agents: Vec<Value>,
    rollup: Value,
    instances: Vec<Value>,
    inbox: Vec<Value>,
}

pub async fn top(layout: &Layout, interval_secs: u64) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let mut terminal = ratatui::init();
    let result = run(&mut terminal, &mut client, interval_secs.max(1)).await;
    ratatui::restore();
    result
}

async fn run(
    terminal: &mut ratatui::DefaultTerminal,
    client: &mut Client,
    interval_secs: u64,
) -> Result<()> {
    let mut snapshot = fetch(client).await?;
    let mut refreshed = Instant::now();
    loop {
        terminal.draw(|f| draw(f, &snapshot))?;
        // Short input poll keeps keys responsive between refreshes without
        // burning CPU; the data refresh rides its own interval.
        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            return Ok(())
                        }
                        KeyCode::Char('r') => {
                            snapshot = fetch(client).await?;
                            refreshed = Instant::now();
                        }
                        _ => {}
                    }
                }
            }
        }
        if refreshed.elapsed() >= Duration::from_secs(interval_secs) {
            snapshot = fetch(client).await?;
            refreshed = Instant::now();
        }
    }
}

async fn fetch(client: &mut Client) -> Result<Snapshot> {
    let status = client.call("status", json!({})).await?;
    let agents = client.call("agent.list", json!({})).await?["agents"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let rollup = client.call("budget.rollup", json!({})).await?;
    let instances = client.call("workflow.list", json!({})).await?["instances"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let inbox = client.call("inbox.list", json!({})).await?["items"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    Ok(Snapshot {
        status,
        agents,
        rollup,
        instances,
        inbox,
    })
}

fn draw(frame: &mut Frame, snap: &Snapshot) {
    let [header, agents, workflows, inbox] = TuiLayout::vertical([
        Constraint::Length(1),
        Constraint::Min(6),
        Constraint::Length((snap.instances.len().clamp(1, 8) + 2) as u16),
        Constraint::Length((snap.inbox.len().clamp(1, 6) + 2) as u16),
    ])
    .areas(frame.area());

    draw_header(frame, header, snap);
    draw_agents(frame, agents, snap);
    draw_workflows(frame, workflows, snap);
    draw_inbox(frame, inbox, snap);
}

fn draw_header(frame: &mut Frame, area: Rect, snap: &Snapshot) {
    let fleet = &snap.rollup["fleet"];
    let spend = match fleet["max_usd"].as_f64() {
        Some(max) if max > 0.0 => format!(
            "${:.2}/${:.2}",
            fleet["spent_usd"].as_f64().unwrap_or(0.0),
            max
        ),
        _ => format!("${:.2}", fleet["spent_usd"].as_f64().unwrap_or(0.0)),
    };
    let live = snap
        .agents
        .iter()
        .filter(|a| matches!(a["state"].as_str(), Some("spawning" | "running")))
        .count();
    let line = Line::from(vec![
        Span::styled("rk top ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            "castle {} · up {} · {} tuples · {} agents ({} live) · fleet {} · ",
            snap.status["castle"].as_str().unwrap_or("?"),
            human_secs(snap.status["uptime_secs"].as_u64().unwrap_or(0)),
            snap.status["tuples"].as_u64().unwrap_or(0),
            snap.agents.len(),
            live,
            spend,
        )),
        Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" quit · "),
        Span::styled("r", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" refresh"),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn draw_agents(frame: &mut Frame, area: Rect, snap: &Snapshot) {
    let mut agents = snap.agents.clone();
    // Live first, then most recently touched — the screen may be shorter than
    // the fleet, so the rows that matter must sort to the top.
    agents.sort_by_key(|a| {
        let live = matches!(a["state"].as_str(), Some("spawning" | "running"));
        let updated = a["updated_at"].as_str().unwrap_or("").to_string();
        (!live, std::cmp::Reverse(updated))
    });
    let rows: Vec<Row> = agents
        .iter()
        .map(|a| {
            let state = a["state"].as_str().unwrap_or("?");
            Row::new(vec![
                Cell::from(a["name"].as_str().unwrap_or("?").to_string()),
                Cell::from(state.to_string()).style(state_style(state)),
                Cell::from(a["role"].as_str().unwrap_or("?").to_string()),
                Cell::from(a["repo_name"].as_str().unwrap_or("?").to_string()),
                Cell::from(a["task"].as_str().unwrap_or("-").to_string()),
                Cell::from(format!("${:.3}", a["cost_usd"].as_f64().unwrap_or(0.0))),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(14),
            Constraint::Min(20),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["NAME", "STATE", "ROLE", "REPO", "TASK", "COST"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::TOP).title("agents"));
    frame.render_widget(table, area);
}

fn draw_workflows(frame: &mut Frame, area: Rect, snap: &Snapshot) {
    // Instance spend comes from the rollup's per-instance list.
    let spend_of = |id: &str| -> String {
        snap.rollup["instances"]
            .as_array()
            .and_then(|list| {
                list.iter()
                    .find(|i| i["instance"].as_str() == Some(id))
                    .and_then(|i| i["spent_usd"].as_f64())
            })
            .map(|s| format!("${s:.3}"))
            .unwrap_or_else(|| "-".into())
    };
    let mut instances = snap.instances.clone();
    // Running first, newest last-started first within each group.
    instances.sort_by_key(|i| {
        let running = i["status"].as_str() == Some("running");
        let started = i["started_at"].as_str().unwrap_or("").to_string();
        (!running, std::cmp::Reverse(started))
    });
    let rows: Vec<Row> = instances
        .iter()
        .map(|i| {
            let status = i["status"].as_str().unwrap_or("?");
            let where_at = match i["awaiting"].as_str() {
                Some(a) => format!("parked: {a}"),
                None => i["error"].as_str().unwrap_or("").to_string(),
            };
            Row::new(vec![
                Cell::from(i["id"].as_str().unwrap_or("?").to_string()),
                Cell::from(i["workflow"].as_str().unwrap_or("?").to_string()),
                Cell::from(status.to_string()).style(instance_style(status)),
                Cell::from(format!("{}/{}", i["current_step"], i["total_steps"])),
                Cell::from(spend_of(i["id"].as_str().unwrap_or(""))),
                Cell::from(where_at),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(6),
            Constraint::Length(8),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(vec!["INSTANCE", "WORKFLOW", "STATUS", "STEP", "SPEND", "WHERE"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::TOP).title("workflows"));
    frame.render_widget(table, area);
}

fn draw_inbox(frame: &mut Frame, area: Rect, snap: &Snapshot) {
    let lines: Vec<Line> = if snap.inbox.is_empty() {
        vec![Line::from(Span::styled(
            "clear — nothing awaiting a human",
            Style::default().fg(Color::DarkGray),
        ))]
    } else {
        snap.inbox
            .iter()
            .map(|it| {
                Line::from(vec![
                    Span::styled(
                        format!("{:<16}", it["kind"].as_str().unwrap_or("?")),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::raw(format!(
                        "{:<14} → {}",
                        it["subject"].as_str().unwrap_or("?"),
                        it["action"].as_str().unwrap_or("?"),
                    )),
                ])
            })
            .collect()
    };
    let title = format!("inbox ({})", snap.inbox.len());
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::TOP).title(title)),
        area,
    );
}

fn state_style(state: &str) -> Style {
    match state {
        "running" => Style::default().fg(Color::Green),
        "spawning" => Style::default().fg(Color::Cyan),
        "completed" => Style::default().fg(Color::DarkGray),
        "dismissed" => Style::default().fg(Color::DarkGray),
        "failed" => Style::default().fg(Color::Red),
        "orphaned" => Style::default().fg(Color::Magenta),
        _ => Style::default(),
    }
}

fn instance_style(status: &str) -> Style {
    match status {
        "running" => Style::default().fg(Color::Green),
        "completed" => Style::default().fg(Color::DarkGray),
        "failed" => Style::default().fg(Color::Red),
        _ => Style::default(),
    }
}

fn human_secs(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d{}h", secs / 86_400, (secs % 86_400) / 3600)
    }
}
