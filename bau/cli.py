"""
CLI entry point for BAU-Assistant.

Commands:
  bau run          — trigger the full diagnosis pipeline
  bau status       — view current runs and pending actions
  bau approve <id> — approve a pending action
  bau reject <id>  — reject a pending action
  bau serve        — start as a long-running service with scheduler
"""

from __future__ import annotations

import asyncio
import logging

import typer
from rich.console import Console
from rich.table import Table

from bau.orchestrator import run_pipeline
from bau.state.store import (
    get_pending_actions,
    get_run,
    init_db,
    update_action_status,
    update_run_status,
)

app = typer.Typer(help="BAU-Assistant — AI-powered data pipeline diagnosis")
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@app.command()
def run():
    """Trigger the full diagnosis pipeline."""
    console.print("[bold]Starting BAU pipeline...[/bold]")
    run_id = asyncio.run(run_pipeline())
    console.print(f"[green]Pipeline complete.[/green] Run ID: {run_id}")


@app.command()
def status():
    """View current runs and pending actions."""
    init_db()
    actions = get_pending_actions()

    if not actions:
        console.print("[dim]No pending actions.[/dim]")
        return

    table = Table(title="Pending Actions")
    table.add_column("Action ID", style="cyan", max_width=12)
    table.add_column("Run ID", style="dim", max_width=12)
    table.add_column("Type", style="green")
    table.add_column("Target", max_width=40)
    table.add_column("Reason")

    for a in actions:
        table.add_row(
            a["action_id"][:12],
            a["run_id"][:12],
            a["action_type"],
            a["target"][:40],
            a["reason"],
        )

    console.print(table)


@app.command()
def approve(action_id: str):
    """Approve a pending action for execution."""
    init_db()
    update_action_status(action_id, "APPROVED")
    console.print(f"[green]Approved[/green] action {action_id[:12]}")
    # TODO: Execute the approved action (Phase 3)


@app.command()
def reject(action_id: str):
    """Reject a pending action."""
    init_db()
    update_action_status(action_id, "REJECTED")
    console.print(f"[red]Rejected[/red] action {action_id[:12]}")


@app.command()
def serve(
    interval_minutes: int = typer.Option(
        60, "--interval", "-i", help="Minutes between pipeline runs"
    ),
):
    """Start as a long-running service with scheduled pipeline runs."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    init_db()
    console.print(
        f"[bold]BAU-Assistant service started.[/bold] "
        f"Pipeline will run every {interval_minutes} minutes."
    )

    def scheduled_run():
        logging.info("Scheduled pipeline run triggered")
        asyncio.run(run_pipeline())

    scheduler = BlockingScheduler()
    scheduler.add_job(scheduled_run, "interval", minutes=interval_minutes)

    # Run once immediately on startup
    scheduled_run()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        console.print("[dim]Service stopped.[/dim]")
