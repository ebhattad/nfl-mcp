"""
NFL MCP — command-line entry point

  nfl-mcp init                Interactive setup wizard
  nfl-mcp serve               Start the MCP server (Streamable HTTP)
  nfl-mcp ingest              Load NFL play-by-play data into the database
  nfl-mcp setup-client        Configure Claude Desktop / VS Code MCP
  nfl-mcp doctor              Check that everything is working
"""

import json
import os
import sys
from pathlib import Path

import click
import uvicorn

from .seasons import FIRST_SEASON, current_season
from .server import create_app


@click.group()
def main():
    """NFL play-by-play MCP server."""


# ── serve ──────────────────────────────────────────────────────────────────────

def _env_flag(name: str) -> bool:
    """Read a boolean env var, accepting the usual truthy spellings."""
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


@main.command()
@click.option("--host", default="127.0.0.1", show_default=True,
              help="Host to bind the HTTP server to. Use 0.0.0.0 for LAN access.")
@click.option("--port", default=8000, show_default=True, type=int,
              help="Port to listen on.")
@click.option("--auto-update", is_flag=True,
              help="Poll nflverse while serving and refresh the current season. "
                   "Also enabled by NFL_MCP_AUTO_UPDATE=1.")
@click.option("--update-interval", default=None,
              help="Poll interval for --auto-update.  "
                   "[default: NFL_MCP_UPDATE_INTERVAL, or 30m]")
def serve(host, port, auto_update, update_interval):
    """Start the MCP server over Streamable HTTP."""
    display_host = "localhost" if host == "0.0.0.0" else host
    click.echo(f"🏈 NFL MCP server listening on http://{display_host}:{port}/mcp")
    if host == "0.0.0.0":
        click.echo(f"   (bound to all interfaces — clients should connect via http://localhost:{port}/mcp)")

    if auto_update or _env_flag("NFL_MCP_AUTO_UPDATE"):
        from .updater import parse_interval, start_background_updater

        interval = update_interval or os.getenv("NFL_MCP_UPDATE_INTERVAL") or "30m"
        try:
            interval_seconds = parse_interval(interval)
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc
        start_background_updater(interval_seconds)
        click.echo(f"   auto-update on — checking nflverse every {interval}")

    uvicorn.run(create_app(), host=host, port=port)


# ── ingest ─────────────────────────────────────────────────────────────────────

def _resolve_datasets(names) -> list[str]:
    """Expand the `all` / `default` aliases into a deduplicated dataset list."""
    from .registry import REGISTRY, DEFAULT_DATASETS, ALL_DATASETS

    resolved: list[str] = []
    for name in names:
        if name == "all":
            resolved = ALL_DATASETS
            break
        elif name == "default":
            resolved.extend(DEFAULT_DATASETS)
        elif name in REGISTRY:
            resolved.append(name)
        else:
            raise click.UsageError(
                f"Unknown dataset '{name}'. Run 'nfl-mcp ingest --list' to see options."
            )
    seen: set[str] = set()
    return [d for d in resolved if not (d in seen or seen.add(d))]


def _reject_future_seasons(*values) -> None:
    """Fail fast on seasons nflverse hasn't published yet.

    The loaders reject them anyway, but far deeper in the call stack and with a
    message that doesn't mention which flag was at fault.
    """
    latest = current_season()
    for value in values:
        if value is not None and value > latest:
            raise click.UsageError(
                f"Season {value} is beyond the current season ({latest}). "
                "nflverse publishes a season's data once it kicks off."
            )


@main.command()
@click.option("--dataset", "datasets", multiple=True, default=["default"],
              metavar="NAME",
              help=(
                  "Dataset(s) to load. Pass multiple times or use 'all' / 'default'. "
                  "Run with --list to see all available names."
              ))
@click.option("--start",      default=None, type=click.IntRange(FIRST_SEASON),
              help="First season to load. Omit to load all available seasons.")
@click.option("--end",        default=None, type=click.IntRange(FIRST_SEASON),
              help="Last season to load (inclusive). Omit to load all available seasons.")
@click.option("--fresh",      is_flag=True,
              help="Re-ingest even if dataset+season is already recorded as loaded.")
@click.option("--skip-views", is_flag=True,
              help="Skip rebuilding PBP aggregate tables.")
@click.option("--list", "list_datasets", is_flag=True,
              help="Print all available dataset names and exit.")
def ingest(datasets, start, end, fresh, skip_views, list_datasets):
    """Download NFL data via nflreadpy and load into DuckDB.

    Examples:
      nfl-mcp ingest                          # default datasets, all seasons
      nfl-mcp ingest --dataset all            # every dataset, all seasons
      nfl-mcp ingest --dataset schedules      # one dataset, all seasons
      nfl-mcp ingest --dataset pbp --start 2020 --end 2024
      nfl-mcp ingest --list                   # show all dataset names
    """
    from .registry import REGISTRY

    if list_datasets:
        click.echo("\nAvailable datasets:\n")
        for ds_id, defn in REGISTRY.items():
            default_tag = "[default]" if defn.default else ""
            click.echo(f"  {ds_id:<35} {default_tag:<10}  {defn.description}")
        click.echo()
        return

    if start is not None and end is not None and start > end:
        raise click.UsageError("--start must be less than or equal to --end.")
    _reject_future_seasons(start, end)

    dataset_ids = _resolve_datasets(datasets)

    from .ingest import run_ingest_datasets
    run_ingest_datasets(
        dataset_ids=dataset_ids,
        start=start,
        end=end,
        fresh=fresh,
        skip_views=skip_views,
    )


# ── update ─────────────────────────────────────────────────────────────────────

@main.command()
@click.option("--season", default=None, type=click.IntRange(FIRST_SEASON),
              help="Season to refresh.  [default: current season]")
@click.option("--dataset", "datasets", multiple=True, default=["default"],
              metavar="NAME",
              help="Dataset(s) to refresh. Run 'nfl-mcp ingest --list' to see names.")
@click.option("--force", is_flag=True,
              help="Refresh even when nflverse reports no new data.")
@click.option("--watch", "watch_mode", is_flag=True,
              help="Keep running, re-checking every --interval.")
@click.option("--interval", default="30m", show_default=True,
              help="Poll interval for --watch, e.g. 900s / 30m / 2h.")
@click.option("--skip-views", is_flag=True,
              help="Skip rebuilding PBP aggregate tables.")
def update(season, datasets, force, watch_mode, interval, skip_views):
    """Refresh the current season as nflverse publishes new games.

    Asks nflverse when it last republished the season's play-by-play and only
    downloads when that timestamp has moved, so polling costs one small HTTP
    request. Only the named season's rows are replaced — other seasons are
    left alone.

    \b
    Examples:
      nfl-mcp update                          # one pass over the current season
      nfl-mcp update --watch                  # poll every 30 minutes
      nfl-mcp update --watch --interval 2h    # ...or every two hours
      nfl-mcp update --season 2026 --force    # re-ingest 2026 regardless
    """
    from .updater import parse_interval, run_update, watch as watch_updates

    _reject_future_seasons(season)
    dataset_ids = _resolve_datasets(datasets)
    options = dict(season=season, dataset_ids=dataset_ids,
                   force=force, skip_views=skip_views)

    if not watch_mode:
        changed = run_update(**options)
        click.secho("  ✓ Updated" if changed else "  ✓ Already up to date", fg="green")
        return

    try:
        interval_seconds = parse_interval(interval)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    click.echo(f"🏈 Watching nflverse — checking every {interval}  (Ctrl-C to stop)")
    try:
        watch_updates(interval_seconds, **options)
    except KeyboardInterrupt:
        click.echo("\n  Stopped.")


# ── init ───────────────────────────────────────────────────────────────────────

@main.command()
@click.option("--start", default=FIRST_SEASON, show_default=True,
              type=click.IntRange(FIRST_SEASON),
              help="First season to load.")
@click.option("--end", default=None,
              type=click.IntRange(FIRST_SEASON),
              help="Last season to load.  [default: current season]")
@click.option("--skip-ingest", is_flag=True,
              help="Skip data ingestion (configure only).")
def init(start, end, skip_ingest):
    """Interactive setup wizard — configure database, load data, set up your IDE."""
    end = current_season() if end is None else end
    if start > end:
        raise click.UsageError("--start must be less than or equal to --end.")
    _reject_future_seasons(start, end)
    from .config import save_config, load_config, DEFAULT_DUCKDB_PATH

    click.echo()
    click.secho("🏈 NFL MCP Setup", bold=True)
    click.echo("=" * 50)

    # ── 1. Configure database path ─────────────────────────────────────────
    config = load_config()
    db_path = str(DEFAULT_DUCKDB_PATH)
    custom = click.confirm(
        f"  Database will be stored at {db_path}. Change location?", default=False
    )
    if custom:
        db_path = click.prompt("  Path", default=db_path)
    config["duckdb_path"] = db_path
    click.secho(f"  ✓ DuckDB → {db_path}", fg="green")

    # ── 2. Save config ─────────────────────────────────────────────────────
    path = save_config(config)
    click.secho(f"  ✓ Config saved to {path}", fg="green")

    # ── 3. Ingest data ─────────────────────────────────────────────────────
    if not skip_ingest:
        click.echo()
        click.echo(f"  Seasons to load: {start}–{end}")
        if not click.confirm("  Download and load NFL play-by-play data now?", default=True):
            skip_ingest = True

    if not skip_ingest:
        click.echo()
        from .ingest import run_ingest_datasets
        from .registry import DEFAULT_DATASETS
        run_ingest_datasets(
            dataset_ids=DEFAULT_DATASETS,
            start=start,
            end=end,
            fresh=False,
            skip_views=False,
        )

    # ── 4. Set up IDE client ───────────────────────────────────────────────
    click.echo()
    if click.confirm("  Configure an MCP client (Claude Desktop / VS Code)?", default=True):
        _setup_client_interactive(config)

    # ── 5. Offer to start the server ───────────────────────────────────────
    click.echo()
    click.secho("🎉 Setup complete!", bold=True, fg="green")
    click.echo()
    click.echo("   The MCP server needs to be running for your IDE to connect.")
    click.echo("   You can start it any time with:  nfl-mcp serve")
    click.echo()
    if click.confirm("  Start the server now?", default=True):
        click.secho("   Starting server on http://localhost:8000/mcp  (Ctrl-C to stop)", fg="cyan")
        click.echo()
        uvicorn.run(create_app(), host="127.0.0.1", port=8000)
    else:
        click.echo("   Run  nfl-mcp serve  when you're ready.")
        click.echo()


# ── setup-client ───────────────────────────────────────────────────────────────

@main.command("setup-client")
@click.option("--client", type=click.Choice(["claude-desktop", "claude-code", "vscode", "auto"]),
              default="auto", show_default=True,
              help="Which client to configure.")
def setup_client(client):
    """Auto-configure Claude Desktop, Claude Code, or VS Code to use the NFL MCP server."""
    from .config import load_config
    config = load_config()
    if client == "auto":
        _setup_client_interactive(config)
    elif client == "claude-desktop":
        _configure_claude_desktop(config)
    elif client == "claude-code":
        _configure_claude_code(config)
    elif client == "vscode":
        _configure_vscode(config)


def _setup_client_interactive(config: dict):
    """Prompt user to pick which clients to configure."""
    clients = []
    claude_path = _claude_desktop_config_path()
    if claude_path:
        clients.append(("Claude Desktop", "claude-desktop"))
    clients.append(("Claude Code (global ~/.claude/mcp.json)", "claude-code"))
    clients.append(("VS Code (.vscode/mcp.json in current directory)", "vscode"))

    for name, key in clients:
        if click.confirm(f"    Configure {name}?", default=True):
            if key == "claude-desktop":
                _configure_claude_desktop(config)
            elif key == "claude-code":
                _configure_claude_code(config)
            else:
                _configure_vscode(config)


def _build_server_config(config: dict) -> dict:
    """Build the MCP server JSON block for a client config file."""
    host = config.get("serve_host", "localhost")
    port = config.get("serve_port", 8000)
    # 0.0.0.0 is a bind address, not a connectable URL — use localhost for clients
    display_host = "localhost" if host == "0.0.0.0" else host
    return {"url": f"http://{display_host}:{port}/mcp"}


def _claude_desktop_config_path() -> Path | None:
    """Return Claude Desktop config path if it exists."""
    if sys.platform == "darwin":
        p = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    elif sys.platform == "win32":
        p = Path(os.environ.get("APPDATA", "")) / "Claude" / "claude_desktop_config.json"
    else:
        p = Path.home() / ".config" / "claude" / "claude_desktop_config.json"
    return p if p.parent.exists() else None


def _configure_claude_desktop(config: dict):
    """Write or merge NFL MCP server into Claude Desktop config."""
    path = _claude_desktop_config_path()
    if not path:
        click.secho("    ✗ Claude Desktop config directory not found", fg="yellow")
        return

    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except json.JSONDecodeError:
            existing = {}

    existing.setdefault("mcpServers", {})
    existing["mcpServers"]["nfl"] = _build_server_config(config)

    path.write_text(json.dumps(existing, indent=2))
    click.secho(f"    ✓ Claude Desktop configured → {path}", fg="green")
    click.echo("      Restart Claude Desktop to pick up changes.")


def _configure_vscode(config: dict):
    """Write or merge NFL MCP server into .vscode/mcp.json."""
    vscode_dir = Path.cwd() / ".vscode"
    vscode_dir.mkdir(exist_ok=True)
    path = vscode_dir / "mcp.json"

    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except json.JSONDecodeError:
            existing = {}

    existing.setdefault("servers", {})
    existing["servers"]["nfl"] = _build_server_config(config)

    path.write_text(json.dumps(existing, indent=2))
    click.secho(f"    ✓ VS Code configured → {path}", fg="green")


def _configure_claude_code(config: dict):
    """Write or merge NFL MCP server into ~/.claude/mcp.json (Claude Code global config)."""
    path = Path.home() / ".claude" / "mcp.json"
    path.parent.mkdir(exist_ok=True)

    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except json.JSONDecodeError:
            existing = {}

    existing.setdefault("mcpServers", {})
    existing["mcpServers"]["nfl"] = _build_server_config(config)

    path.write_text(json.dumps(existing, indent=2))
    click.secho(f"    ✓ Claude Code configured → {path}", fg="green")
    click.echo("      Restart Claude Code to pick up changes.")


# ── doctor ─────────────────────────────────────────────────────────────────────

@main.command()
def doctor():
    """Check that everything is configured and working."""
    from .config import load_config, config_exists, get_duckdb_path

    click.echo()
    click.secho("🏈 NFL MCP Doctor", bold=True)
    click.echo("=" * 50)
    all_ok = True

    # 1. Config
    if config_exists():
        click.secho("  ✓ Config file found", fg="green")
    else:
        click.secho("  ✗ No config file — run 'nfl-mcp init' first", fg="red")
        all_ok = False
        click.echo()
        return

    # 2. Database
    db_path = get_duckdb_path()
    if db_path.exists():
        click.secho(f"  ✓ DuckDB file exists ({db_path})", fg="green")
        try:
            import duckdb
            conn = duckdb.connect(str(db_path), read_only=True)
            count = conn.execute("SELECT COUNT(*) FROM plays").fetchone()[0]
            seasons = conn.execute(
                "SELECT MIN(season), MAX(season) FROM plays"
            ).fetchone()
            conn.close()
            click.secho(f"  ✓ {count:,} plays loaded (seasons {seasons[0]}–{seasons[1]})", fg="green")
        except Exception as e:
            click.secho(f"  ✗ Database error: {e}", fg="red")
            all_ok = False
    else:
        click.secho(f"  ✗ DuckDB file not found at {db_path}", fg="red")
        click.echo("    Run 'nfl-mcp ingest' to load data.")
        all_ok = False

    # 3. Claude Desktop config
    claude_path = _claude_desktop_config_path()
    if claude_path and claude_path.exists():
        try:
            cd_config = json.loads(claude_path.read_text())
            if "nfl" in cd_config.get("mcpServers", {}):
                click.secho("  ✓ Claude Desktop configured", fg="green")
            else:
                click.secho("  ⚠ Claude Desktop config exists but 'nfl' server not found", fg="yellow")
        except Exception:
            click.secho("  ⚠ Claude Desktop config exists but is invalid JSON", fg="yellow")
    else:
        click.secho("  – Claude Desktop not detected (skipped)", fg="bright_black")

    # 4. VS Code config
    vscode_path = Path.cwd() / ".vscode" / "mcp.json"
    if vscode_path.exists():
        try:
            vs_config = json.loads(vscode_path.read_text())
            if "nfl" in vs_config.get("servers", {}):
                click.secho("  ✓ VS Code MCP configured", fg="green")
            else:
                click.secho("  ⚠ .vscode/mcp.json exists but 'nfl' server not found", fg="yellow")
        except Exception:
            click.secho("  ⚠ .vscode/mcp.json exists but is invalid JSON", fg="yellow")
    else:
        click.secho("  – VS Code MCP config not found (run 'nfl-mcp setup-client')", fg="bright_black")

    # Summary
    click.echo()
    if all_ok:
        click.secho("  All checks passed! ✅", bold=True, fg="green")
    else:
        click.secho("  Some checks failed — see above for fixes.", bold=True, fg="red")
    click.echo()
