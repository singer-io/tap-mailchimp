#!/usr/bin/env python3
"""
run_tap_sync_and_discovery.py - Universal Discover and Sync runner for any Singer tap

Reads credentials from config.json, runs discovery and sync, displays
results in tabular form (tabulate), saves outputs to JSON files.

Usage:
    python run_tap_sync_and_discovery.py <tap-name> [options]

Examples:
    python run_tap_sync_and_discovery.py tap-delighted
    python run_tap_sync_and_discovery.py tap-contentful
    python run_tap_sync_and_discovery.py tap-contentful --sync-only --catalog /path/to/catalog.json
    python run_tap_sync_and_discovery.py tap-ebay --tap-dir ~/workspace/taps/tap-ebay

Options:
    --discover-only      Run only discovery (no sync)
    --sync-only          Run sync using an existing catalog (skip discovery)
    --tap-dir PATH       Full path to the tap directory
    --config PATH        Path to credentials JSON file
    --catalog PATH       Path to write/read catalog JSON
    --state PATH         Path to state file
    --venv PATH          Path to virtualenv directory
    --log-dir PATH       Directory for log files
    --output-dir PATH    Directory for per-stream JSON output
    --exclude-file PATH  Path to stream exclusion list file
    --run-id ID          Suffix appended to output filenames for versioning

Runner Config File:
    Place a runner_config.json in the working directory or tap directory to
    set persistent defaults. CLI arguments take precedence over the config file.
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

# ---- Colors ----
RED = "\033[0;31m"
GREEN = "\033[0;32m"
CYAN = "\033[0;36m"
BOLD = "\033[1m"
NC = "\033[0m"


def separator(title: str):
    line = "=" * 70
    print(f"\n{CYAN}{line}{NC}")
    print(f"{BOLD}  {title}{NC}")
    print(f"{CYAN}{line}{NC}\n")


def mask_value(v: str) -> str:
    v = str(v)
    if len(v) > 12:
        return v[:8] + "..." + v[-4:]
    if len(v) > 4:
        return v[:2] + "***" + v[-1:]
    return "****"


# Default timeout for subprocess calls (seconds). None = no timeout.
SUBPROCESS_TIMEOUT = 3600  # 1 hour


def run_cmd(cmd: list, stdout_path: str = None, stderr_path: str = None,
            check: bool = True, timeout: int = None) -> subprocess.CompletedProcess:
    """Run a subprocess, optionally redirecting stdout/stderr to files."""
    stdout_f = None
    stderr_f = None
    try:
        stdout_f = open(stdout_path, "w") if stdout_path else None
        stderr_f = open(stderr_path, "w") if stderr_path else None
        result = subprocess.run(
            cmd,
            stdout=stdout_f or subprocess.PIPE,
            stderr=stderr_f or subprocess.PIPE,
            text=True,
            check=check,
            timeout=timeout or SUBPROCESS_TIMEOUT,
        )
        return result
    finally:
        if stdout_f:
            stdout_f.close()
        if stderr_f:
            stderr_f.close()


def find_tap_executable(tap_name: str, tap_venv: Path) -> str:
    """Find the tap executable in the venv."""
    bin_dir = "Scripts" if sys.platform == "win32" else "bin"
    venv_bin = tap_venv / bin_dir / tap_name
    if venv_bin.exists():
        return str(venv_bin)
    # Fallback: check if it's on PATH
    which = shutil.which(tap_name)
    if which:
        return which
    return None


def extract_last_state(output_path: str):
    """Extract the value from the last STATE message in a Singer output file."""
    last_state = None
    with open(output_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "STATE":
                last_state = msg.get("value", {})
    return last_state


def parse_sync_output(output_path: str):
    """Parse a Singer output file and return records, schemas, bookmarks."""
    records = {}
    schemas = {}
    bookmarks = {}

    with open(output_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            if msg_type == "SCHEMA":
                stream = msg.get("stream", "")
                schemas[stream] = msg.get("schema", {})
                if stream not in records:
                    records[stream] = []
            elif msg_type == "RECORD":
                stream = msg.get("stream", "")
                if stream not in records:
                    records[stream] = []
                records[stream].append(msg.get("record", {}))
            elif msg_type == "STATE":
                bm = msg.get("value", {}).get("bookmarks", {})
                bookmarks.update(bm)

    return records, schemas, bookmarks


def count_records_from_output(output_path: str):
    """Count records per stream from a Singer output file."""
    counts = {}
    with open(output_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "RECORD":
                stream = msg.get("stream", "")
                counts[stream] = counts.get(stream, 0) + 1
    return counts


# ==========================================================================
# Steps
# ==========================================================================

def step_validate(tap_dir: Path, config_path: Path):
    separator("Step 1: Validating setup")

    if not tap_dir.is_dir():
        print(f"{RED}ERROR: Tap directory not found: {tap_dir}{NC}")
        sys.exit(1)

    if not config_path.is_file():
        print(f"{RED}ERROR: config.json not found at {config_path}{NC}")
        print(f"  Create it from sample_config.json:")
        print(f"    cp {tap_dir}/sample_config.json {config_path}")
        sys.exit(1)

    print(f"  Tap directory : {tap_dir}")
    print(f"  Config file   : {config_path}")

    config = json.loads(config_path.read_text())
    for k, v in config.items():
        print(f"  {k:<15}: {mask_value(v)}")
    print(f"{GREEN}  Config loaded.{NC}")


def _ensure_tabulate(tap_venv: Path):
    """Ensure tabulate is importable, installing into the venv if needed."""
    global tabulate
    if tabulate is not None:
        return

    bin_dir = "Scripts" if sys.platform == "win32" else "bin"
    pip = tap_venv / bin_dir / "pip"
    subprocess.run(
        [str(pip), "install", "tabulate", "-q"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        timeout=120,
    )

    site_packages = next((tap_venv / "lib").glob("python*/site-packages"), None)
    if site_packages:
        sys.path.insert(0, str(site_packages))
        from tabulate import tabulate as _tabulate
        tabulate = _tabulate
    else:
        print(f"{RED}ERROR: Could not find site-packages in venv to import tabulate.{NC}")
        sys.exit(1)


def step_ensure_venv(tap_name: str, tap_dir: Path, tap_venv: Path) -> str:
    separator(f"Step 2: Activating {tap_name} virtualenv")

    if not tap_venv.is_dir():
        print(f"  Creating virtualenv at {tap_venv} ...")
        subprocess.run([sys.executable, "-m", "venv", str(tap_venv)],
                       check=True, timeout=120)

    tap_exe = find_tap_executable(tap_name, tap_venv)
    if not tap_exe:
        print(f"  Installing {tap_name} ...")
        bin_dir = "Scripts" if sys.platform == "win32" else "bin"
        pip = tap_venv / bin_dir / "pip"
        subprocess.run(
            [str(pip), "install", "-e", str(tap_dir), "-q"],
            check=True,
            timeout=300,
        )
        tap_exe = find_tap_executable(tap_name, tap_venv)
        if not tap_exe:
            print(f"{RED}ERROR: Could not find {tap_name} executable after install.{NC}")
            sys.exit(1)

    _ensure_tabulate(tap_venv)

    print(f"{GREEN}  {tap_name} is available: {tap_exe}{NC}")
    return tap_exe


def step_discover(tap_exe: str, config_path: Path, catalog_path: Path,
                  discover_log: Path):
    separator("Step 3: Running Discovery Mode")

    run_cmd(
        [tap_exe, "--config", str(config_path), "--discover"],
        stdout_path=str(catalog_path),
        stderr_path=str(discover_log),
    )
    print(f"{GREEN}  Catalog written to {catalog_path}{NC}")
    print(f"  Discovery log  : {discover_log}")
    print()

    catalog = json.loads(catalog_path.read_text())
    streams = catalog.get("streams", [])
    if not streams:
        print(f"{RED}ERROR: No streams found in catalog. Discovery may have failed.{NC}")
        sys.exit(1)
    rows = []
    for s in streams:
        name = s["stream"]
        keys = ", ".join(s.get("key_properties", [])) or "—"
        root_md = next(
            (m["metadata"] for m in s.get("metadata", []) if m["breadcrumb"] == []),
            {},
        )
        repl = root_md.get("forced-replication-method", "—")
        repl_keys = ", ".join(root_md.get("valid-replication-keys", [])) or "—"
        parent = root_md.get("parent-tap-stream-id", "—")
        fields = len(s.get("schema", {}).get("properties", {}))
        rows.append([name, keys, repl, repl_keys, parent, fields])

    print(tabulate(
        rows,
        headers=["Stream", "Primary Keys", "Replication", "Replication Key", "Parent", "Fields"],
        tablefmt="pretty",
        stralign="left",
        numalign="right",
    ))
    print(f"\n  Total streams discovered: {len(rows)}")


def load_excluded_streams(exclude_file: Path) -> set:
    """Load stream names to exclude from a text file (one per line)."""
    if not exclude_file.is_file():
        return set()
    excluded = set()
    for line in exclude_file.read_text().splitlines():
        name = line.strip()
        if name and not name.startswith("#"):
            excluded.add(name)
    return excluded


def step_select_all(catalog_path: Path, excluded_streams: set = None):
    separator("Step 4: Selecting all streams and fields")

    excluded_streams = excluded_streams or set()
    catalog = json.loads(catalog_path.read_text())
    selected_count = 0
    skipped = []
    for stream in catalog["streams"]:
        stream_name = stream.get("stream", stream.get("tap_stream_id", ""))
        is_excluded = stream_name in excluded_streams
        for md in stream.get("metadata", []):
            if md["breadcrumb"] == []:
                md["metadata"]["selected"] = not is_excluded
        if is_excluded:
            skipped.append(stream_name)
        else:
            selected_count += 1

    catalog_path.write_text(json.dumps(catalog, indent=2))
    print(f"  Streams selected : {selected_count}")
    if skipped:
        print(f"  Streams excluded : {', '.join(sorted(skipped))}")
    else:
        print("  All streams selected (no exclusions).")


def step_historical_sync(tap_exe: str, config_path: Path, catalog_path: Path,
                         sync_output: Path, sync_log: Path,
                         state_file: Path):
    separator("Step 5: Running Historical Sync")

    run_cmd(
        [tap_exe, "--config", str(config_path), "--catalog", str(catalog_path)],
        stdout_path=str(sync_output),
        stderr_path=str(sync_log),
    )
    print(f"  Sync log : {sync_log}")

    if not sync_output.is_file() or sync_output.stat().st_size == 0:
        print(f"{RED}WARNING: Sync produced no output. The tap may have exited without emitting records.{NC}")

    last_state = extract_last_state(str(sync_output))
    if last_state:
        state_file.write_text(json.dumps(last_state, indent=2))
    print(f"{GREEN}  State saved to {state_file}{NC}")


def step_sync_results(sync_output: Path, results_file: Path, output_dir: Path):
    separator("Step 6: Sync Results")

    records, schemas, bookmarks = parse_sync_output(str(sync_output))

    results = []
    for stream in sorted(records.keys()):
        count = len(records[stream])
        results.append({
            "stream": stream,
            "record_count": count,
            "has_schema": stream in schemas,
        })

    # Save per-stream records
    output_dir.mkdir(exist_ok=True)
    for stream, recs in records.items():
        path = output_dir / f"{stream}.json"
        path.write_text(json.dumps(recs, indent=2))

    total = sum(r["record_count"] for r in results)
    results_file.write_text(json.dumps({"streams": results, "total_records": total}, indent=2))

    # Table
    rows = []
    for r in results:
        bm = bookmarks.get(r["stream"], {})
        bm_val = next(iter(bm.values()), "—") if bm else "—"
        rows.append([r["stream"], r["record_count"], "✓" if r["has_schema"] else "✗", bm_val])
    rows.append(["TOTAL", total, "", ""])

    print(tabulate(
        rows,
        headers=["Stream", "Records", "Schema", "Bookmark"],
        tablefmt="pretty",
        stralign="left",
        numalign="right",
    ))
    print()
    print(f"  Per-stream JSON files : {output_dir}/")
    print(f"  Summary JSON          : {results_file}")


def step_show_state(state_file: Path, label: str):
    separator(label)
    if not state_file.is_file():
        print("  No state file found (tap may not emit STATE messages).")
        return
    state = json.loads(state_file.read_text())
    print(json.dumps(state, indent=2))


def step_bookmark_sync(tap_exe: str, config_path: Path, catalog_path: Path,
                       state_file: Path, bookmark_sync_output: Path,
                       bookmark_sync_log: Path):
    separator("Step 8: Running Bookmark Sync (using state from historical sync)")

    if not state_file.is_file():
        print("  Skipping bookmark sync — no state file from historical sync.")
        return

    run_cmd(
        [tap_exe, "--config", str(config_path), "--catalog", str(catalog_path),
         "--state", str(state_file)],
        stdout_path=str(bookmark_sync_output),
        stderr_path=str(bookmark_sync_log),
    )
    print(f"  Bookmark sync log : {bookmark_sync_log}")

    # Update state from bookmark sync
    last_state = extract_last_state(str(bookmark_sync_output))
    if last_state:
        state_file.write_text(json.dumps(last_state, indent=2))

    # Show bookmark sync results
    counts = count_records_from_output(str(bookmark_sync_output))
    if counts:
        rows = [[s, c] for s, c in sorted(counts.items())]
        rows.append(["TOTAL", sum(counts.values())])
        print(tabulate(rows, headers=["Stream", "Records"],
                       tablefmt="pretty", stralign="left", numalign="right"))
    else:
        print("  No new records since last sync.")


def step_summary(tap_name: str, tap_dir: Path):
    separator(f"Done — {tap_name}")

    print()
    print(f"  Output files ({tap_dir}/):")
    print("    catalog.json               - Discovery catalog")
    print("    state.json                 - Bookmark state after sync")
    print("    sync_output.json           - Raw sync output (all Singer messages)")
    print("    results.json               - Stream/record count summary")
    print("    output/<stream>.json       - Per-stream record data")
    print("    bookmark_sync_output.json  - Bookmark sync output")
    print("    logs/discovery.log         - Discovery stderr log")
    print("    logs/sync.log              - Historical sync stderr log")
    print("    logs/bookmark_sync.log     - Bookmark sync stderr log")
    print()


# ==========================================================================
# Main
# ==========================================================================

def _load_runner_config(*search_dirs):
    """Load runner_config.json from the first directory where it exists."""
    for d in search_dirs:
        cfg_file = Path(d) / "runner_config.json"
        if cfg_file.is_file():
            return json.loads(cfg_file.read_text())
    return {}


def _resolve_path(cli_value, config_value, default):
    """Resolve a path: CLI > config file > default. All converted to Path."""
    if cli_value:
        return Path(cli_value)
    if config_value:
        return Path(config_value)
    return Path(default)


def main():
    parser = argparse.ArgumentParser(
        description="Universal Discover, Sync, and Test runner for any Singer tap",
    )
    parser.add_argument("tap_name", help="Name of the tap (e.g. tap-delighted)")
    parser.add_argument("--discover-only", action="store_true",
                        help="Run only discovery (no sync)")
    parser.add_argument("--sync-only", action="store_true",
                        help="Run sync only using an existing catalog (skip discovery)")
    parser.add_argument("--tap-dir", type=str, default=None,
                        help="Full path to the tap directory")
    parser.add_argument("--config", type=str, default=None,
                        help="Path to credentials JSON file")
    parser.add_argument("--catalog", type=str, default=None,
                        help="Path to write/read catalog JSON")
    parser.add_argument("--state", type=str, default=None,
                        help="Path to state file")
    parser.add_argument("--venv", type=str, default=None,
                        help="Path to virtualenv directory")
    parser.add_argument("--log-dir", type=str, default=None,
                        help="Directory for log files")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Directory for per-stream JSON output")
    parser.add_argument("--exclude-file", type=str, default=None,
                        help="Path to stream exclusion list file")
    parser.add_argument("--run-id", type=str, default=None,
                        help="Suffix appended to output filenames for versioning")
    args = parser.parse_args()

    if args.discover_only and args.sync_only:
        parser.error("--discover-only and --sync-only are mutually exclusive")

    tap_name = args.tap_name

    # S-01: Validate tap_name to prevent path traversal
    if not re.match(r'^[a-zA-Z0-9_-]+$', tap_name):
        print(f"{RED}ERROR: Invalid tap name '{tap_name}'. Only alphanumerics, hyphens, and underscores are allowed.{NC}")
        sys.exit(1)

    # Determine tap_dir (C-01)
    taps_root = Path(__file__).resolve().parent
    default_tap_dir = taps_root / tap_name

    # Load runner config from CWD or tap_dir
    cfg = _load_runner_config(Path.cwd(), default_tap_dir)

    tap_dir = _resolve_path(args.tap_dir, cfg.get("tap_dir"), default_tap_dir)

    # If tap_dir was overridden, reload config from the new location
    if tap_dir != default_tap_dir:
        cfg = _load_runner_config(Path.cwd(), tap_dir)

    # Derive paths with CLI > config > default precedence
    run_id = args.run_id or cfg.get("run_id", "")
    suffix = f"_{run_id}" if run_id else ""

    config_path = _resolve_path(args.config, cfg.get("config"), tap_dir / "config.json")
    catalog_path = _resolve_path(args.catalog, cfg.get("catalog"), tap_dir / "catalog.json")
    state_file = _resolve_path(args.state, cfg.get("state"), tap_dir / "state.json")
    tap_venv = _resolve_path(args.venv, cfg.get("venv"), tap_dir / "venv")
    log_dir = _resolve_path(args.log_dir, cfg.get("log_dir"), tap_dir / "logs")
    output_dir = _resolve_path(args.output_dir, cfg.get("output_dir"), tap_dir / "output")
    exclude_file = _resolve_path(args.exclude_file, cfg.get("exclude_file"),
                                 tap_dir / "streams_to_exclude.txt")

    # Output files (with optional run_id suffix for C-10)
    sync_output = tap_dir / f"sync_output{suffix}.json"
    results_file = tap_dir / f"results{suffix}.json"
    bookmark_sync_output = tap_dir / f"bookmark_sync{suffix}_output.json"
    discover_log = log_dir / f"discovery{suffix}.log"
    sync_log = log_dir / f"sync{suffix}.log"
    bookmark_sync_log = log_dir / f"bookmark_sync{suffix}.log"

    separator(f"Running: {tap_name}")

    # Step 1: Validate
    step_validate(tap_dir, config_path)

    # Step 2: Ensure venv
    tap_exe = step_ensure_venv(tap_name, tap_dir, tap_venv)

    # Create log directory
    log_dir.mkdir(parents=True, exist_ok=True)

    # --sync-only: skip discovery and selection, use existing catalog
    if args.sync_only:
        if not catalog_path.is_file():
            print(f"{RED}ERROR: Catalog file not found: {catalog_path}{NC}")
            print("  Use --catalog to provide an existing catalog, or run without --sync-only.")
            sys.exit(1)
        # Validate catalog has streams
        catalog = json.loads(catalog_path.read_text())
        if not catalog.get("streams"):
            print(f"{RED}ERROR: Catalog file has no streams: {catalog_path}{NC}")
            sys.exit(1)
    else:
        # Step 3: Discovery
        step_discover(tap_exe, config_path, catalog_path, discover_log)

        if args.discover_only:
            separator("Done (discovery only)")
            return

        # Step 4: Select all (respecting streams_to_exclude.txt)
        excluded_streams = load_excluded_streams(exclude_file)
        step_select_all(catalog_path, excluded_streams)

    # Step 5: Historical sync
    step_historical_sync(tap_exe, config_path, catalog_path,
                         sync_output, sync_log, state_file)

    # Step 6: Sync results
    step_sync_results(sync_output, results_file, output_dir)

    # Step 7: Show state (historical)
    step_show_state(state_file, "Step 7: Bookmark State (Historical Sync)")

    # Step 8: Bookmark sync
    step_bookmark_sync(tap_exe, config_path, catalog_path,
                       state_file, bookmark_sync_output, bookmark_sync_log)

    # Step 9: Show state (after bookmark sync)
    step_show_state(state_file, "Step 9: Bookmark State (After Bookmark Sync)")

    # Summary
    step_summary(tap_name, tap_dir)


if __name__ == "__main__":
    main()