from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence, TextIO

from playwright.sync_api import Download, Error, Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


REQUIRED_FIELDS = ["date", "code", "registrations", "ftds", "state"]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
STATE_RE = re.compile(r"^[A-Z]{2}$")
USERNAME_SELECTORS = [
    "input[name='username']",
    "input[type='email']",
    "input[autocomplete='username']",
    "#username-input",
]
PASSWORD_SELECTORS = [
    "input[name='password']",
    "input[type='password']",
    "input[autocomplete='current-password']",
    "#password-input input",
]
GRID_SCROLL_CONTAINER_SELECTOR = "div.grid-container"
GRID_BODY_SELECTOR = "div.grid"
GRID_CELL_SELECTOR = f"{GRID_BODY_SELECTOR} div.cell"


class ScrapeError(RuntimeError):
    pass


class ConsoleLogger:
    _RESET = "\033[0m"
    _DIM = "\033[2m"
    _CYAN = "\033[36m"
    _GREEN = "\033[32m"
    _YELLOW = "\033[33m"
    _RED = "\033[31m"

    def __init__(self, stream: TextIO | None = None) -> None:
        self.stream = stream or sys.stdout
        self._interactive = bool(getattr(self.stream, "isatty", lambda: False)())
        self._use_color = self._interactive and os.environ.get("NO_COLOR") is None and os.environ.get("TERM") != "dumb"
        self._lock = threading.Lock()

    def step(self, title: str) -> "ProgressStep":
        return ProgressStep(self, title)

    def info(self, message: str) -> None:
        if self._use_color:
            self._write_line(self._colorize(message, self._YELLOW))
            return
        self._write_line(f"[info] {message}")

    def _write_line(self, line: str) -> None:
        with self._lock:
            self.stream.write(f"{line}\n")
            self.stream.flush()

    def _colorize(self, text: str, color: str) -> str:
        if not self._use_color:
            return text
        return f"{color}{text}{self._RESET}"

    def _dim(self, text: str) -> str:
        if not self._use_color:
            return text
        return f"{self._DIM}{text}{self._RESET}"


class ProgressStep:
    _FRAMES = ("|", "/", "-", "\\")

    def __init__(self, logger: ConsoleLogger, title: str) -> None:
        self.logger = logger
        self.title = title
        self.detail = ""
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at = 0.0
        self._last_render_width = 0

    def __enter__(self) -> "ProgressStep":
        self._started_at = time.monotonic()
        if self.logger._interactive:
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
        else:
            self.logger._write_line(f"[....] {self._display_message()}")
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, _tb: object) -> bool:
        if exc is None:
            self.succeed()
            return False

        self.fail(str(exc))
        return False

    def update(self, detail: str) -> None:
        self.detail = detail

    def succeed(self) -> None:
        self._finish(" ok ", self._display_message())

    def fail(self, error: str) -> None:
        message = self._display_message()
        if error:
            message = f"{message} - {error}"
        self._finish("fail", message)

    def _display_message(self) -> str:
        if not self.detail:
            return self.title
        return f"{self.title} - {self.detail}"

    def _spin(self) -> None:
        frame_index = 0
        while not self._done.wait(0.1):
            elapsed = time.monotonic() - self._started_at
            frame = self._FRAMES[frame_index % len(self._FRAMES)]
            plain = f"[{frame}] {self._display_message()} ({elapsed:.1f}s)"
            if self.logger._use_color:
                rendered = (
                    f"{self.logger._colorize(f'[{frame}]', self.logger._CYAN)} "
                    f"{self._display_message()} {self.logger._dim(f'({elapsed:.1f}s)')}"
                )
            else:
                rendered = plain
            self._render(rendered, plain)
            frame_index += 1

    def _render(self, line: str, plain: str) -> None:
        with self.logger._lock:
            visible_width = max(self._last_render_width, len(plain))
            self.logger.stream.write(f"\r{line}")
            if visible_width > len(plain):
                self.logger.stream.write(" " * (visible_width - len(plain)))
            self.logger.stream.flush()
            self._last_render_width = visible_width

    def _finish(self, label: str, message: str) -> None:
        if self._done.is_set():
            return

        self._done.set()
        if self._thread is not None:
            self._thread.join()

        elapsed = time.monotonic() - self._started_at
        final_line = f"[{label}] {message} ({elapsed:.1f}s)"
        if self.logger._use_color:
            color = self.logger._GREEN if label.strip() == "ok" else self.logger._RED
            rendered = f"{self.logger._colorize(message, color)} {self.logger._dim(f'({elapsed:.1f}s)')}"
        else:
            rendered = final_line
        if self.logger._interactive:
            with self.logger._lock:
                self.logger.stream.write("\r" + (" " * self._last_render_width) + "\r")
                self.logger.stream.write(f"{rendered}\n")
                self.logger.stream.flush()
        else:
            self.logger._write_line(rendered)


@dataclass(frozen=True)
class Settings:
    dashboard_url: str
    username: str
    password: str
    output_path: Path
    downloads_dir: Path
    headless: bool = True
    timeout_ms: int = 45_000


class QuickSightScraper:
    def __init__(self, page: Page, settings: Settings, logger: ConsoleLogger) -> None:
        self.page = page
        self.settings = settings
        self.logger = logger

    def run(self) -> list[dict[str, object]]:
        with self.logger.step("Signing in to QuickSight") as step:
            self.login(step)

        with self.logger.step("Loading dashboard") as step:
            self.open_dashboard(step)

        with self.logger.step("Trying CSV export") as step:
            rows = self.try_export_csv(step)
            if rows:
                step.update(f"downloaded {len(rows)} row(s)")
        if rows:
            return rows

        self.logger.info("CSV export was not available; falling back to visible grid scraping.")

        with self.logger.step("Scraping visible QuickSight grid") as step:
            rows = self.fallback_dom_scrape(step)
            if rows:
                step.update(f"collected {len(rows)} row(s)")
        if rows:
            return rows

        raise ScrapeError(
            "Could not extract dashboard data. CSV export was not found and the DOM fallback returned no usable rows."
        )

    def login(self, step: ProgressStep) -> None:
        step.update("opening sign-in page")
        self.page.goto(self.settings.dashboard_url, wait_until="domcontentloaded", timeout=self.settings.timeout_ms)
        if self._is_dashboard_page():
            step.update("existing session is already authenticated")
            return

        step.update("waiting for username field")
        self._best_effort_fill_login_field(USERNAME_SELECTORS, self.settings.username)
        step.update("submitting username")
        self._submit_login_step()

        step.update("waiting for password step")
        if not self._wait_for_auth_state():
            raise ScrapeError("Username submission did not advance to a password page or the dashboard.")

        if self._is_dashboard_page():
            step.update("signed in")
            return

        step.update("entering password")
        self._best_effort_fill_login_field(PASSWORD_SELECTORS, self.settings.password)
        step.update("submitting password")
        self._submit_login_step()

        step.update("waiting for dashboard redirect")
        if not self._wait_for_dashboard_page():
            raise ScrapeError("Password submission did not reach the QuickSight dashboard.")

        step.update("signed in")

    def open_dashboard(self, step: ProgressStep) -> None:

        if not self._is_dashboard_page():
            step.update("navigating to the dashboard")
            self.page.goto(self.settings.dashboard_url, wait_until="domcontentloaded", timeout=self.settings.timeout_ms)

        step.update("waiting for dashboard shell")
        self.page.wait_for_load_state("domcontentloaded", timeout=self.settings.timeout_ms)
        self._wait_for_dashboard_shell()
        step.update("checking for welcome modal")
        self._dismiss_welcome_modal()
        step.update("waiting for table rows to appear")
        self.page.wait_for_function(
            f"() => document.querySelectorAll('{GRID_CELL_SELECTOR}').length >= 5",
            timeout=self.settings.timeout_ms,
        )
        self.page.wait_for_timeout(1_000)
        step.update("dashboard is ready")

    def try_export_csv(self, step: ProgressStep) -> list[dict[str, object]]:
        step.update("looking for export controls")
        self._dismiss_welcome_modal()

        # try find obvious export btn
        direct_export_labels = [
            r"export\s+to\s+csv",
            r"download\s+csv",
            r"export\s+data",
        ]
        for pattern in direct_export_labels:
            try:
                with self.page.expect_download(timeout=4_000) as download_info:
                    clicked = self._click_first([
                        self.page.get_by_role("button", name=re.compile(pattern, re.I)),
                        self.page.get_by_role("menuitem", name=re.compile(pattern, re.I)),
                        self.page.get_by_text(re.compile(pattern, re.I)),
                    ])
                if clicked:
                    rows = self._parse_csv_download(download_info.value)
                    step.update(f"downloaded {len(rows)} row(s)")
                    return rows
            except PlaywrightTimeoutError:
                self.page.keyboard.press("Escape")
                pass

        # check alternative export menus
        step.update("checking alternate export menus")
        action_candidates = self.page.locator(
            "button, [role='button'], [aria-label*='more' i], [aria-label*='action' i], [aria-label*='menu' i], [aria-label*='options' i]"
        )
        count = min(action_candidates.count(), 25)
        for i in range(count):
            try:
                candidate = action_candidates.nth(i)
                label = (candidate.get_attribute("aria-label") or "").lower()
                text = (candidate.inner_text(timeout=500) or "").lower().strip()
                if not any(word in f"{label} {text}" for word in ["more", "action", "menu", "option", "...", "⋯", "ellipsis"]):
                    continue
                candidate.click(timeout=1_000)
                self.page.wait_for_timeout(500)
                try:
                    with self.page.expect_download(timeout=3_000) as download_info:
                        clicked = self._click_first([
                            self.page.get_by_role("menuitem", name=re.compile(r"export\s+to\s+csv|download\s+csv|export\s+data", re.I)),
                            self.page.get_by_text(re.compile(r"export\s+to\s+csv|download\s+csv|export\s+data", re.I)),
                        ])
                    if clicked:
                        rows = self._parse_csv_download(download_info.value)
                        step.update(f"downloaded {len(rows)} row(s)")
                        return rows
                except PlaywrightTimeoutError:
                    self.page.keyboard.press("Escape")
            except Error:
                continue

        step.update("CSV export was not available")
        return []

    def fallback_dom_scrape(self, step: ProgressStep) -> list[dict[str, object]]:
        step.update("reading table headers")
        self._dismiss_welcome_modal()
        headers = self._grid_headers()
        if len(headers) < 5:
            raise ScrapeError(f"Could not determine QuickSight table headers from the dashboard: {headers}")

        rows_seen: set[tuple[str, ...]] = set()
        parsed: list[dict[str, object]] = []
        stagnant_passes = 0
        last_reported_rows = -1

        for pass_index in range(120):
            visible_rows = self._visible_grid_rows()

            before = len(rows_seen)
            for row in visible_rows:
                row_tuple = tuple(str(x).strip() for x in row)
                if row_tuple not in rows_seen:
                    rows_seen.add(row_tuple)
                    maybe = row_to_record(row_tuple, headers=headers)
                    if maybe:
                        parsed.append(maybe)

            if len(parsed) != last_reported_rows and (len(parsed) < 25 or len(parsed) // 25 > last_reported_rows // 25):
                step.update(f"collected {len(parsed)} row(s)")
                last_reported_rows = len(parsed)

            if len(rows_seen) == before:
                stagnant_passes += 1
            else:
                stagnant_passes = 0

            if stagnant_passes >= 3:
                break

            step.update(f"scroll pass {pass_index + 1} - {len(parsed)} row(s) collected")
            scrolled = self.page.evaluate(
                f"""
                () => {{
                  const container = document.querySelector('{GRID_SCROLL_CONTAINER_SELECTOR}');
                  if (!container) return false;
                  const nextTop = Math.min(
                    container.scrollTop + Math.max(200, container.clientHeight - 50),
                    container.scrollHeight - container.clientHeight,
                  );
                  const changed = nextTop > container.scrollTop;
                  container.scrollTop = nextTop;
                  return changed;
                }}
                """
            )
            if not scrolled:
                break
            self.page.wait_for_timeout(800)

        step.update(f"collected {len(parsed)} row(s)")
        return dedupe_records(parsed)

    def _best_effort_fill_login_field(self, selectors: Sequence[str], value: str) -> None:
        deadline = time.monotonic() + (self.settings.timeout_ms / 1_000)
        for selector in selectors:
            locator = self.page.locator(selector).first
            if locator.count() > 0:
                try:
                    locator.wait_for(state="visible", timeout=3_000)
                    locator.fill(value, timeout=3_000)
                    return
                except Error:
                    continue

        while time.monotonic() < deadline:
            for selector in selectors:
                locator = self.page.locator(selector).first
                try:
                    locator.wait_for(state="visible", timeout=1_000)
                    locator.fill(value, timeout=3_000)
                    return
                except (Error, PlaywrightTimeoutError):
                    continue
            self.page.wait_for_timeout(250)
        raise ScrapeError(f"Could not find a login field for selectors: {selectors}")

    def _click_first(self, locators: Iterable[Locator]) -> bool:
        for locator in locators:
            try:
                if locator.count() == 0:
                    continue
                locator.first.wait_for(state="visible", timeout=1_500)
                locator.first.click(timeout=1_500)
                return True
            except (Error, PlaywrightTimeoutError):
                continue
        return False

    def _submit_login_step(self) -> None:
        clicked = self._click_first([
            self.page.get_by_role("button", name=re.compile(r"next|continue|sign\s*in|log\s*in|submit", re.I)),
            self.page.locator("button[type='submit']"),
            self.page.locator("input[type='submit']"),
        ])
        if not clicked:
            raise ScrapeError("Login form was detected, but no submit control could be clicked.")

    def _is_dashboard_page(self) -> bool:
        return "quicksight.aws.amazon.com" in self.page.url and "/auth/" not in self.page.url

    def _wait_for_auth_state(self) -> bool:
        try:
            self.page.wait_for_function(
                f"""
                () => {{
                  const url = window.location.href;
                  if (url.includes('signin.aws') || (url.includes('quicksight.aws.amazon.com') && !url.includes('/auth/'))) {{
                    return true;
                  }}
                  return {self._selector_js_expr(PASSWORD_SELECTORS)};
                }}
                """,
                timeout=self.settings.timeout_ms,
            )
        except PlaywrightTimeoutError:
            return False
        return True

    def _wait_for_dashboard_page(self) -> bool:
        try:
            self.page.wait_for_function(
                """
                () => {
                  const url = window.location.href;
                  return url.includes('quicksight.aws.amazon.com') && !url.includes('/auth/');
                }
                """,
                timeout=self.settings.timeout_ms,
            )
        except PlaywrightTimeoutError:
            return False
        return True

    def _wait_for_dashboard_shell(self) -> None:
        self.page.wait_for_function(
            """
            () => {
              return !!(
                document.querySelector("[data-automation-id='welcome-modal']") ||
                document.querySelector("button[aria-label='Export']") ||
                document.querySelector("div.grid-container")
              );
            }
            """,
            timeout=self.settings.timeout_ms,
        )

    def _dismiss_welcome_modal(self) -> None:
        close_button = self.page.locator("[data-automation-id='welcome-modal-close-btn']").first
        if close_button.count() == 0:
            return
        try:
            close_button.click(timeout=3_000)
            self.page.wait_for_function(
                "() => !document.querySelector(\"[data-automation-id='welcome-modal']\")",
                timeout=5_000,
            )
        except (Error, PlaywrightTimeoutError):
            self.page.keyboard.press("Escape")
            self.page.wait_for_timeout(500)

    def _grid_headers(self) -> list[str]:
        headers = self.page.evaluate(
            """
            () => Array.from(document.querySelectorAll('div.column'))
              .map(el => (el.innerText || el.getAttribute('title') || '').trim())
              .filter(Boolean)
            """
        )
        return [str(header) for header in headers]

    def _visible_grid_rows(self) -> list[list[str]]:
        return self.page.evaluate(
            f"""
            () => {{
              const grid = document.querySelector('{GRID_BODY_SELECTOR}');
              if (!grid) return [];

              const rows = new Map();
              for (const cell of grid.querySelectorAll('div.cell')) {{
                const value = (cell.getAttribute('title') || cell.innerText || '').trim();
                if (!value) continue;

                const style = cell.getAttribute('style') || '';
                const topMatch = style.match(/top:\\s*([\\d.]+)px/i);
                const leftMatch = style.match(/left:\\s*([\\d.]+)px/i);
                if (!topMatch) continue;

                const top = topMatch[1];
                const left = leftMatch ? Number(leftMatch[1]) : Number.MAX_SAFE_INTEGER;
                const row = rows.get(top) || [];
                row.push({{ left, value }});
                rows.set(top, row);
              }}

              return Array.from(rows.entries())
                .sort((a, b) => Number(a[0]) - Number(b[0]))
                .map(([, cells]) => cells.sort((a, b) => a.left - b.left).map(cell => cell.value))
                .filter(row => row.length >= 5);
            }}
            """
        )

    def _selector_js_expr(self, selectors: Sequence[str]) -> str:
        quoted = ", ".join(json.dumps(selector) for selector in selectors)
        return f"[{quoted}].some(selector => !!document.querySelector(selector))"

    def _parse_csv_download(self, download: Download) -> list[dict[str, object]]:
        suggested = download.suggested_filename or f"export-{int(time.time())}.csv"
        destination = self.settings.downloads_dir / suggested
        destination.parent.mkdir(parents=True, exist_ok=True)
        download.save_as(str(destination))
        return parse_csv_file(destination)


HEADER_ALIASES = {
    "date": "date",
    "code": "code",
    "affiliate code": "code",
    "partner code": "code",
    "registrations": "registrations",
    "registration": "registrations",
    "new user registrations": "registrations",
    "ftds": "ftds",
    "ftd": "ftds",
    "first time depositors": "ftds",
    "state": "state",
    "us state": "state",
}


def normalize_header(header: str) -> str:
    return re.sub(r"\s+", " ", header.strip().lower())


def parse_csv_file(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ScrapeError(f"Downloaded CSV {path.name} did not contain headers.")

        remapped: dict[str, str] = {}
        for source in reader.fieldnames:
            alias = HEADER_ALIASES.get(normalize_header(source))
            if alias:
                remapped[source] = alias

        rows: list[dict[str, object]] = []
        for raw in reader:
            candidate: dict[str, object] = {}
            for source, target in remapped.items():
                candidate[target] = raw.get(source, "")
            if set(candidate) >= set(REQUIRED_FIELDS):
                normalized = normalize_record(candidate)
                if normalized:
                    rows.append(normalized)

    return dedupe_records(rows)


def row_to_record(row: Sequence[str], headers: Sequence[str] | None = None) -> dict[str, object] | None:
    cleaned = [str(x).strip() for x in row if str(x).strip()]
    if headers:
        normalized_headers = [HEADER_ALIASES.get(normalize_header(header)) for header in headers]
        candidate = {
            alias: value
            for alias, value in zip(normalized_headers, cleaned, strict=False)
            if alias
        }
        if set(candidate) >= set(REQUIRED_FIELDS):
            return normalize_record(candidate)

    for start in range(0, max(1, len(cleaned) - 4)):
        window = cleaned[start : start + 5]
        if len(window) < 5:
            continue
        candidate = dict(zip(REQUIRED_FIELDS, window, strict=True))
        normalized = normalize_record(candidate)
        if normalized:
            return normalized
    return None


def normalize_record(raw: dict[str, object]) -> dict[str, object] | None:
    try:
        date = normalize_date(str(raw["date"]).strip())
        code = str(raw["code"]).strip()
        registrations = int(str(raw["registrations"]).replace(",", "").strip())
        ftds = int(str(raw["ftds"]).replace(",", "").strip())
        state = str(raw["state"]).strip().upper()
    except (KeyError, ValueError, TypeError):
        return None

    if not DATE_RE.match(date):
        return None
    if not code:
        return None
    if not STATE_RE.match(state):
        return None

    return {
        "date": date,
        "code": code,
        "registrations": registrations,
        "ftds": ftds,
        "state": state,
    }


def normalize_date(raw_date: str) -> str:
    if DATE_RE.match(raw_date):
        return raw_date

    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {raw_date}")


def dedupe_records(records: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[tuple[object, ...]] = set()
    out: list[dict[str, object]] = []
    for record in records:
        key = tuple(record[field] for field in REQUIRED_FIELDS)
        if key in seen:
            continue
        seen.add(key)
        out.append(record)
    return sorted(out, key=lambda r: (str(r["date"]), str(r["code"]), str(r["state"])))


def validate_records(records: Sequence[dict[str, object]]) -> None:
    if not isinstance(records, Sequence):
        raise ScrapeError("Output must be a JSON array.")
    for i, record in enumerate(records):
        keys = list(record.keys())
        if keys != REQUIRED_FIELDS:
            raise ScrapeError(f"Record {i} keys were {keys}, expected {REQUIRED_FIELDS} in that exact order.")
        if not isinstance(record["registrations"], int) or not isinstance(record["ftds"], int):
            raise ScrapeError(f"Record {i} has non-integer numeric fields: {record}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape a QuickSight dashboard and write output.json")
    parser.add_argument("--dashboard-url", default=os.environ.get("VAULT_DASHBOARD_URL", "https://us-east-1.quicksight.aws.amazon.com/sn/account/vault-network-inteview/dashboards/3b1cdcb4-3d00-4612-9ff3-4940982b2e99"))
    parser.add_argument("--username", default=os.environ.get("VAULT_USERNAME", ""))
    parser.add_argument("--password", default=os.environ.get("VAULT_PASSWORD", ""))
    parser.add_argument("--output", default="output.json")
    parser.add_argument("--downloads-dir", default=".downloads")
    parser.add_argument("--headful", action="store_true", help="Run Chromium with a visible window for debugging")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = ConsoleLogger()

    if not args.username or not args.password:
        print("Missing credentials. Set VAULT_USERNAME and VAULT_PASSWORD or pass --username/--password.", file=sys.stderr)
        return 2

    settings = Settings(
        dashboard_url=args.dashboard_url,
        username=args.username,
        password=args.password,
        output_path=Path(args.output).resolve(),
        downloads_dir=Path(args.downloads_dir).resolve(),
        headless=not args.headful,
    )

    settings.downloads_dir.mkdir(parents=True, exist_ok=True)
    settings.output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sync_playwright() as p:
            with logger.step("Launching Chromium browser") as step:
                step.update("starting Playwright Chromium")
                browser = p.chromium.launch(headless=settings.headless)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                page.set_default_timeout(settings.timeout_ms)
                page.set_default_navigation_timeout(settings.timeout_ms)
                step.update("browser is ready")

            records = QuickSightScraper(page, settings, logger).run()

            with logger.step("Validating records") as step:
                validate_records(records)
                step.update(f"{len(records)} row(s) passed validation")

            with logger.step("Writing output file") as step:
                settings.output_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
                step.update(f"saved {len(records)} row(s) to {settings.output_path.name}")

            with logger.step("Closing browser") as step:
                browser.close()
                step.update("browser closed")
    except ScrapeError as exc:
        print(f"Scrape failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc: 
        print(f"Unexpected failure: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote {settings.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
