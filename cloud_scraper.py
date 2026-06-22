"""
cloud_scraper.py
=================
Headless, single-run refactor of the CaddyComps scraper + delta-report
mailer, built to run unattended on GitHub Actions (or any other headless,
cron-triggered runner) -- no GUI, no background scheduler, no on-disk
credential file.

What changed vs. the desktop app:
  - No tkinter / ttkbootstrap, no `schedule` background loop. The script
    runs top-to-bottom exactly once per invocation and exits. Scheduling
    (Wed/Sun 19:58 GMT) is GitHub Actions' job now, not this script's.
  - No caddy_credentials.json. SENDER_EMAIL, SENDER_PASSWORD, and
    RECEIVER_EMAIL are read from environment variables -- set these as
    encrypted repository secrets in your GitHub Actions workflow.
  - Logging goes to stdout via print() instead of a Tk text widget, so it
    shows up directly in the Actions run log.
  - caddycomps_master_data.csv is still read/appended exactly as before.
    This script does NOT commit it back to the repo -- that's the workflow
    step you're already planning to handle on the Actions side.
  - See get_comparison_cutoff() below for how the old "last_email_sent"
    tracking (previously stored in the JSON file) is replaced.

CI job prerequisites (install these before running this script):
    pip install pandas playwright
    playwright install --with-deps chromium

Required environment variables (set as GitHub Actions secrets):
    SENDER_EMAIL, SENDER_PASSWORD (a Gmail App Password), RECEIVER_EMAIL

Note: requires Python 3.10+ (the `str | None` type hints below, carried
over unchanged from the original script, need it).
"""

import asyncio
import os
import re
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from playwright.async_api import async_playwright

# ── Persistence Config ─────────────────────────────────────────────────────────
MASTER_DATA_FILE = "caddycomps_master_data.csv"


# ── Logging ────────────────────────────────────────────────────────────────────
def log(message: str, level: str = "info"):
    """
    Stand-in for the old Tk text-widget logger. Every call becomes one
    timestamped line on stdout, which is exactly what shows up in the
    GitHub Actions run log -- no GUI required.
    """
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level.upper()}] {message}")


# ── Master Data Persistence (unchanged) ──────────────────────────────────────────
def append_to_master(scraped_data, scraped_at: str):
    """Append a scrape run (with timestamp) to the master CSV."""
    rows = [{"scraped_at": scraped_at, **r} for r in scraped_data]
    df_new = pd.DataFrame(rows)
    if os.path.exists(MASTER_DATA_FILE):
        df_existing = pd.read_csv(MASTER_DATA_FILE)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    df_combined.to_csv(MASTER_DATA_FILE, index=False)


def load_delta_data(last_email_sent: str | None):
    """
    Return rows from the master CSV scraped after last_email_sent.
    If last_email_sent is None, return all rows.
    """
    if not os.path.exists(MASTER_DATA_FILE):
        return pd.DataFrame()
    df = pd.read_csv(MASTER_DATA_FILE)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    if last_email_sent:
        cutoff = pd.to_datetime(last_email_sent)
        df = df[df["scraped_at"] > cutoff]
    return df


def get_comparison_cutoff():
    """
    Replaces the old caddy_credentials.json 'last_email_sent' tracking.

    The desktop app persisted a last_email_sent timestamp to disk and used
    it as the cutoff for load_delta_data(). Cloud runners don't keep local
    state between runs -- there's nowhere to write that timestamp, and
    nothing to read it back from, EXCEPT caddycomps_master_data.csv itself,
    which already survives across runs because your workflow commits it
    back to the repo. So instead of "time since the last email", the
    cutoff here is derived as "the run before the previous run".

    That distinction matters: this script always scrapes once and emails
    once in the same execution. If the cutoff were simply the previous
    run's timestamp, this run's delta window would contain only this run's
    own single data point, and the earliest-vs-latest comparison inside
    build_email_html() would diff that point against itself -- Ticket
    Delta = 0 for every competition, every time, even if sales moved.
    Looking one run further back gives a window of exactly
    [previous run, this run], producing the real run-over-run (i.e.
    Wednesday/Sunday-over-Wednesday/Sunday) delta the report is meant to
    show.

    IMPORTANT: must be called BEFORE scrape_caddycomps() runs, because
    that function (via append_to_master) writes this run's row into
    MASTER_DATA_FILE. Call this after scraping and "the previous run"
    would actually be this run.

    Returns None for the first two runs ever -- there isn't two-runs-back
    of history yet -- in which case load_delta_data(None) falls back to
    returning everything available, which is the right behaviour for an
    initial report.
    """
    if not os.path.exists(MASTER_DATA_FILE):
        return None
    existing_df = pd.read_csv(MASTER_DATA_FILE)
    if existing_df.empty or "scraped_at" not in existing_df.columns:
        return None
    previous_timestamps = sorted(existing_df["scraped_at"].unique())
    return previous_timestamps[-2] if len(previous_timestamps) >= 2 else None


# ── Scraper (UNCHANGED core logic) ────────────────────────────────────────────────
async def scrape_caddycomps(log_callback):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        log_callback("Navigating to CaddyComps...", "info")
        await page.goto(
            "https://caddycomps.com/competitions",
            wait_until="domcontentloaded",
            timeout=60000,
        )

        scraped_data = []
        page_num     = 1
        total_value  = 0.0
        scraped_at   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        while True:
            log_callback(f"Scraping page {page_num}...", "info")
            await page.wait_for_selector("li.product", state="visible", timeout=30000)
            product_locators = page.locator("li.product")
            count = await product_locators.count()

            for i in range(count):
                product = product_locators.nth(i)
                try:
                    name = await product.locator(".woocommerce-loop-product__title").inner_text()

                    price_locator = product.locator("ins .amount bdi")
                    if await price_locator.count() == 0:
                        price_locator = product.locator(".price .amount bdi")

                    price_text = await price_locator.last.inner_text()
                    price      = float(re.sub(r"[^\d.]", "", price_text))

                    max_text      = await product.locator('span[class^="zapc-refresh-max-"]').inner_text()
                    total_tickets = int(re.sub(r"\D", "", max_text))

                    sold_text    = await product.locator('span[class^="zapc-refresh-sold-"]').inner_text()
                    sold_tickets = int(re.sub(r"\D", "", sold_text))

                    pct_text       = await product.locator('span[class^="zapc-refresh-percentage-"]').inner_text()
                    sold_percentage = float(re.sub(r"[^\d.]", "", pct_text))

                    total_sold_value = round(price * sold_tickets, 2)
                    total_value     += total_sold_value

                    scraped_data.append({
                        "Competition Name":     name,
                        "Price (£)":            price,
                        "Total Tickets":        total_tickets,
                        "Sold Tickets":         sold_tickets,
                        "Sold Percentage (%)":  sold_percentage,
                        "Total Sold Value (£)": total_sold_value,
                    })
                except Exception:
                    pass

            next_button = page.locator("a.next.page-numbers")
            if await next_button.count() > 0 and await next_button.is_visible():
                page_num += 1
                await next_button.click()
                await page.wait_for_load_state("domcontentloaded")
            else:
                break

        await browser.close()

        # Save point-in-time CSV
        filename = f"caddycomps_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        pd.DataFrame(scraped_data).to_csv(filename, index=False)

        # Append to master log
        append_to_master(scraped_data, scraped_at)

        log_callback(f"Success! Extracted {len(scraped_data)} records to {filename}.", "success")
        log_callback(f"Master data log updated: {MASTER_DATA_FILE}", "info")
        return filename, scraped_data, total_value


# ── Email Builder (UNCHANGED) ──────────────────────────────────────────────────────
def build_email_html(delta_df: pd.DataFrame, last_email_sent: str | None, now_str: str):
    """
    Build a rich HTML email from delta_df (rows scraped since last_email_sent).
    For each competition we show its latest scraped values and, if multiple
    scrape runs exist for it, a ticket-sales delta vs the earliest run in range.
    """
    if delta_df.empty:
        return "<html><body><p>No new scrape data since the last report.</p></body></html>"

    # Latest state per competition
    latest = (
        delta_df.sort_values("scraped_at")
                .groupby("Competition Name", as_index=False)
                .last()
    )
    # Earliest state per competition in the window (for delta calc)
    earliest = (
        delta_df.sort_values("scraped_at")
                .groupby("Competition Name", as_index=False)
                .first()
    )

    total_competitions = len(latest)
    total_revenue      = latest["Total Sold Value (£)"].sum()
    avg_sold_pct       = round(latest["Sold Percentage (%)"].mean(), 2)
    scrape_runs        = delta_df["scraped_at"].nunique()

    period_from = (
        pd.to_datetime(last_email_sent).strftime("%b %d, %Y %I:%M %p")
        if last_email_sent else "All time"
    )
    period_to   = pd.to_datetime(now_str).strftime("%b %d, %Y %I:%M %p")
    generated   = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

    top5 = latest.nlargest(5, "Total Sold Value (£)")

    # ── Merge latest & earliest for delta ──
    merged = latest.merge(
        earliest[["Competition Name", "Sold Tickets"]],
        on="Competition Name",
        suffixes=("", "_prev"),
    )
    merged["Ticket Delta"] = merged["Sold Tickets"] - merged["Sold Tickets_prev"]

    # ── Table rows ──
    rows_html = ""
    for _, r in merged.iterrows():
        bar  = min(int(r["Sold Percentage (%)"]), 100)
        col  = "#629755" if bar >= 75 else "#ffc66d" if bar >= 40 else "#cc666e"
        delta_val = int(r["Ticket Delta"])
        delta_html = (
            f'<span style="color:#629755;">+{delta_val}</span>' if delta_val > 0
            else f'<span style="color:#888;">—</span>' if delta_val == 0
            else f'<span style="color:#cc666e;">{delta_val}</span>'
        )
        rows_html += f"""
        <tr>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;">{r['Competition Name']}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;text-align:center;">£{r['Price (£)']:.2f}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;text-align:center;">{r['Total Tickets']}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;text-align:center;">{int(r['Sold Tickets'])}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;text-align:center;">{delta_html}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;text-align:center;">
            <div style="background:#e0e0e0;border-radius:4px;height:10px;width:100px;display:inline-block;vertical-align:middle;">
              <div style="background:{col};width:{bar}px;height:10px;border-radius:4px;"></div>
            </div>
            &nbsp;{r['Sold Percentage (%)']:.1f}%
          </td>
          <td style="padding:8px 10px;border-bottom:1px solid #e8e8e8;text-align:center;font-weight:bold;">
            £{r['Total Sold Value (£)']:.2f}
          </td>
        </tr>"""

    top5_rows = ""
    for idx, (_, r) in enumerate(top5.iterrows(), 1):
        top5_rows += f"""
        <tr>
          <td style="padding:6px 10px;border-bottom:1px solid #e8e8e8;color:#888;">#{idx}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #e8e8e8;">{r['Competition Name']}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #e8e8e8;text-align:right;font-weight:bold;color:#629755;">£{r['Total Sold Value (£)']:.2f}</td>
        </tr>"""

    html = f"""
    <html><body style="margin:0;padding:0;font-family:'Segoe UI',Arial,sans-serif;background:#f4f4f4;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:30px 0;">
      <tr><td align="center">
        <table width="720" cellpadding="0" cellspacing="0"
               style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

          <!-- Header -->
          <tr>
            <td style="background:#2c3e50;padding:30px 40px;text-align:center;">
              <h1 style="margin:0;color:#fff;font-size:22px;letter-spacing:1px;">CaddyComps Sales Report</h1>
              <p style="margin:6px 0 0;color:#aab7c4;font-size:13px;">Generated on {generated}</p>
              <p style="margin:4px 0 0;color:#7f8c8d;font-size:12px;">
                Period: <strong style="color:#aab7c4;">{period_from}</strong>
                &nbsp;→&nbsp;
                <strong style="color:#aab7c4;">{period_to}</strong>
                &nbsp;·&nbsp;{scrape_runs} scrape run(s)
              </p>
            </td>
          </tr>

          <!-- Summary Cards -->
          <tr>
            <td style="padding:25px 40px 10px;">
              <table width="100%" cellpadding="0" cellspacing="10">
                <tr>
                  <td style="background:#f0f7ff;border-left:4px solid #4a90d9;border-radius:4px;padding:15px 20px;width:30%;">
                    <p style="margin:0;font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;">Competitions Tracked</p>
                    <p style="margin:5px 0 0;font-size:26px;font-weight:bold;color:#2c3e50;">{total_competitions}</p>
                  </td>
                  <td style="background:#f0fff4;border-left:4px solid #629755;border-radius:4px;padding:15px 20px;width:35%;">
                    <p style="margin:0;font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;">Total Revenue (Period)</p>
                    <p style="margin:5px 0 0;font-size:26px;font-weight:bold;color:#629755;">£{total_revenue:,.2f}</p>
                  </td>
                  <td style="background:#fffbf0;border-left:4px solid #ffc66d;border-radius:4px;padding:15px 20px;width:30%;">
                    <p style="margin:0;font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;">Avg. Sold Percentage</p>
                    <p style="margin:5px 0 0;font-size:26px;font-weight:bold;color:#e6a817;">{avg_sold_pct}%</p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Top 5 -->
          <tr>
            <td style="padding:20px 40px 10px;">
              <h2 style="font-size:15px;color:#2c3e50;border-bottom:2px solid #f0f0f0;padding-bottom:8px;">
                🏆 Top 5 Competitions by Revenue
              </h2>
              <table width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;">
                <tr style="background:#f8f8f8;">
                  <th style="padding:8px 10px;text-align:left;color:#888;font-weight:600;">#</th>
                  <th style="padding:8px 10px;text-align:left;color:#888;font-weight:600;">Competition</th>
                  <th style="padding:8px 10px;text-align:right;color:#888;font-weight:600;">Revenue</th>
                </tr>
                {top5_rows}
              </table>
            </td>
          </tr>

          <!-- Full Table -->
          <tr>
            <td style="padding:20px 40px 10px;">
              <h2 style="font-size:15px;color:#2c3e50;border-bottom:2px solid #f0f0f0;padding-bottom:8px;">
                📋 Full Competition Breakdown
                <span style="font-size:11px;color:#aaa;font-weight:normal;">
                  (Ticket Delta = change since period start)
                </span>
              </h2>
              <table width="100%" cellpadding="0" cellspacing="0"
                     style="font-size:12px;border-collapse:collapse;">
                <tr style="background:#2c3e50;color:#fff;">
                  <th style="padding:10px;text-align:left;">Competition</th>
                  <th style="padding:10px;text-align:center;">Price</th>
                  <th style="padding:10px;text-align:center;">Total</th>
                  <th style="padding:10px;text-align:center;">Sold</th>
                  <th style="padding:10px;text-align:center;">Δ Tickets</th>
                  <th style="padding:10px;text-align:center;">Progress</th>
                  <th style="padding:10px;text-align:center;">Revenue</th>
                </tr>
                {rows_html}
              </table>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#f8f8f8;padding:20px 40px;text-align:center;border-top:1px solid #e8e8e8;">
              <p style="margin:0;font-size:12px;color:#aaa;">
                Full data CSV is attached to this email.
              </p>
              <p style="margin:6px 0 0;font-size:11px;color:#ccc;">
                CaddyComps Scraper Engine &bull; Automated Delta Report
              </p>
            </td>
          </tr>

        </table>
      </td></tr>
    </table>
    </body></html>
    """
    return html


# ── Email Sender (credentials now passed in from env vars; no JSON state) ───────────
def send_email_report(sender_email, sender_password, receiver_email,
                      last_email_sent, log_callback):
    """
    Build a delta report from master data and send it.

    NOTE: the original desktop version called update_last_email_sent() here
    to persist a timestamp into caddy_credentials.json. That file doesn't
    exist in the cloud version -- there's nothing to write it to, and
    nothing needs to read it back either: get_comparison_cutoff() derives
    the next run's comparison baseline directly from
    caddycomps_master_data.csv, which is already being persisted.
    """
    try:
        now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        delta_df  = load_delta_data(last_email_sent)

        if delta_df.empty:
            log_callback("No new data since last email — nothing to send.", "warning")
            return

        log_callback(
            f"Building delta report ({len(delta_df)} rows across "
            f"{delta_df['scraped_at'].nunique()} scrape run(s))...",
            "info",
        )

        # Export delta CSV for attachment
        delta_csv = f"caddycomps_delta_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        delta_df.to_csv(delta_csv, index=False)

        period_from = (
            pd.to_datetime(last_email_sent).strftime("%b %d, %Y")
            if last_email_sent else "All time"
        )
        period_to = pd.to_datetime(now_str).strftime("%b %d, %Y")

        msg            = MIMEMultipart("alternative")
        msg["Subject"] = (
            f"CaddyComps Delta Report | {period_from} → {period_to}"
        )
        msg["From"] = sender_email
        msg["To"]   = receiver_email

        html_body = build_email_html(delta_df, last_email_sent, now_str)
        msg.attach(MIMEText(html_body, "html"))

        # Attach delta CSV
        with open(delta_csv, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(delta_csv)}"',
        )
        msg.attach(part)
        log_callback(f"Attached: {delta_csv}", "info")

        log_callback("Connecting to Gmail SMTP server...", "info")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        log_callback(f"Email sent successfully to {receiver_email}.", "success")
        return now_str

    except smtplib.SMTPAuthenticationError:
        log_callback(
            "Email failed: Authentication error. Use a Gmail App Password.", "error"
        )
    except smtplib.SMTPException as e:
        log_callback(f"Email failed (SMTP): {str(e)}", "error")
    except Exception as e:
        log_callback(f"Email failed: {str(e)}", "error")

    return None


# ── Entrypoint ─────────────────────────────────────────────────────────────────────
def main():
    log("=== CaddyComps Cloud Scraper: run starting ===", "info")

    # 1. Pull credentials from the environment (GitHub Actions secrets)
    sender_email    = os.environ.get("SENDER_EMAIL")
    sender_password = os.environ.get("SENDER_PASSWORD")
    receiver_email  = os.environ.get("RECEIVER_EMAIL")

    missing = [
        name for name, value in [
            ("SENDER_EMAIL", sender_email),
            ("SENDER_PASSWORD", sender_password),
            ("RECEIVER_EMAIL", receiver_email),
        ] if not value
    ]
    if missing:
        log(f"Missing required environment variable(s): {', '.join(missing)}", "error")
        log("Set these as encrypted secrets in your GitHub Actions workflow.", "error")
        raise SystemExit(1)

    # 2. Capture the comparison cutoff BEFORE this run's data is scraped/appended
    #    (see get_comparison_cutoff()'s docstring for why ordering matters here)
    comparison_cutoff = get_comparison_cutoff()
    if comparison_cutoff:
        log(f"Comparing against previous run: {comparison_cutoff}", "info")
    else:
        log("No prior run history found — this report will use all available data.", "info")

    # 3. Scrape CaddyComps (this also appends the new row(s) to MASTER_DATA_FILE)
    try:
        filename, scraped_data, total_value = asyncio.run(scrape_caddycomps(log))
    except Exception as e:
        log(f"Scraping failed: {e}", "error")
        raise SystemExit(1)

    log(
        f"Scrape complete: {len(scraped_data)} competitions, "
        f"£{total_value:,.2f} total sold value.",
        "info",
    )

    # 4. Build and send the delta report email
    sent_timestamp = send_email_report(
        sender_email=sender_email,
        sender_password=sender_password,
        receiver_email=receiver_email,
        last_email_sent=comparison_cutoff,
        log_callback=log,
    )

    if not sent_timestamp:
        log("=== Run finished, but the email did not send. Check logs above. ===", "error")
        raise SystemExit(1)

    log("=== CaddyComps Cloud Scraper: run complete. ===", "success")


if __name__ == "__main__":
    main()
