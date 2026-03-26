import html
import json
import os
from urllib.parse import parse_qs, quote, unquote, urlencode
from wsgiref.simple_server import make_server

from webapp import dashboard_data


DEFAULT_DB_URL = os.getenv("DATABASE_URL", "sqlite:///dividends.db")


def _money(value):
    return f"${value:,.2f}"


def _pct(value):
    if value is None:
        return ""
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def _text(value):
    if value is None:
        return ""
    return html.escape(str(value))


def _account_href(account):
    return f"/accounts/{quote(str(account), safe='')}"


def _security_href(security):
    return f"/securities/{quote(str(security), safe='')}"


def _dashboard_href(year=None):
    return "/" if not year else f"/?year={quote(str(year), safe='')}"


def _account_label(display_name, account):
    primary = _text(display_name or account)
    if display_name and display_name != account:
        return f"{primary}<div class='subtle'>{_text(account)}</div>"
    return primary


def _render_monthly_chart(monthly_income):
    if not monthly_income:
        return "<p>No monthly income data available.</p>"

    values = [row["total"] for row in monthly_income]
    max_value = max(values) or 1
    width = 780
    height = 220
    left = 42
    bottom = 24
    usable_width = width - left - 10
    usable_height = height - 20 - bottom
    step_x = usable_width / max(len(monthly_income) - 1, 1)

    points = []
    for idx, row in enumerate(monthly_income):
        x = left + idx * step_x
        y = 20 + usable_height - (row["total"] / max_value) * usable_height
        points.append((x, y, row))

    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)
    labels = []
    for x, _, row in points[:: max(1, len(points) // 6)]:
        labels.append(
            f"<text x='{x:.1f}' y='{height - 4}' text-anchor='middle'>{_text(row['month'])}</text>"
        )

    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart" role="img" aria-label="Monthly income">
      <line x1="{left}" y1="{height-bottom}" x2="{width-10}" y2="{height-bottom}" class="axis" />
      <line x1="{left}" y1="20" x2="{left}" y2="{height-bottom}" class="axis" />
      <polyline fill="none" stroke="var(--accent)" stroke-width="3" points="{polyline}" />
      {''.join(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.5' fill='var(--accent)'><title>{_text(row['month'])}: {_money(row['total'])}</title></circle>" for x, y, row in points)}
      {''.join(labels)}
    </svg>
    """


def _base_page(title, body):
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_text(title)}</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: #fffaf1;
      --ink: #1f2a1f;
      --muted: #5f6b5f;
      --accent: #1d6b52;
      --line: #d7cfbf;
      --shadow: rgba(38, 42, 31, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff6dc 0, transparent 32%),
        linear-gradient(180deg, #f7f2e8 0%, #efe9db 100%);
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 28px 20px 48px; }}
    .topbar {{ display: flex; justify-content: space-between; gap: 16px; align-items: end; margin-bottom: 24px; }}
    .brand h1 {{ margin: 0; font-size: 2.1rem; }}
    .brand p {{ margin: 6px 0 0; color: var(--muted); }}
    .nav {{ display: flex; gap: 14px; flex-wrap: wrap; }}
    .nav a {{ padding: 8px 10px; border: 1px solid var(--line); border-radius: 999px; background: rgba(255,255,255,0.5); }}
    .hero {{ background: var(--panel); border: 1px solid var(--line); border-radius: 22px; padding: 22px; box-shadow: 0 10px 30px var(--shadow); margin-bottom: 22px; }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 20px; padding: 18px; box-shadow: 0 10px 30px var(--shadow); }}
    a.card {{ display: block; color: inherit; }}
    .metric {{ grid-column: span 3; }}
    .metric .label {{ color: var(--muted); font-size: 0.92rem; }}
    .metric .value {{ font-size: 1.8rem; margin-top: 6px; font-weight: 700; }}
    .wide {{ grid-column: span 8; }}
    .side {{ grid-column: span 4; }}
    .full {{ grid-column: 1 / -1; }}
    h2, h3 {{ margin-top: 0; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-size: 0.92rem; }}
    .money {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .tag {{ display: inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--line); background: #f5f1e8; font-size: 0.85rem; }}
    .meta-tags {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }}
    .subtle {{ color: var(--muted); font-size: 0.84rem; margin-top: 2px; }}
    .delta-pos {{ color: #1d6b52; font-weight: 600; }}
    .delta-neg {{ color: #9d3d2f; font-weight: 600; }}
    .compact-table {{ table-layout: fixed; }}
    .compact-table th, .compact-table td {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; vertical-align: middle; }}
    .compact-table .col-date {{ width: 96px; }}
    .compact-table .col-security {{ width: 96px; }}
    .compact-table .col-account {{ width: 170px; }}
    .compact-table .col-type {{ width: 110px; }}
    .compact-table .col-amount {{ width: 105px; }}
    .compact-table .col-qualified {{ width: 86px; }}
    .compact-table .col-source {{ width: 110px; }}
    .report-wrap {{ overflow-x: auto; }}
    .report-table {{ min-width: 1220px; table-layout: fixed; font-size: 0.94rem; }}
    .report-table th, .report-table td {{ white-space: nowrap; padding: 10px 12px; border-right: 1px solid var(--line); }}
    .report-table th:last-child, .report-table td:last-child {{ border-right: none; }}
    .report-table thead th {{ position: sticky; top: 0; background: #f7f1e5; z-index: 1; }}
    .report-table tbody tr:nth-child(even) td {{ background: rgba(255,255,255,0.35); }}
    .report-table td:first-child, .report-table th:first-child {{ position: sticky; left: 0; background: #fffaf1; z-index: 1; min-width: 140px; }}
    .report-table thead th:first-child {{ background: #f7f1e5; z-index: 2; }}
    .summary-row td {{ background: #f4ede0; font-weight: 600; }}
    .total-row td {{ background: #ece3d2; font-weight: 700; border-top: 2px solid var(--line); }}
    .compact-table .col-action {{ width: 140px; }}
    .compact-table .col-file {{ width: 320px; }}
    .path {{ display: inline-block; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: monospace; font-size: 0.88rem; }}
    .chart {{ width: 100%; height: auto; }}
    .axis {{ stroke: #b7ae9c; stroke-width: 1; }}
    .filters {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }}
    .filters label {{ display: block; font-size: 0.9rem; color: var(--muted); margin-bottom: 4px; }}
    .filters input, .filters select {{ width: 100%; padding: 9px 10px; border-radius: 10px; border: 1px solid var(--line); background: #fffdf8; }}
    .actions {{ display: flex; gap: 10px; align-items: center; }}
    .button {{ cursor: pointer; padding: 10px 14px; border-radius: 12px; border: 1px solid var(--accent); background: var(--accent); color: white; }}
    .button.secondary {{ background: transparent; color: var(--accent); }}
    .footer {{ margin-top: 26px; color: var(--muted); font-size: 0.9rem; }}
    @media (max-width: 900px) {{
      .metric, .wide, .side {{ grid-column: 1 / -1; }}
      .filters {{ grid-template-columns: 1fr; }}
      .topbar {{ align-items: start; flex-direction: column; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <h1>dgi-dash</h1>
        <p>Read-only income dashboard on top of the local SQLite source of truth.</p>
      </div>
      <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/reports/monthly-income">Monthly</a>
        <a href="/accounts">Accounts</a>
        <a href="/securities">Securities</a>
        <a href="/transactions">Transactions</a>
      </div>
    </div>
    {body}
    <div class="footer">Database URL: {_text(DEFAULT_DB_URL)}</div>
  </div>
</body>
</html>"""


def render_dashboard(params=None, db_url=DEFAULT_DB_URL):
    params = params or {}
    selected_year = params.get("year", [""])[0] or None
    data = dashboard_data.get_dashboard_data(db_url=db_url, year=selected_year)
    totals = data["totals"]
    cards = [
        ("Transactions", str(totals["transactions"])),
        ("Accounts", str(totals["accounts"])),
        ("Securities", str(totals["securities"])),
        (
            f"{data['current_year']} Dividend Income",
            _money(totals["ytd_dividend_total"]),
            f"/transactions?txn_type=DIVIDEND&year={data['current_year']}",
        ),
        (f"{data['current_year']} Interest", _money(totals["ytd_interest_total"])),
        (f"{data['current_year']} Reinvestments", _money(totals["ytd_reinvestment_total"])),
    ]
    metrics_html = ""
    for card in cards:
        if len(card) == 3:
            label, value, href = card
            metrics_html += (
                f"<a class='card metric' href='{_text(href)}'>"
                f"<div class='label'>{_text(label)}</div><div class='value'>{_text(value)}</div></a>"
            )
        else:
            label, value = card
            metrics_html += (
                f"<div class='card metric'><div class='label'>{_text(label)}</div>"
                f"<div class='value'>{_text(value)}</div></div>"
            )

    account_rows = "".join(
        f"<tr><td><a href='{_account_href(row['account'])}'>{_account_label(row.get('display_name'), row['account'])}</a></td><td>{_text(row.get('tax_treatment'))}</td><td>{_text(row.get('institution'))}</td><td class='money'>{_money(row['total'])}</td><td>{_text(row['last_date'])}</td><td>{row['txn_count']}</td></tr>"
        for row in data["account_totals"]
    )
    security_rows = "".join(
        f"<tr><td><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td><td class='money'>{_money(row['total'])}</td><td>{row['txn_count']}</td><td>{_text(row['last_date'])}</td></tr>"
        for row in data["top_securities"]
    )
    recent_rows = "".join(
        f"<tr><td>{_text(row['date'])}</td><td><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td><td><a href='{_account_href(row['account'])}'>{_account_label(row.get('account_display_name'), row['account'])}</a></td><td><span class='tag'>{_text(row['txn_type'])}</span></td><td class='money'>{_money(row['amount'])}</td><td>{_text(row['source_system'])}</td></tr>"
        for row in data["recent_transactions"]
    )
    source_rows = "".join(
        f"<tr><td>{_text(row['source_system'])}</td><td>{_text(row['last_imported_at'])}</td><td>{_text(row['last_txn_date'])}</td><td>{row['file_count']}</td><td>{row['txn_count']}</td></tr>"
        for row in data["import_status"]["sources"]
    )
    coverage_rows = "".join(
        f"<tr><td><a href='{_account_href(row['account'])}'>{_text(row['account'])}</a></td><td>{_text(row['last_date'])}</td><td>{'' if row['days_since_last'] is None else row['days_since_last']}</td><td>{row['txn_count']}</td></tr>"
        for row in data["import_status"]["coverage"]
    )
    expected_rows = "".join(
        f"<tr>"
        f"<td>{row['day']}</td>"
        f"<td><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td>"
        f"<td><a href='{_account_href(row['account'])}'>{_text(row['account'])}</a></td>"
        f"<td class='money'>{_money(row['last_observed_amount']) if row['last_observed_amount'] is not None else ''}</td>"
        f"<td>{_text(str(row['last_observed_year']) + ' vs ' + str(row['previous_observed_year'])) if row['previous_observed_year'] else ''}</td>"
        f"<td class='{'delta-pos' if (row['trend_pct'] or 0) >= 0 else 'delta-neg'}'>{_pct(row['trend_pct'])}</td>"
        f"<td class='money'>{_money(row['expected_amount'])}</td>"
        f"<td>{row['years_seen']}</td>"
        f"</tr>"
        for row in data["expected_remainder"]["rows"]
    )

    body = f"""
    <div class="hero">
      <h2>Income Snapshot</h2>
      <p>As of {_text(data['as_of']) if data['as_of'] else 'n/a'}, this view summarizes dividend, reinvestment, and manual interest activity from the local database.</p>
      <form method="get" action="/" class="actions">
        <label for="year">Year</label>
        <select id="year" name="year">
          {''.join(f"<option value='{year}'{' selected' if year == data['current_year'] else ''}>{year}</option>" for year in data['available_years'])}
        </select>
        <button class="button" type="submit">Change Year</button>
      </form>
    </div>
    <div class="grid">
      {metrics_html}
      <div class="card wide">
        <h3>Monthly Dividend Income</h3>
        {_render_monthly_chart(data["monthly_income"])}
      </div>
      <div class="card side">
        <h3>Largest Accounts</h3>
        <table>
          <thead><tr><th>Account</th><th>Tax</th><th>Inst.</th><th class="money">Total</th><th>Last Date</th><th>Txns</th></tr></thead>
          <tbody>{account_rows}</tbody>
        </table>
      </div>
      <div class="card side">
        <h3>Top Payers</h3>
        <table>
          <thead><tr><th>Security</th><th class="money">Total</th><th>Txns</th><th>Last Date</th></tr></thead>
          <tbody>{security_rows}</tbody>
        </table>
      </div>
      <div class="card wide">
        <h3>Recent Transactions</h3>
        <table>
          <thead><tr><th>Date</th><th>Security</th><th>Account</th><th>Type</th><th class="money">Amount</th><th>Source</th></tr></thead>
          <tbody>{recent_rows}</tbody>
        </table>
      </div>
      <div class="card side">
        <h3>Import Status</h3>
        <p>Latest import at: <strong>{_text(data['import_status']['latest_imported_at'])}</strong></p>
        <p>Latest transaction date: <strong>{_text(data['import_status']['latest_txn_date'])}</strong></p>
        <table>
          <thead><tr><th>Source</th><th>Imported</th><th>Txn Date</th><th>Files</th><th>Txns</th></tr></thead>
          <tbody>{source_rows}</tbody>
        </table>
      </div>
      <div class="card wide">
        <h3>Coverage By Account</h3>
        <table>
          <thead><tr><th>Account</th><th>Last Date</th><th>Days Since Last</th><th>Txns</th></tr></thead>
          <tbody>{coverage_rows}</tbody>
        </table>
      </div>
      <div class="card full">
        <h3>Expected Remainder Of Month</h3>
        <p>Baseline uses the last observed payment pattern after day {data['expected_remainder']['as_of'].day}. The 3Y average is shown as a secondary smoothing reference. Estimated remainder by 3Y average: <strong>{_money(data['expected_remainder']['total_estimate'])}</strong>.</p>
        <table>
          <thead><tr><th>Day</th><th>Security</th><th>Account</th><th class="money">Baseline</th><th>Trend Basis</th><th>% Change</th><th class="money">3Y Avg</th><th>Years</th></tr></thead>
          <tbody>{expected_rows or "<tr><td colspan='8'>No historical end-of-month pattern found.</td></tr>"}</tbody>
        </table>
      </div>
    </div>
    """
    return _base_page("dgi-dash dashboard", body)


def render_transactions(params, db_url=DEFAULT_DB_URL):
    data = dashboard_data.get_transactions(
        db_url=db_url,
        account=params.get("account", [""])[0] or None,
        security=params.get("security", [""])[0] or None,
        txn_type=params.get("txn_type", [""])[0] or None,
        year=params.get("year", [""])[0] or None,
        limit=int(params.get("limit", ["200"])[0] or 200),
        sort=params.get("sort", ["date"])[0] or "date",
        direction=params.get("direction", ["desc"])[0] or "desc",
    )
    rows_html = "".join(
        f"<tr>"
        f"<td class='col-date' title='{_text(row['date'])}'>{_text(row['date'])}</td>"
        f"<td class='col-security' title='{_text(row['security'])}'><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td>"
        f"<td class='col-account' title='{_text(row['account'])}'><a href='{_account_href(row['account'])}'>{_text(row['account'])}</a></td>"
        f"<td class='col-type' title='{_text(row['txn_type'])}'>{_text(row['txn_type'])}</td>"
        f"<td class='money col-amount' title='{_money(row['amount'])}'>{_money(row['amount'])}</td>"
        f"<td class='col-qualified' title='{'yes' if row['is_qualified'] else ''}'>{'yes' if row['is_qualified'] else ''}</td>"
        f"<td class='col-source' title='source={_text(row['source_system'])}&#10;action={_text(row['raw_action'])}&#10;file={_text(row['source_file'])}&#10;tag={_text(row.get('reporting_tag'))}&#10;note={_text(row.get('annotation_note'))}'>{_text(row['source_system'])}</td>"
        f"</tr>"
        for row in data["rows"]
    )

    def options(items, selected):
        html_items = ["<option value=''>All</option>"]
        for item in items:
            sel = " selected" if str(item) == str(selected) else ""
            html_items.append(f"<option value='{_text(item)}'{sel}>{_text(item)}</option>")
        return "".join(html_items)

    def sort_href(column):
        next_direction = "asc"
        if filters["sort"] == column and filters["direction"] == "asc":
            next_direction = "desc"
        query = {
            "account": filters["account"],
            "security": filters["security"],
            "txn_type": filters["txn_type"],
            "year": filters["year"],
            "limit": filters["limit"],
            "sort": column,
            "direction": next_direction,
        }
        return "/transactions?" + urlencode(query)

    def sort_label(column, label):
        marker = ""
        if filters["sort"] == column:
            marker = " ▲" if filters["direction"] == "asc" else " ▼"
        return f"<a href='{_text(sort_href(column))}'>{_text(label)}{marker}</a>"

    filters = data["filters"]
    body = f"""
    <div class="hero">
      <h2>Transactions</h2>
      <p>Filter the read-only transaction table by account, security, year, and type. Source provenance is still available on hover.</p>
    </div>
    <div class="card full">
      <form method="get" action="/transactions">
        <div class="filters">
          <div><label>Account</label><select name="account">{options(data['options']['accounts'], filters['account'])}</select></div>
          <div><label>Security</label><input type="text" name="security" value="{_text(filters['security'])}" placeholder="e.g. XOM"></div>
          <div><label>Type</label><select name="txn_type">{options(data['options']['txn_types'], filters['txn_type'])}</select></div>
          <div><label>Year</label><select name="year">{options(data['options']['years'], filters['year'])}</select></div>
          <div><label>Limit</label><input type="number" min="1" max="1000" name="limit" value="{filters['limit']}"></div>
        </div>
        <div class="actions">
          <button class="button" type="submit">Apply Filters</button>
          <a class="button secondary" href="/transactions">Reset</a>
        </div>
      </form>
    </div>
    <div class="card full">
      <table class="compact-table">
        <thead>
          <tr>
            <th class='col-date'>{sort_label('date', 'Date')}</th><th class='col-security'>{sort_label('security', 'Security')}</th><th class='col-account'>{sort_label('account', 'Account')}</th><th class='col-type'>{sort_label('txn_type', 'Type')}</th><th class="money col-amount">{sort_label('amount', 'Amount')}</th><th class='col-qualified'>{sort_label('qualified', 'Qualified')}</th><th class='col-source'>{sort_label('source_system', 'Source')}</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """
    return _base_page("dgi-dash transactions", body)


def render_accounts(db_url=DEFAULT_DB_URL):
    data = dashboard_data.get_accounts_directory(db_url=db_url)
    rows = "".join(
        f"<tr><td><a href='{_account_href(row['account'])}'>{_account_label(row.get('display_name'), row['account'])}</a></td><td>{_text(row.get('tax_treatment'))}</td><td>{_text(row.get('institution'))}</td><td>{_text(row.get('account_group'))}</td><td>{'yes' if row.get('is_active') else 'no'}</td><td class='money'>{_money(row['total_amount'])}</td><td>{row['txn_count']}</td><td>{_text(row['last_date'])}</td></tr>"
        for row in data
    )
    body = f"""
    <div class="hero">
      <h2>Accounts</h2>
      <p>Account directory and drill-down entrypoint, including inactive and legacy accounts.</p>
    </div>
    <div class="card full">
      <table>
        <thead><tr><th>Account</th><th>Tax</th><th>Inst.</th><th>Group</th><th>Active</th><th class="money">Total</th><th>Txns</th><th>Last Date</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """
    return _base_page("dgi-dash accounts", body)


def render_account_detail(account_name, db_url=DEFAULT_DB_URL):
    data = dashboard_data.get_account_detail(account_name, db_url=db_url)
    if not data:
        return _base_page("Account Not Found", "<div class='hero'><h2>Account Not Found</h2></div>")

    metadata_rows = "".join(
        f"<tr><th>{label}</th><td>{_text(value)}</td></tr>"
        for label, value in [
            ("Display Name", data["metadata"]["display_name"]),
            ("Institution", data["metadata"]["institution"]),
            ("Last 4", data["metadata"]["account_last4"]),
            ("Group", data["metadata"]["account_group"]),
            ("Tax Treatment", data["metadata"]["tax_treatment"]),
            ("Active", "yes" if data["metadata"]["is_active"] else "no"),
            ("Notes", data["metadata"]["notes"]),
        ]
        if value not in (None, "")
    )
    by_type_rows = "".join(
        f"<tr><td>{_text(row['txn_type'])}</td><td>{row['txn_count']}</td><td class='money'>{_money(row['total_amount'])}</td></tr>"
        for row in data["by_type"]
    )
    top_security_rows = "".join(
        f"<tr><td><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td><td>{row['txn_count']}</td><td class='money'>{_money(row['total_amount'])}</td><td>{_text(row['last_date'])}</td></tr>"
        for row in data["top_securities"]
    )
    recent_rows = "".join(
        f"<tr><td>{_text(row['date'])}</td><td><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td><td>{_text(row['txn_type'])}</td><td class='money'>{_money(row['amount'])}</td><td>{_text(row['source_system'])}</td></tr>"
        for row in data["recent_transactions"]
    )
    body = f"""
    <div class="hero">
      <h2>{_text(data['metadata']['display_name'] or data['account'])}</h2>
      <div class="meta-tags">
        <span class="tag">{_text(data['account'])}</span>
        <span class="tag">{_text(data['metadata']['institution'])}</span>
        <span class="tag">{_text(data['metadata']['account_group'])}</span>
        <span class="tag">{_text(data['metadata']['tax_treatment'])}</span>
      </div>
      <p>From {_text(data['summary']['first_date'])} through {_text(data['summary']['last_date'])}, this account has {data['summary']['txn_count']} transactions totaling {_money(data['summary']['total_amount'])}.</p>
    </div>
    <div class="grid">
      <div class="card side">
        <h3>Metadata</h3>
        <table>
          <tbody>{metadata_rows}</tbody>
        </table>
      </div>
      <div class="card side">
        <h3>By Type</h3>
        <table>
          <thead><tr><th>Type</th><th>Txns</th><th class='money'>Total</th></tr></thead>
          <tbody>{by_type_rows}</tbody>
        </table>
      </div>
      <div class="card wide">
        <h3>Top Securities</h3>
        <table>
          <thead><tr><th>Security</th><th>Txns</th><th class='money'>Total</th><th>Last Date</th></tr></thead>
          <tbody>{top_security_rows}</tbody>
        </table>
      </div>
      <div class="card full">
        <h3>Recent Transactions</h3>
        <table>
          <thead><tr><th>Date</th><th>Security</th><th>Type</th><th class='money'>Amount</th><th>Source</th></tr></thead>
          <tbody>{recent_rows}</tbody>
        </table>
      </div>
    </div>
    """
    return _base_page(f"dgi-dash account {data['account']}", body)


def render_monthly_income_report(params, db_url=DEFAULT_DB_URL):
    income_mode = params.get("income_mode", ["combined"])[0] or "combined"
    tax_treatment = params.get("tax_treatment", [""])[0] or None
    data = dashboard_data.get_monthly_income_report(
        db_url=db_url,
        income_mode=income_mode,
        tax_treatment=tax_treatment,
    )

    def options(items, selected, include_all=True):
        html_items = ["<option value=''>All</option>"] if include_all else []
        for item in items:
            sel = " selected" if str(item) == str(selected) else ""
            html_items.append(f"<option value='{_text(item)}'{sel}>{_text(item)}</option>")
        return "".join(html_items)

    years = data["years"]

    if not years:
        body = """
        <div class="hero">
          <h2>Monthly Income</h2>
          <p>No matching data found for the selected filters.</p>
        </div>
        """
        return _base_page("dgi-dash monthly income", body)

    def render_matrix(rows, value_getter, *, top_rows=None, bottom_rows=None, money=True):
        header = "".join(f"<th class='money'>{year}</th>" for year in years)
        body_rows = []
        for row in top_rows or []:
            values = "".join(
                f"<td class='money'>{_money(value_getter(row, year)) if money else _text(value_getter(row, year))}</td>"
                for year in years
            )
            body_rows.append(f"<tr class='summary-row'><td>{_text(row['label'])}</td>{values}</tr>")
        for row in rows:
            values = "".join(
                f"<td class='money'>{_money(value_getter(row, year)) if money else _text(value_getter(row, year))}</td>"
                for year in years
            )
            body_rows.append(f"<tr><td>{_text(row['label'])}</td>{values}</tr>")
        for row in bottom_rows or []:
            values = "".join(
                f"<td class='money'>{_money(value_getter(row, year)) if money else _text(value_getter(row, year))}</td>"
                for year in years
            )
            body_rows.append(f"<tr class='total-row'><td>{_text(row['label'])}</td>{values}</tr>")
        return f"<div class='report-wrap'><table class='report-table'><thead><tr><th>Month</th>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"

    summary_rows = [
        {"label": "Avg. Monthly", "values": data["avg_monthly"]},
        {"label": "YoY Increase", "values": {year: (None if data["annual_yoy"][year] is None else data["annual_yoy"][year] * 100) for year in years}},
    ]
    total_rows = [
        {"label": "Total", "values": data["annual_totals"]},
    ]

    def money_or_percent(row, year):
        value = row["values"].get(year)
        if row["label"] == "YoY Increase":
            return "" if value is None else f"{value:,.1f}%"
        return _money(value or 0.0)

    t3m_table = render_matrix(data["rolling_three_month"], lambda row, year: row["values"].get(year, 0.0))
    summary_header = "".join(f"<th class='money'>{year}</th>" for year in years)
    summary_rows_html = "".join(
        f"<tr class='summary-row'><td>{_text(row['label'])}</td>" + "".join(
            f"<td class='money'>{money_or_percent(row, year)}</td>" for year in years
        ) + "</tr>"
        for row in summary_rows
    )
    total_rows_html = "".join(
        f"<tr class='total-row'><td>{_text(row['label'])}</td>" + "".join(
            f"<td class='money'>{_money(row['values'][year])}</td>" for year in years
        ) + "</tr>"
        for row in total_rows
    )
    monthly_body_rows = "".join(
        f"<tr><td>{_text(row['label'])}</td>" + "".join(
            f"<td class='money'>{_money(row['values'].get(year, 0.0))}</td>" for year in years
        ) + "</tr>"
        for row in data["monthly_matrix"]
    )
    monthly_table = (
        f"<div class='report-wrap'><table class='report-table'>"
        f"<thead><tr><th>Month</th>{summary_header}</tr></thead>"
        f"<tbody>{summary_rows_html}{monthly_body_rows}{total_rows_html}</tbody></table></div>"
    )

    body = f"""
    <div class="hero">
      <h2>Monthly Income</h2>
      <p>DB-backed replacement for the spreadsheet monthly tab. Monthly averages, YoY growth, month-by-month totals, and annual totals now live in one matrix.</p>
    </div>
    <div class="card full">
      <form method="get" action="/reports/monthly-income">
        <div class="filters">
          <div><label>Income</label><select name="income_mode">{options(['dividends', 'interest', 'combined'], data['income_mode'], include_all=False)}</select></div>
          <div><label>Tax Treatment</label><select name="tax_treatment">{options(data['available_tax_treatments'], data['tax_treatment'])}</select></div>
        </div>
        <div class="actions">
          <button class="button" type="submit">Apply Filters</button>
          <a class="button secondary" href="/reports/monthly-income">Reset</a>
        </div>
      </form>
    </div>
    <div class="card full">
      <h3>Monthly Matrix</h3>
      {monthly_table}
    </div>
    <div class="card full">
      <h3>Rolling 3-Month Average</h3>
      {t3m_table}
    </div>
    """
    return _base_page("dgi-dash monthly income", body)


def render_securities(db_url=DEFAULT_DB_URL):
    data = dashboard_data.get_dashboard_data(db_url=db_url)
    rows = "".join(
        f"<tr><td><a href='{_security_href(row['security'])}'>{_text(row['security'])}</a></td><td class='money'>{_money(row['total'])}</td><td>{row['txn_count']}</td><td>{_text(row['last_date'])}</td></tr>"
        for row in data["top_securities"]
    )
    body = f"""
    <div class="hero">
      <h2>Securities</h2>
      <p>Top securities by total dividend amount. Use the transaction page for full filtering.</p>
    </div>
    <div class="card full">
      <table>
        <thead><tr><th>Security</th><th class="money">Dividend Total</th><th>Txns</th><th>Last Date</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """
    return _base_page("dgi-dash securities", body)


def render_security_detail(security_ticker, db_url=DEFAULT_DB_URL):
    data = dashboard_data.get_security_detail(security_ticker, db_url=db_url)
    if not data:
        return _base_page("Security Not Found", "<div class='hero'><h2>Security Not Found</h2></div>")

    by_account_rows = "".join(
        f"<tr><td><a href='{_account_href(row['account'])}'>{_text(row['account'])}</a></td><td>{row['txn_count']}</td><td class='money'>{_money(row['total_amount'])}</td><td>{_text(row['last_date'])}</td></tr>"
        for row in data["by_account"]
    )
    by_year_rows = "".join(
        f"<tr><td>{_text(row['year'])}</td><td>{row['txn_count']}</td><td class='money'>{_money(row['total_amount'])}</td></tr>"
        for row in data["by_year"]
    )
    recent_rows = "".join(
        f"<tr><td>{_text(row['date'])}</td><td><a href='{_account_href(row['account'])}'>{_text(row['account'])}</a></td><td>{_text(row['txn_type'])}</td><td class='money'>{_money(row['amount'])}</td><td>{_text(row['source_system'])}</td></tr>"
        for row in data["recent_transactions"]
    )
    body = f"""
    <div class="hero">
      <h2>{_text(data['security'])}</h2>
      <p>From {_text(data['summary']['first_date'])} through {_text(data['summary']['last_date'])}, this security has {data['summary']['txn_count']} transactions totaling {_money(data['summary']['total_amount'])}.</p>
    </div>
    <div class="grid">
      <div class="card side">
        <h3>By Year</h3>
        <table>
          <thead><tr><th>Year</th><th>Txns</th><th class='money'>Total</th></tr></thead>
          <tbody>{by_year_rows}</tbody>
        </table>
      </div>
      <div class="card wide">
        <h3>By Account</h3>
        <table>
          <thead><tr><th>Account</th><th>Txns</th><th class='money'>Total</th><th>Last Date</th></tr></thead>
          <tbody>{by_account_rows}</tbody>
        </table>
      </div>
      <div class="card full">
        <h3>Recent Transactions</h3>
        <table>
          <thead><tr><th>Date</th><th>Account</th><th>Type</th><th class='money'>Amount</th><th>Source</th></tr></thead>
          <tbody>{recent_rows}</tbody>
        </table>
      </div>
    </div>
    """
    return _base_page(f"dgi-dash security {data['security']}", body)


def render_json(path, params, db_url=DEFAULT_DB_URL):
    if path == "/api/summary":
        payload = dashboard_data.get_dashboard_data(db_url=db_url)
    else:
        payload = dashboard_data.get_transactions(
            db_url=db_url,
            account=params.get("account", [""])[0] or None,
            security=params.get("security", [""])[0] or None,
            txn_type=params.get("txn_type", [""])[0] or None,
            year=params.get("year", [""])[0] or None,
            limit=int(params.get("limit", ["200"])[0] or 200),
        )
    return json.dumps(payload, default=str, indent=2).encode("utf-8")


def create_app(db_url=DEFAULT_DB_URL):
    def app(environ, start_response):
        path = environ.get("PATH_INFO", "/")
        params = parse_qs(environ.get("QUERY_STRING", ""))

        try:
            if path == "/":
                body = render_dashboard(params, db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path == "/accounts":
                body = render_accounts(db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path == "/reports/monthly-income":
                body = render_monthly_income_report(params, db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path.startswith("/accounts/"):
                account_name = unquote(path[len("/accounts/"):])
                body = render_account_detail(account_name, db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path == "/securities":
                body = render_securities(db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path.startswith("/securities/"):
                security_ticker = unquote(path[len("/securities/"):])
                body = render_security_detail(security_ticker, db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path == "/transactions":
                body = render_transactions(params, db_url=db_url).encode("utf-8")
                start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
                return [body]
            if path in {"/api/summary", "/api/transactions"}:
                body = render_json(path, params, db_url=db_url)
                start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
                return [body]

            body = _base_page("Not Found", "<div class='hero'><h2>Not Found</h2></div>").encode("utf-8")
            start_response("404 Not Found", [("Content-Type", "text/html; charset=utf-8")])
            return [body]
        except Exception as exc:
            body = _base_page("Application Error", f"<div class='hero'><h2>Application Error</h2><p>{_text(exc)}</p></div>").encode("utf-8")
            start_response("500 Internal Server Error", [("Content-Type", "text/html; charset=utf-8")])
            return [body]

    return app


def serve(*, host="127.0.0.1", port=8000, db_url=DEFAULT_DB_URL):
    app = create_app(db_url=db_url)
    with make_server(host, int(port), app) as httpd:
        print(f"web_url=http://{host}:{port}")
        print(f"database_url={db_url}")
        httpd.serve_forever()
