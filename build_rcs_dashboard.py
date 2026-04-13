"""
RCS Dashboard Builder
=====================
Usage:
    python3 build_rcs_dashboard.py --csv "path/to/new_file.csv"

Reads the CSV, recomputes all metrics, and updates the data variables
in rcs_dashboard.html (in the same folder as this script) in-place.

If the CSV path is omitted, it defaults to the original Q1 file.

Snowflake credentials (set as environment variables):
    SF_ACCOUNT   — e.g. klaviyo.us-east-1
    SF_USER      — your Klaviyo email
    SF_PASSWORD  — your password  (or leave blank to use SF_AUTHENTICATOR)
    SF_AUTHENTICATOR — set to 'externalbrowser' for SSO (default if no password)
    SF_WAREHOUSE — e.g. ANALYST_WH
    SF_DATABASE  — KLAVIYO  (default)
    SF_ROLE      — optional
"""

import csv, json, re, sys, os
from collections import defaultdict
from datetime import datetime, timedelta

# ── CONFIG ───────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
HTML_PATH    = os.path.join(SCRIPT_DIR, 'rcs_dashboard.html')
DEFAULT_CSV  = (
    '/Users/wafaa.muhammad/Downloads/'
    'klaviyo-rcs_resource_order_audit-Q1_April_2026_with_duration'
    ' - klaviyo-rcs_resource_order_audit-Q1_April_2026_with_duration.csv'
)
LAUNCH    = datetime(2026, 2, 24)   # Week 0 — update if launch date changes
HOURS_COL = 'duration hours'        # Column name for duration in hours

# ── SNOWFLAKE MRR LOOKUP ─────────────────────────────────────────────────────
def fetch_mrr(entity_ids):
    """
    Query Snowflake for company name, email MRR, SMS MRR for a list of entity IDs.
    Returns dict: { entity_id -> { company_name, email_mrr, sms_mrr, combined_mrr } }
    Skips gracefully if snowflake-connector-python is not installed or creds are missing.
    Install: pip install snowflake-connector-python
    """
    try:
        import snowflake.connector
    except ImportError:
        print('  SKIP Snowflake: snowflake-connector-python not installed.')
        print('  Run: pip install snowflake-connector-python')
        return {}

    account   = os.environ.get('SF_ACCOUNT')
    user      = os.environ.get('SF_USER')
    password  = os.environ.get('SF_PASSWORD', '')
    auth      = os.environ.get('SF_AUTHENTICATOR', 'externalbrowser')
    warehouse = os.environ.get('SF_WAREHOUSE', '')
    database  = os.environ.get('SF_DATABASE', 'KLAVIYO')
    role      = os.environ.get('SF_ROLE', '')

    if not account or not user:
        print('  SKIP Snowflake: set SF_ACCOUNT and SF_USER environment variables.')
        return {}

    try:
        conn_params = dict(account=account, user=user, database=database)
        if password:
            conn_params['password'] = password
        else:
            conn_params['authenticator'] = auth
        if warehouse: conn_params['warehouse'] = warehouse
        if role:      conn_params['role'] = role

        print(f'  Connecting to Snowflake ({account})...')
        conn = snowflake.connector.connect(**conn_params)
        cur  = conn.cursor()

        # Build VALUES list in chunks to avoid query size limits
        CHUNK = 500
        results = {}
        ids = list(entity_ids)
        for i in range(0, len(ids), CHUNK):
            chunk = ids[i:i+CHUNK]
            values = ','.join(f"('{eid}')" for eid in chunk)
            sql = f"""
            WITH entity_ids AS (SELECT col1 AS entity_id FROM (VALUES {values}) t(col1)),
            latest_ds AS (SELECT MAX(DS_TREE) AS max_ds FROM KLAVIYO.STAGING.INT_METRICS_TREE_ACCOUNTS),
            email_mrr AS (
                SELECT KLAVIYO_ACCOUNT_ID, ENDING_MRR AS email_mrr
                FROM KLAVIYO.STAGING.INT_METRICS_TREE_ACCOUNTS, latest_ds
                WHERE LOWER(PRODUCT) = 'email' AND DS_TREE = max_ds
            ),
            sms_mrr AS (
                SELECT KLAVIYO_ACCOUNT_ID, ENDING_MRR AS sms_mrr
                FROM KLAVIYO.STAGING.INT_METRICS_TREE_ACCOUNTS, latest_ds
                WHERE LOWER(PRODUCT) = 'sms' AND DS_TREE = max_ds
            ),
            sfdc AS (
                SELECT PRODUCT_KLAVIYO_ACCOUNT_ID_C AS entity_id, NAME AS company_name
                FROM KLAVIYO.SALESFORCE.ACCOUNT
                WHERE PRODUCT_KLAVIYO_ACCOUNT_ID_C IS NOT NULL AND IS_DELETED = FALSE
            )
            SELECT e.entity_id,
                   s.company_name,
                   ROUND(em.email_mrr, 2)  AS email_mrr,
                   ROUND(sm.sms_mrr, 2)    AS sms_mrr,
                   ROUND(COALESCE(em.email_mrr,0) + COALESCE(sm.sms_mrr,0), 2) AS combined_mrr
            FROM entity_ids e
            LEFT JOIN sfdc      s  ON e.entity_id = s.entity_id
            LEFT JOIN email_mrr em ON e.entity_id = em.KLAVIYO_ACCOUNT_ID
            LEFT JOIN sms_mrr   sm ON e.entity_id = sm.KLAVIYO_ACCOUNT_ID
            """
            cur.execute(sql)
            for row in cur.fetchall():
                eid, name, em, sm, comb = row
                results[eid] = {
                    'company_name': name or '',
                    'email_mrr':    float(em)   if em   is not None else None,
                    'sms_mrr':      float(sm)   if sm   is not None else None,
                    'combined_mrr': float(comb) if comb is not None else None,
                }
        cur.close()
        conn.close()
        print(f'  Snowflake: fetched MRR for {len(results)} of {len(ids)} entities.')
        return results

    except Exception as e:
        print(f'  SKIP Snowflake: {e}')
        return {}

# ── HELPERS ──────────────────────────────────────────────────────────────────
def parse_dt(s):
    s = s.strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S',
                '%Y-%m-%d %H:%M',    '%m/%d/%Y %H:%M'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def dt_to_week(dt):
    return None if dt is None else (dt - LAUNCH).days // 7

def week_label(wk):
    dt = LAUNCH + timedelta(weeks=wk)
    if wk == 0:
        return 'Wk 0 (Feb 24)'
    return f'Wk {wk:+d} ({dt.strftime("%b %d")})'

def mean(lst):
    return round(sum(lst) / len(lst), 1) if lst else None

def median(lst):
    if not lst:
        return None
    s = sorted(lst)
    return round(s[len(s) // 2], 1)

def pct(lst, p):
    if not lst:
        return None
    s = sorted(lst)
    return round(s[int(len(s) * p)], 1)


STATUS_LABELS = {
    '1 - PENDING IB REVIEW': 'Pending IB Review',
    '2 - IN INFOBIP REVIEW': 'In IB Review',
    '3 - IN CARRIER REVIEW': 'In Carrier Review',
    '4 - APPROVED':          'Approved',
    '4 - REJECTED':          'Rejected',
}

# ── LOAD CSV ─────────────────────────────────────────────────────────────────
def load(csv_path):
    with open(csv_path, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row['_dt']   = parse_dt(row['CreatedAt'])
        row['_week'] = dt_to_week(row['_dt'])
    return rows

# ── COMPUTE ALL METRICS ──────────────────────────────────────────────────────
def compute(rows):
    # Derive week range dynamically from the data
    all_event_weeks = [r['_week'] for r in rows if r['_week'] is not None]
    min_wk = min(all_event_weeks)
    max_wk = max(all_event_weeks)
    ALL_WEEKS = list(range(min_wk, max_wk + 1))

    # --- Submission week per UUID (earliest event) ---
    uuid_min_dt = {}
    for r in rows:
        u, dt = r['ResourceOrderUuid'], r['_dt']
        if dt and (u not in uuid_min_dt or dt < uuid_min_dt[u]):
            uuid_min_dt[u] = dt
    uuid_week = {u: dt_to_week(dt) for u, dt in uuid_min_dt.items()}

    # --- Sender submission week (earliest event across all UUIDs) ---
    sender_min_dt = {}
    for r in rows:
        sn, dt = r['senderName'], r['_dt']
        if dt and (sn not in sender_min_dt or dt < sender_min_dt[sn]):
            sender_min_dt[sn] = dt
    sender_sub_week = {sn: dt_to_week(dt) for sn, dt in sender_min_dt.items()}

    # --- Current status per sender (latest non-ignore event) ---
    sender_current = {}
    for r in rows:
        sc = r.get('status clean', '').strip()
        if sc == 'ignore':
            continue
        sn, dt = r['senderName'], r['_dt']
        if dt and (sn not in sender_current or dt > sender_current[sn][0]):
            sender_current[sn] = (dt, sc)
    sender_status = {sn: v[1] for sn, v in sender_current.items()}

    # 1. Weekly total submissions (distinct UUIDs by submission week)
    total_sub = defaultdict(int)
    seen_uuids = set()
    for r in rows:
        u = r['ResourceOrderUuid']
        wk = uuid_week.get(u)
        if wk is not None and u not in seen_uuids:
            seen_uuids.add(u)
            total_sub[wk] += 1

    # 2. First-time senders per week
    first_time = defaultdict(int)
    for sn, dt in sender_min_dt.items():
        wk = dt_to_week(dt)
        if wk is not None:
            first_time[wk] += 1

    # 3. Current status counts by sender
    status_counts = defaultdict(int)
    for sn, sc in sender_status.items():
        status_counts[sc] += 1

    # 4. Rejection categories — read directly from rejection_category column.
    # Multiple categories may be listed per rejection (comma-separated), so
    # cat_totals counts category occurrences across all rows (total > # rejections).
    cat_totals = defaultdict(int)
    sender_rej_cats = defaultdict(set)   # per-sender set, used for WoW chart
    sender_rej_reasons = defaultdict(list)
    for r in rows:
        if r.get('status clean', '').strip() != '4 - REJECTED':
            continue
        sn = r['senderName']

        # Collect rejection_reason for modal display
        rr = r.get('rejection_reason', '').strip()
        if rr and rr not in sender_rej_reasons[sn]:
            sender_rej_reasons[sn].append(rr)

        # Parse rejection_category (pipe-separated, e.g. "Privacy Policy | Brand Contact | Web/Opt-in Issues")
        rc = r.get('rejection_category', '').strip()
        if rc and rc.lower() not in ('n/a', 'na'):
            cats = [c.strip() for c in rc.split('|') if c.strip()]
        else:
            cats = ['Other']

        # Count each category per row (total > number of rejections)
        for c in cats:
            cat_totals[c] += 1

        # Also track per sender for WoW chart
        for c in cats:
            sender_rej_cats[sn].add(c)

    sorted_cats = sorted(cat_totals, key=lambda c: -cat_totals[c])

    # 5. Rejection reasons WoW (by week the rejection event occurred)
    wow = defaultdict(lambda: defaultdict(int))
    for r in rows:
        if r.get('status clean', '').strip() != '4 - REJECTED':
            continue
        wk = r['_week']
        if wk is None:
            continue
        rc = r.get('rejection_category', '').strip()
        if rc and rc.lower() not in ('n/a', 'na'):
            cats = [c.strip() for c in rc.split('|') if c.strip()]
        else:
            cats = ['Other']
        for c in cats:
            wow[wk][c] += 1
    wow_weeks = sorted(wow.keys())

    # 6. IB throughput
    ib_received   = defaultdict(int)
    ib_to_carrier = defaultdict(int)
    ib_rejected   = defaultdict(int)
    ib_approved   = defaultdict(int)
    for r in rows:
        sc = r.get('status clean', '').strip()
        wk = r['_week']
        if wk is None:
            continue
        if sc == '1 - PENDING IB REVIEW':
            ib_received[wk] += 1
        elif sc == '3 - IN CARRIER REVIEW':
            ib_to_carrier[wk] += 1
        elif sc == '4 - REJECTED':
            ib_rejected[wk] += 1
        elif sc == '4 - APPROVED':
            ib_approved[wk] += 1

    # Completed same-week vs prior-week
    completed_same  = defaultdict(int)
    completed_prior = defaultdict(int)
    for r in rows:
        sc = r.get('status clean', '').strip()
        if sc not in ('3 - IN CARRIER REVIEW', '4 - REJECTED'):
            continue
        cw = r['_week']
        if cw is None:
            continue
        sw = uuid_week.get(r['ResourceOrderUuid'])
        if sw is None:
            continue
        if sw == cw:
            completed_same[cw] += 1
        else:
            completed_prior[cw] += 1

    # 7. IB timing by week
    wait_h = defaultdict(list)
    rej_h  = defaultdict(list)
    app_h  = defaultdict(list)
    for r in rows:
        sc = r.get('status clean', '').strip()
        wk = r['_week']
        if wk is None:
            continue
        try:
            h = float(r[HOURS_COL])
        except (ValueError, KeyError):
            continue
        if sc == '2 - IN INFOBIP REVIEW':
            wait_h[wk].append(h)
        elif sc == '4 - REJECTED':
            rej_h[wk].append(h)
        elif sc == '3 - IN CARRIER REVIEW':
            app_h[wk].append(h)

    timing_weeks = sorted(set(wait_h) | set(rej_h) | set(app_h))

    # 8. Entity ID per sender (for MRR lookup)
    sender_entity = {}
    for r in rows:
        sn  = r['senderName']
        eid = r.get('entityId', '').strip()
        if eid and sn not in sender_entity:
            sender_entity[sn] = eid

    # 9. Records for modal (MRR added later in main after Snowflake fetch)
    records = []
    for sn, reasons in sender_rej_reasons.items():
        all_cats = sender_rej_cats.get(sn, set())
        wk  = sender_sub_week.get(sn)
        st  = STATUS_LABELS.get(sender_status.get(sn, ''), sender_status.get(sn, ''))
        records.append({
            's':   sn,
            'eid': sender_entity.get(sn, ''),
            'w':   week_label(wk) if wk is not None else 'Unknown',
            'st':  st,
            'c':   sorted(all_cats),
            'r':   ' | '.join(r for r in reasons[:3] if r),
            # MRR fields — populated after Snowflake fetch
            'co':  '',    # company name
            'em':  None,  # email MRR
            'sm':  None,  # SMS MRR
            'mrr': None,  # combined MRR
        })
    records.sort(key=lambda r: r['w'])

    # 10. Pending IB review senders:
    # All rows where status clean = '1 - PENDING IB REVIEW' AND sender's current
    # status is also '1 - PENDING IB REVIEW'. Each row is a data point distributed
    # by duration in state (rounded to 0 decimal places in days).
    ib_senders = {sn for sn, sc in sender_status.items() if sc == '1 - PENDING IB REVIEW'}
    ib_records = []
    for r in rows:
        if r.get('status clean', '').strip() != '1 - PENDING IB REVIEW':
            continue
        sn = r['senderName']
        if sn not in ib_senders:
            continue
        try:
            d = float(r['duration in state (days)'])
        except (ValueError, KeyError):
            continue
        ib_records.append({
            's':   sn,
            'eid': sender_entity.get(sn, ''),
            'dur': round(d),
            'co':  '',
            'em':  None,
            'sm':  None,
            'mrr': None,
        })

    # 11. Carrier review senders:
    # All rows where status clean = '3 - IN CARRIER REVIEW' AND sender's current
    # status is also '3 - IN CARRIER REVIEW'. Each row is a data point distributed
    # by duration in state (rounded to 0 decimal places in days).
    carrier_senders = {sn for sn, sc in sender_status.items() if sc == '3 - IN CARRIER REVIEW'}
    carrier_records = []
    for r in rows:
        if r.get('status clean', '').strip() != '3 - IN CARRIER REVIEW':
            continue
        sn = r['senderName']
        if sn not in carrier_senders:
            continue
        try:
            d = float(r['duration in state (days)'])
        except (ValueError, KeyError):
            continue
        carrier_records.append({
            's':   sn,
            'eid': sender_entity.get(sn, ''),
            'dur': round(d),
            'co':  '',
            'em':  None,
            'sm':  None,
            'mrr': None,
        })

    return dict(
        ALL_WEEKS       = ALL_WEEKS,
        ib_records      = ib_records,
        total_sub       = total_sub,
        first_time      = first_time,
        status_counts   = status_counts,
        sorted_cats     = sorted_cats,
        cat_totals      = cat_totals,
        wow             = wow,
        wow_weeks       = wow_weeks,
        ib_received     = ib_received,
        ib_to_carrier   = ib_to_carrier,
        ib_rejected     = ib_rejected,
        ib_approved     = ib_approved,
        completed_same  = completed_same,
        completed_prior = completed_prior,
        wait_h          = wait_h,
        rej_h           = rej_h,
        app_h           = app_h,
        timing_weeks    = timing_weeks,
        records         = records,
        sender_entity   = sender_entity,
        carrier_records = carrier_records,
    )

# ── BUILD JS VARIABLE BLOCKS ─────────────────────────────────────────────────
def build_js_vars(m):
    AW = m['ALL_WEEKS']

    wlabels   = [week_label(w) for w in AW]
    total_sub = [m['total_sub'].get(w, 0) for w in AW]
    first_time= [m['first_time'].get(w, 0) for w in AW]

    sc = m['status_counts']
    status_labels = ['Pending IB Review','In IB Review','In Carrier Review','Approved','Rejected']
    status_values = [
        sc.get('1 - PENDING IB REVIEW', 0),
        sc.get('2 - IN INFOBIP REVIEW', 0),
        sc.get('3 - IN CARRIER REVIEW', 0),
        sc.get('4 - APPROVED', 0),
        sc.get('4 - REJECTED', 0),
    ]

    rej_cat_labels = m['sorted_cats']
    rej_cat_values = [m['cat_totals'][c] for c in rej_cat_labels]

    approved_n = sc.get('4 - APPROVED', 0)
    rejected_n = sc.get('4 - REJECTED', 0)

    D = dict(
        wlabels       = wlabels,
        total_sub     = total_sub,
        first_time    = first_time,
        status_labels = status_labels,
        status_values = status_values,
        rej_cat_labels= rej_cat_labels,
        rej_cat_values= rej_cat_values,
        approved_n    = approved_n,
        rejected_n    = rejected_n,
    )

    wow_weeks_labels = [week_label(w) for w in m['wow_weeks']]
    wow_cats = m['sorted_cats']
    wow_categories = {}
    for c in wow_cats:
        wow_categories[c] = [m['wow'][w].get(c, 0) for w in m['wow_weeks']]

    tw = m['timing_weeks']
    ibTiming = dict(
        weeks     = [week_label(w) for w in tw],
        wait_mean = [mean(m['wait_h'][w]) for w in tw],
        rej_mean  = [mean(m['rej_h'][w])  for w in tw],
        app_mean  = [mean(m['app_h'][w])  for w in tw],
    )

    ibData = dict(
        weeks           = [week_label(w) for w in AW],
        ib_received     = [m['ib_received'].get(w, 0)    for w in AW],
        completed_same  = [m['completed_same'].get(w, 0) for w in AW],
        completed_prior = [m['completed_prior'].get(w, 0)for w in AW],
    )

    output = dict(
        labels   = [week_label(w) for w in AW],
        rejected = [m['ib_rejected'].get(w, 0)   for w in AW],
        carrier  = [m['ib_to_carrier'].get(w, 0) for w in AW],
        approved = [m['ib_approved'].get(w, 0)   for w in AW],
    )

    return D, dict(weeks=wow_weeks_labels, categories=wow_categories), ibTiming, ibData, output

# ── CARRIER DATA BUILDER ─────────────────────────────────────────────────────
def build_carrier_js(carrier_records):
    """Build CARRIER_DATA JS object from carrier_records list."""
    buckets = defaultdict(list)
    for rec in carrier_records:
        day_bucket = str(round(rec['dur']))
        buckets[day_bucket].append(rec)

    sorted_keys = sorted(buckets.keys(), key=lambda x: int(x))
    labels  = sorted_keys
    counts  = [len(buckets[b]) for b in sorted_keys]
    records = {}
    for b in sorted_keys:
        # Sort: MRR desc (nulls last), then dur desc as tiebreak
        recs = sorted(buckets[b],
                      key=lambda r: (r['mrr'] is None, -(r['mrr'] or 0), -r['dur']))
        records[b] = recs

    return {'labels': labels, 'counts': counts, 'records': records}

# ── PATCH HTML ───────────────────────────────────────────────────────────────
def patch(html, var_name, new_value_js):
    """
    Replace  `const VAR_NAME = <anything>;`  with the new value.
    Uses a lambda replacement to avoid regex interpreting backslashes in JSON.
    """
    pattern = re.compile(
        rf'(const {re.escape(var_name)}\s*=\s*)(\[[\s\S]*?\]|\{{[\s\S]*?\}})(;)',
        re.MULTILINE
    )
    new_js = new_value_js  # capture for closure
    new_html, n = pattern.subn(lambda m: m.group(1) + new_js + m.group(3), html, count=1)
    if n == 0:
        print(f'  WARNING: could not find "const {var_name}" — skipped')
    return new_html

def patch_array(html, var_name, values):
    """Replace a simple `const VAR = [...];` line."""
    js = json.dumps(values)
    pattern = re.compile(rf'(const {re.escape(var_name)}\s*=\s*)(\[[\s\S]*?\])(;)', re.MULTILINE)
    new_html, n = pattern.subn(lambda m: m.group(1) + js + m.group(3), html, count=1)
    if n == 0:
        print(f'  WARNING: could not find "const {var_name}" — skipped')
    return new_html

def patch_rej_records(html, records):
    """REJ_RECORDS — use lambda to prevent regex interpreting backslashes in JSON."""
    records_js = json.dumps(records, ensure_ascii=False)
    pattern = re.compile(r'(const REJ_RECORDS\s*=\s*)(\[[\s\S]*?\])(;)', re.MULTILINE)
    new_html, n = pattern.subn(lambda m: m.group(1) + records_js + m.group(3), html, count=1)
    if n == 0:
        print('  WARNING: could not find REJ_RECORDS — skipped')
    return new_html

def patch_carrier_data(html, carrier_data):
    """CARRIER_DATA — patch the nested object using lambda replacement."""
    js = json.dumps(carrier_data, ensure_ascii=False)
    pattern = re.compile(r'(const CARRIER_DATA\s*=\s*)(\{[\s\S]*?\})(;)', re.MULTILINE)
    new_html, n = pattern.subn(lambda m: m.group(1) + js + m.group(3), html, count=1)
    if n == 0:
        print('  WARNING: could not find CARRIER_DATA — skipped')
    return new_html

def patch_carrier_subtitle(html, total):
    """Update the hardcoded sender count in the carrier chart subtitle."""
    pattern = re.compile(r'(Click a bar to see the list\. )(\d+)( total senders\.)')
    new_html, n = pattern.subn(lambda m: m.group(1) + str(total) + m.group(3), html, count=1)
    if n == 0:
        print('  WARNING: could not find carrier subtitle count — skipped')
    return new_html

def patch_ib_data(html, ib_data):
    """IB_DATA — patch the nested object using lambda replacement."""
    js = json.dumps(ib_data, ensure_ascii=False)
    pattern = re.compile(r'(const IB_DATA\s*=\s*)(\{[\s\S]*?\})(;)', re.MULTILINE)
    new_html, n = pattern.subn(lambda m: m.group(1) + js + m.group(3), html, count=1)
    if n == 0:
        print('  WARNING: could not find IB_DATA — skipped')
    return new_html

def patch_ib_subtitle(html, total):
    """Update the IB chart subtitle with the total sender count."""
    pattern = re.compile(r'(id="ibSubtitle">Each bar = number of senders who have been in IB review for that many days\. Click a bar to see the list\.)(.*?)(</div>)')
    new_text = f' {total} total senders.'
    new_html, n = pattern.subn(lambda m: m.group(1) + new_text + m.group(3), html, count=1)
    if n == 0:
        print('  WARNING: could not find IB subtitle — skipped')
    return new_html

# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default=DEFAULT_CSV, help='Path to updated CSV file')
    args = parser.parse_args()

    print(f'Reading CSV: {args.csv}')
    rows = load(args.csv)
    print(f'  {len(rows)} rows loaded')

    print('Computing metrics...')
    m = compute(rows)
    D, rejWow, ibTiming, ibData, output = build_js_vars(m)

    # Fetch MRR from Snowflake
    print('Fetching MRR from Snowflake...')
    entity_ids = set(m['sender_entity'].values())
    mrr_data   = fetch_mrr(entity_ids)

    # Merge MRR into records (rejection modal)
    if mrr_data:
        for rec in m['records']:
            eid = rec.get('eid', '')
            if eid in mrr_data:
                d = mrr_data[eid]
                rec['co']  = d['company_name']
                rec['em']  = d['email_mrr']
                rec['sm']  = d['sms_mrr']
                rec['mrr'] = d['combined_mrr']
        print(f'  MRR merged into {sum(1 for r in m["records"] if r["mrr"] is not None)} rejection records.')

    # Merge MRR into IB review records
    if mrr_data:
        for rec in m['ib_records']:
            eid = rec.get('eid', '')
            if eid in mrr_data:
                d = mrr_data[eid]
                rec['co']  = d['company_name']
                rec['em']  = d['email_mrr']
                rec['sm']  = d['sms_mrr']
                rec['mrr'] = d['combined_mrr']
        print(f'  MRR merged into {sum(1 for r in m["ib_records"] if r["mrr"] is not None)} IB review records.')

    # Merge MRR into carrier records
    if mrr_data:
        for rec in m['carrier_records']:
            eid = rec.get('eid', '')
            if eid in mrr_data:
                d = mrr_data[eid]
                rec['co']  = d['company_name']
                rec['em']  = d['email_mrr']
                rec['sm']  = d['sms_mrr']
                rec['mrr'] = d['combined_mrr']
        print(f'  MRR merged into {sum(1 for r in m["carrier_records"] if r["mrr"] is not None)} carrier records.')

    print(f'Reading HTML: {HTML_PATH}')
    with open(HTML_PATH, encoding='utf-8') as f:
        html = f.read()

    print('Patching JS variables...')
    html = patch(html, 'D',       json.dumps(D,       ensure_ascii=False))
    html = patch(html, 'rejWow',  json.dumps(rejWow,  ensure_ascii=False))
    html = patch(html, 'ibTiming',json.dumps(ibTiming,ensure_ascii=False))
    html = patch(html, 'ibData',  json.dumps(ibData,  ensure_ascii=False))
    html = patch_array(html, 'outputLabels',  output['labels'])
    html = patch_array(html, 'outputRej',     output['rejected'])
    html = patch_array(html, 'outputCarrier', output['carrier'])
    html = patch_array(html, 'outputApproved',output['approved'])
    html = patch_rej_records(html, m['records'])

    print('Building IB review data...')
    ib_data = build_carrier_js(m['ib_records'])
    html = patch_ib_data(html, ib_data)
    html = patch_ib_subtitle(html, len(m['ib_records']))

    print('Building carrier data...')
    carrier_data = build_carrier_js(m['carrier_records'])
    html = patch_carrier_data(html, carrier_data)
    html = patch_carrier_subtitle(html, len(m['carrier_records']))

    print(f'Writing HTML: {HTML_PATH}')
    with open(HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'\nDone.')
    print(f'  {len(m["records"])} rejected sender records embedded.')
    print(f'  {len(m["ib_records"])} IB review sender records embedded.')
    print(f'  {len(m["carrier_records"])} carrier review sender records embedded.')
    if mrr_data:
        print(f'  MRR data included for {len(mrr_data)} entities.')
    else:
        print('  MRR not included (Snowflake unavailable — set SF_ACCOUNT and SF_USER to enable).')
    print('Open rcs_dashboard.html in your browser to view.')
