import streamlit as st
import pandas as pd
import sqlite3
import trino
from trino.auth import BasicAuthentication
from multiprocessing import Process, Lock
import concurrent.futures
import time
import os
from datetime import datetime, timedelta
import numpy as np
import warnings
from streamlit_autorefresh import st_autorefresh

# --- 1. GLOBAL CONFIG & MAPPINGS ---
warnings.filterwarnings("ignore")
st.set_page_config(page_title="Real Time Monitoring", page_icon="🛰️", layout="wide")

# Force Absolute Path for Mac Process Safety
DB_PATH = os.path.join(os.getcwd(), 'payment_data.db')
lock = Lock() 

IMPACT_MAPPINGS = {
    'Standard UPI': "method='upi' AND recurring=0 AND international=0",
    'UPI Intent': "method='upi' AND flow='intent' AND recurring=0 AND international=0",
    'UPI Collect': "method='upi' AND flow='collect' AND recurring=0 AND international=0",
    'UPI QR': "method='upi' AND (mode='upi_qr' OR receiver_type='qr_code') AND recurring=0 AND international=0",
    'UPI Turbo (in_app)': "method='upi' AND flow='in_app' AND recurring=0 AND international=0",
    'CC on UPI Intent': "method='upi' AND reference2='credit_card' AND flow='intent' AND recurring=0 AND international=0",
    'CC on UPI Collect': "method='upi' AND reference2='credit_card' AND flow='collect' AND recurring=0 AND international=0",
    'Cards': "method IN ('card', 'emi')",
    'Credit Card - Domestic': "method='card' AND type='credit' AND international=0 AND recurring=0",
    'Debit Card - Domestic': "method='card' AND type='debit' AND international=0 AND recurring=0",
    'Credit Card - International': "method='card' AND type='credit' AND international=1 AND recurring=0",
    'Debit Card - International': "method='card' AND type='debit' AND international=1 AND recurring=0",
    'CC EMI': "method='emi' AND type='credit'",
    'DC EMI': "method='emi' AND type='debit'",
    'Credit Card - Recurring Initial': "method='card' AND type='credit' AND recurring=1 AND recurring_type IN ('initial','card_change')",
    'Credit Card - Recurring Auto': "method='card' AND type='credit' AND recurring=1 AND recurring_type='auto'",
    'Debit Card - Recurring Initial': "method='card' AND type='debit' AND recurring=1 AND recurring_type IN ('initial','card_change')",
    'Debit Card - Recurring Auto': "method='card' AND type='debit' AND recurring=1 AND recurring_type='auto'",
    'Netbanking': "method='netbanking'",
    'Cred Pay / App': "method='app'",
    'Wallet': "method='wallet'",
    'UPI Recurring': "method='upi' AND recurring=1",
    'UPI Autopay - Intent Initial': "method='upi' AND recurring=1 AND recurring_type='initial' AND flow='intent'",
    'UPI Autopay - Collect Initial': "method='upi' AND recurring=1 AND recurring_type='initial' AND flow='collect'",
    'UPI Autopay - Auto': "method='upi' AND recurring=1 AND recurring_type='auto'",
    'Emandate': "method='emandate'",
    'Emandate - Initial': "method='emandate' AND recurring_type='initial'",
    'Emandate - Auto': "method='emandate' AND recurring_type='auto'",
    'ALL UPI (Combined)': "method='upi'",
    'ALL Cards (Combined)': "method IN ('card', 'emi')"
}

def run_trino_query(query, user, pwd):
    try:
        conn = trino.dbapi.connect(
            host='trino-gateway-router-looker.de.razorpay.com', port=443, 
            user=user, http_scheme='https', auth=BasicAuthentication(user, pwd), 
            catalog='startree', schema='default'
        )
        return pd.read_sql(query, conn)
    except:
        return pd.DataFrame()

# --- 2. DATABASE LAYER ---
def get_db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF") # "Turbo" write speed
    return conn

def init_db():
    if os.path.exists(DB_PATH):
        try: os.remove(DB_PATH)
        except: pass
    conn = get_db_conn()
    conn.execute("CREATE TABLE system_logs (id INTEGER PRIMARY KEY, timestamp TEXT, message TEXT)")
    conn.execute("""
        CREATE TABLE downtime_logs (
            id INTEGER PRIMARY KEY, method TEXT, l1_type TEXT, l1_name TEXT, 
            start_time TEXT, end_time TEXT, status TEXT, peak_failures INTEGER, anomaly_score REAL
        )
    """)
    conn.commit()
    conn.close()

def log_event(msg):
    try:
        conn = get_db_conn()
        conn.execute("INSERT INTO system_logs (timestamp, message) VALUES (?, ?)", 
                     (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
        conn.commit()
        conn.close()
    except:
        pass

# --- 3. THE VECTORIZED ANOMALY ENGINE ---

def discover_incidents_vectorized(df, method_name, l1_col, conn):
    """Zero-Loop Detection: Calculates 24h baseline in one pass."""
    if df.empty: return
    
    # Sort and calculate SR
    work_df = df.sort_values(['dt']).copy()
    work_df['sr'] = (work_df['success'] / work_df['attempts'] * 100).fillna(0)
    
    # VECTORIZED: Calculate 3h rolling mean for every entity simultaneously
    work_df['mean_sr'] = work_df.groupby(l1_col)['sr'].transform(
        lambda x: x.rolling(window=180, min_periods=5).mean()
    )
    
    work_df['abs_drop'] = work_df['mean_sr'] - work_df['sr']
    work_df['rel_drop'] = work_df['abs_drop'] / work_df['mean_sr'].replace(0, 1)

    # Filter for anomalies
    crit_mask = (work_df['abs_drop'] > 20) | (work_df['rel_drop'] > 0.35)
    degr_mask = (work_df['abs_drop'] > 8) | (work_df['rel_drop'] > 0.15)
    
    anomalies = work_df[(crit_mask | degr_mask) & (work_df['attempts'] > 20)].copy()
    if anomalies.empty: return

    # Log identified incidents
    for name in anomalies[l1_col].unique():
        entity_data = anomalies[anomalies[l1_col] == name]
        start_t = entity_data['dt'].min().strftime('%Y-%m-%d %H:%M:%S')
        peak_att = int(entity_data['attempts'].max())
        max_drop = round(float(entity_data['abs_drop'].max()), 1)
        status = "CRITICAL" if entity_data['abs_drop'].max() > 20 else "DEGRADED"

        exists = pd.read_sql(f"SELECT id FROM downtime_logs WHERE method='{method_name}' AND l1_name='{name}' AND start_time='{start_t}'", conn)
        if exists.empty:
            conn.execute("INSERT INTO downtime_logs (method, l1_type, l1_name, start_time, status, peak_failures, anomaly_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (method_name, l1_col, name, start_t, status, peak_att, max_drop))

def execute_single_sync(table_name, sql, user, pwd, is_backfill, lk):
    start_time = time.time()
    try:
        df = run_trino_query(sql, user, pwd)
        if not df.empty:
            df['dt'] = pd.to_datetime(df['dt'])
            with lk:
                conn = get_db_conn()
                # Bulk Save Data
                df.head(0).to_sql(table_name, conn, if_exists='append', index=False)
                min_dt = df['dt'].min().strftime('%Y-%m-%d %H:%M:%S')
                conn.execute(f"DELETE FROM {table_name} WHERE dt >= ?", (min_dt,))
                df.to_sql(table_name, conn, if_exists='append', index=False)
                
                # VECTORIZED SCAN (Lightning Fast)
                col_map = {
                    'upi_data': (['gateway', 'bank', 'cps_route', 'provider'], 'Standard UPI'),
                    'cards_data': (['issuer', 'network', 'gateway', 'cps_route'], 'Cards'),
                    'nb_data': (['bank', 'gateway'], 'Netbanking'),
                    'recurring_data': (['gateway'], 'UPI Recurring'),
                    'emandate_data': (['bank', 'gateway'], 'Emandate')
                }
                cols, m_name = col_map.get(table_name, ([], 'Unknown'))
                
                for c in cols:
                    discover_incidents_vectorized(df, m_name, c, conn)
                
                conn.commit()
                conn.close()
            log_event(f"✅ {table_name} Synced ({int(time.time()-start_time)}s)")
    except Exception as e:
        log_event(f"❌ {table_name} Error: {str(e)[:50]}")

def background_engine(user, pwd, lk):
    log_event("🚀 Supersonic Engine Online.")
    first_run = True
    while True:
        lookback = 86400 if first_run else 900
        t_filter = f"(created_at + 19800) >= CAST(TO_UNIXTIME(CURRENT_TIMESTAMP) AS BIGINT) + 19800 - {lookback}"
        queries = {
            'upi_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, flow, mode, receiver_type, reference2, gateway, provider, bank, cps_route, internal_error_code, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method = 'upi' AND recurring = 0 AND international = 0 GROUP BY 1,2,3,4,5,6,7,8,9,10,11",
            'cards_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, type, international, recurring, recurring_type, network, issuer, cps_route, gateway, internal_error_code, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method IN ('card', 'emi') GROUP BY 1,2,3,4,5,6,7,8,9,10,11",
            'nb_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, bank, gateway, provider, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method IN ('netbanking', 'wallet', 'app') GROUP BY 1,2,3,4,5",
            'emandate_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, recurring, recurring_type, bank, auth_type, gateway, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method IN ('emandate', 'nach') GROUP BY 1,2,3,4,5,6,7",
            'recurring_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, recurring, flow, recurring_type, gateway, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND recurring = 1 AND method = 'upi' GROUP BY 1,2,3,4,5,6"
        }
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            for t, s in queries.items(): ex.submit(execute_single_sync, t, s, user, pwd, first_run, lk)
        if first_run: 
            log_event("INITIAL_SYNC_COMPLETE")
            first_run = False
        time.sleep(300 - (time.time() % 300))

# --- 4. RENDER UTILS ---

def get_relative_text_color(val, r_min, r_max, is_sr=True):
    try:
        val, r_min, r_max = float(val), float(r_min), float(r_max)
        spread = r_max - r_min
        if spread < 8.0:
            if is_sr:
                if val > 80: return 'color: #2e7d32; font-weight: bold;'
                if val > 60: return 'color: #ed6c02; font-weight: bold;'
                return 'color: #e61919; font-weight: bold;'
            return 'color: #2e7d32;'
        pos = (val - r_min) / spread
        if not is_sr: pos = 1 - pos 
        if pos > 0.8: return 'color: #2e7d32; font-weight: bold;'
        elif pos < 0.25: return 'color: #e61919; font-weight: bold;'
        else: return 'color: #ed6c02; font-weight: bold;'
    except: return ''

def color_grade_styler(row):
    styles = ['' for _ in row.index]
    sr_indices = [i for i, col in enumerate(row.index) if col[1] == 'SR']
    if len(sr_indices) >= 4:
        settled = sr_indices[:-3]
        vals = row.iloc[settled].astype(float)
        r_min, r_max, mean_v = vals.min(), vals.max(), vals.mean()
        for idx in settled: styles[idx] = get_relative_text_color(row.iloc[idx], r_min, r_max, True)
        trend_style = styles[sr_indices[-4]]
        for idx in sr_indices[-3:]:
            v = float(row.iloc[idx])
            if v < (mean_v * 0.5): styles[idx] = 'color: #e61919; font-weight: bold; border-bottom: 2px solid red;'
            elif v > (mean_v * 1.2): styles[idx] = 'color: #2e7d32; font-weight: bold; opacity: 0.8;'
            else: styles[idx] = trend_style + ' opacity: 0.7; font-style: italic;'
    elif len(sr_indices) > 0:
        for idx in sr_indices: styles[idx] = 'color: #2e7d32; font-weight: bold;'
    share_indices = [i for i, col in enumerate(row.index) if col[1] == '% Share']
    if share_indices:
        vals = row.iloc[share_indices].astype(float)
        r_min, r_max = vals.min(), vals.max()
        for idx in share_indices: styles[idx] = get_relative_text_color(row.iloc[idx], r_min, r_max, False)
    return styles

def render_pane(df, dimension, title, gran):
    if df.empty or dimension not in df.columns: return
    st.subheader(f"📊 {title}")
    d = df.copy()
    d[dimension] = d[dimension].fillna('Unknown')
    d['dt_bucket'] = d['dt'].dt.floor(gran)
    pivot = d.pivot_table(index=dimension, columns='dt_bucket', values=['attempts', 'success'], aggfunc='sum').fillna(0)
    pivot.columns = pivot.columns.swaplevel(0, 1)
    frames = []
    unique_ts = sorted(d['dt_bucket'].dropna().unique())
    for ts in unique_ts:
        if ts in pivot:
            a, s = pivot[ts]['attempts'].astype(int), pivot[ts]['success'].astype(int)
            m_v, m_n = ((a/a.sum()*100).fillna(0), '% Share') if dimension == 'internal_error_code' else ((s/a.replace(0,np.nan)*100).fillna(0), 'SR')
            frames.append(pd.DataFrame({(ts.strftime('%H:%M'), 'Attempts'): a, (ts.strftime('%H:%M'), m_n): m_v}))
    if frames:
        fdf = pd.concat(frames, axis=1)
        last_t = unique_ts[-1].strftime('%H:%M')
        if (last_t, 'Attempts') in fdf.columns: fdf = fdf.sort_values(by=(last_t, 'Attempts'), ascending=False).head(50)
        st.dataframe(fdf.style.apply(color_grade_styler, axis=1).format({c: '{:.2f}%' if any(x in c[1] for x in ['SR', '% Share']) else '{:,.0f}' for c in fdf.columns}), use_container_width=True, height=400)
    st.divider()

@st.dialog("🎯 Merchant Impact Analysis")
def show_impact_popup(method, l1_name, l1_type, start_time, end_time):
    st.write(f"### Impact for {l1_name} ({method})")
    actual_end = end_time if end_time and str(end_time) not in ['-', 'NaT', 'None'] else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.caption(f"Period: {start_time} to {actual_end}")
    f_sql = IMPACT_MAPPINGS.get(method, "method IS NOT NULL")
    query = f"SELECT business_dba, merchant_id, COUNT(id) as Attempts, ROUND(COUNT(IF(authorized_at > 0, id, NULL)) * 100.0 / NULLIF(COUNT(id), 0), 2) as SR FROM startree.default.sr_view_v9 WHERE {f_sql} AND {l1_type} = '{l1_name}' AND (created_at + 19800) BETWEEN to_unixtime(timestamp '{start_time}') AND to_unixtime(timestamp '{actual_end}') GROUP BY 1, 2 ORDER BY Attempts DESC LIMIT 20"
    with st.spinner("Fetching..."):
        res = run_trino_query(query, st.session_state['u'], st.session_state['p'])
        if not res.empty: st.dataframe(res, use_container_width=True, hide_index=True)
        else: st.warning("No data found.")

def render_downtime_center():
    st.subheader("🕵️ Historical Incident & Impact Tracker")
    try:
        conn = get_db_conn()
        df = pd.read_sql("SELECT * FROM downtime_logs ORDER BY id DESC LIMIT 20", conn); conn.close()
        if not df.empty:
            cols = st.columns([1.2, 1.8, 0.8, 0.8, 1.2, 0.8, 1.2])
            h_labels = ["**Method**", "**Entity**", "**Start**", "**End**", "**Status**", "**Drop**", "**Action**"]
            for i, h in enumerate(h_labels): cols[i].write(h)
            st.divider()
            for _, r in df.iterrows():
                c = st.columns([1.2, 1.8, 0.8, 0.8, 1.2, 0.8, 1.2])
                c[0].write(f"**{r['method']}**"); c[1].write(f"`{r['l1_name']}`"); c[2].write(pd.to_datetime(r['start_time']).strftime('%H:%M'))
                c[3].write(pd.to_datetime(r['end_time']).strftime('%H:%M') if r['end_time'] and str(r['end_time']) != 'None' else ":red[LIVE]")
                emo = "🟢" if r['status'] == 'RESOLVED' else "🔴" if r['status'] == 'CRITICAL' else "🟡"
                c[4].write(f"{emo} {r['status']}"); c[5].write(f"-{r['anomaly_score']}%")
                if c[6].button("Impact", key=f"btn_{r['id']}"): show_impact_popup(r['method'], r['l1_name'], r['l1_type'], r['start_time'], r['end_time'])
    except: pass

# --- 5. MAIN EXECUTION ---
if __name__ == "__main__":
    if 'logged_in' not in st.session_state:
        with st.form("login"):
            st.title("🛡️ Login")
            u, p = st.text_input("LDAP"), st.text_input("Password", type="password")
            if st.form_submit_button("Launch"):
                if not run_trino_query("SELECT 1", u, p).empty:
                    init_db() # CREATE TABLES BEFORE ENGINE STARTS
                    st.session_state.update({'logged_in': True, 'u': u, 'p': p, 'sync_done': False})
                    Process(target=background_engine, args=(u, p, lock), daemon=True).start()
                    st.rerun()
                else: st.error("Auth Failed")
        st.stop()

    if not st.session_state.get('sync_done'):
        st.title("⏳ Syncing Backfill...")
        st_autorefresh(interval=2000, key="boot_refresh")
        try:
            conn = get_db_conn(); logs = pd.read_sql("SELECT message FROM system_logs ORDER BY id DESC", conn)['message'].tolist(); conn.close()
            st.code("\n".join(logs))
            if any("INITIAL_SYNC_COMPLETE" in m for m in logs): (st.session_state.update({'sync_done': True}), st.rerun())
        except: pass
        st.stop()

    st_autorefresh(interval=60000, key="data_refresh")
    with st.sidebar:
        st.title("⚙️ Command")
        lb, gran = st.selectbox("History", [1, 2, 6, 12], index=1), st.selectbox("Granularity", ["1min", "5min", "15min", "1h"], index=1)
        st.divider()
        with st.expander("📡 Engine Sync Logs", expanded=True):
            try:
                conn = get_db_conn(); s_logs = pd.read_sql("SELECT timestamp, message FROM system_logs ORDER BY id DESC LIMIT 30", conn); conn.close()
                for _, r in s_logs.iterrows():
                    clr = "green" if "✅" in r['message'] else "red" if "❌" in r['message'] else None
                    st.caption(f"**{r['timestamp']}** :{clr}[{r['message']}]" if clr else f"**{r['timestamp']}** {r['message']}")
            except: pass

    def get_data(table):
        try:
            conn = get_db_conn(); df = pd.read_sql(f"SELECT * FROM {table}", conn); conn.close()
            if df.empty: return df
            df['dt'] = pd.to_datetime(df['dt'])
            return df[df['dt'] >= df['dt'].max() - timedelta(hours=lb)]
        except: return pd.DataFrame()

    st.title("🛡️ Real Time Monitoring")
    with st.expander("🚨 INCIDENT HISTORY & IMPACT EXPLORER", expanded=True): render_downtime_center()
    st.divider()
    tabs = st.tabs(["📱 Standard UPI", "💳 Cards", "🏦 NB & Affordibility", "🔁 UPI Recurring", "🔄 Emandate & NACH"])
    
    with tabs[0]:
        d = get_data('upi_data')
        if not d.empty:
            c, v = [(d['mode']=='upi_qr')|(d['receiver_type']=='qr_code'), (d['reference2']=='credit_card')&(d['flow']=='collect'), (d['reference2']=='credit_card')&(d['flow']=='intent'), (d['flow']=='in_app'), (d['flow']=='collect'), (d['flow']=='intent')], ['UPI QR', 'CC on UPI Collect', 'CC on UPI Intent', 'UPI Turbo', 'UPI Collect', 'UPI Intent']
            d['method_drilled'] = np.select(c, v, default='UPI Unknown')
            for dim in ['method_drilled', 'cps_route']: render_pane(d, dim, f"UPI {dim} Trend", gran)
            render_pane(d[d['flow']=='intent'], 'gateway', "Intent Gateway", gran)
            render_pane(d[d['flow']=='collect'], 'gateway', "Collect Gateway", gran)
            for dim in ['provider', 'bank', 'internal_error_code']: render_pane(d, dim, f"UPI {dim}", gran)

    with tabs[1]:
        d = get_data('cards_data')
        if not d.empty:
            d['lt'] = d['type'].str.lower()
            c = [(d['method']=='card')&(d['lt']=='debit')&(d['recurring']==1)&(d['recurring_type'].isin(['initial','card_change'])), (d['method']=='card')&(d['lt']=='debit')&(d['recurring']==1)&(d['recurring_type']=='auto'), (d['method']=='card')&(d['lt']=='credit')&(d['recurring']==1)&(d['recurring_type'].isin(['initial','card_change'])), (d['method']=='card')&(d['lt']=='credit')&(d['recurring']==1)&(d['recurring_type']=='auto'), (d['method']=='card')&(d['lt']=='debit')&(d['international']==1), (d['method']=='card')&(d['lt']=='credit')&(d['international']==1), (d['method']=='card')&(d['lt']=='debit')&(d['international']==0), (d['method']=='card')&(d['lt']=='credit')&(d['international']==0), (d['method']=='emi')&(d['lt']=='debit'), (d['method']=='emi')&(d['lt']=='credit')]
            v = ['Debit-Rec-I', 'Debit-Rec-A', 'Credit-Rec-I', 'Credit-Recurring-A', 'Debit-International', 'Credit-International', 'Debit Card - Domestic', 'Credit Card - Domestic', 'DC EMI', 'CC EMI']
            d['method_drilled'] = np.select(c, v, default='Other Card')
            for dim in ['method_drilled', 'cps_route']: render_pane(d, dim, f"Cards {dim}", gran)
            for t in ['credit', 'debit']:
                df_t = d[d['lt']==t]
                for dim in ['network', 'gateway', 'issuer']: render_pane(df_t, dim, f"{t.upper()} {dim}", gran)
            render_pane(d, 'internal_error_code', "Cards Errors", gran)

    with tabs[2]:
        d = get_data('nb_data')
        if not d.empty:
            d['method_drilled'] = np.where(d['method'] == 'app', 'Cred Pay', d['method'])
            render_pane(d[d['method']=='netbanking'], 'bank', "NB Bank", gran)
            render_pane(d[d['method']=='netbanking'], 'gateway', "NB Gateway", gran)
            render_pane(d[d['method']!='netbanking'], 'method_drilled', "Apps & Affordability", gran)

    with tabs[3]:
        d = get_data('recurring_data')
        if not d.empty:
            c, v = [(d['recurring_type']=='initial')&(d['flow']=='collect'), (d['recurring_type']=='initial')&(d['flow']=='intent'), (d['recurring_type']=='auto')], ['Autopay-Coll-I', 'Autopay-Int-I', 'Autopay-Auto']
            d['method_drilled'] = np.select(c, v, default='Autopay Unknown')
            render_pane(d, 'method_drilled', "UPI Recurring", gran)
            render_pane(d[d['recurring_type']=='initial'], 'gateway', "Setup Gateway", gran)

    with tabs[4]:
        d = get_data('emandate_data')
        if not d.empty:
            d['method_drilled'] = d['method'].str.upper() + " - " + d['recurring_type'].str.title()
            render_pane(d, 'method_drilled', "Emandate/NACH", gran)
            render_pane(d, 'gateway', "Emandate Gateway", gran)
            for at in ['netbanking', 'debitcard']: render_pane(d[d['auth_type']==at], 'bank', f"Bank (Auth: {at})", gran)

    st.divider()
    st.subheader("🎯 Merchant Impact Explorer")
    with st.expander("Investigate Incident Window (A to B)", expanded=True):
        with st.form("impact_form"):
            c1, c2, c3 = st.columns([2, 1.5, 1.5])
            with c1:
                i_cat = st.selectbox("Flow Category", list(IMPACT_MAPPINGS.keys()), key="cs1")
                i_dim = st.selectbox("Dimension", ['gateway', 'bank', 'issuer', 'network', 'provider', 'cps_route'], key="ds1")
                i_val = st.text_input("Dimension Value", key="vs1")
            with c2:
                st.write("**Start**"); sd, sh, sm = st.date_input("Date", datetime.now(), key="isd1"), st.number_input("H", 0, 23, datetime.now().hour, key="ish1"), st.number_input("M", 0, 59, 0, key="ism1")
            with c3:
                st.write("**End**"); ed, eh, em = st.date_input("Date", datetime.now(), key="ied1"), st.number_input("H", 0, 23, datetime.now().hour, key="ieh1"), st.number_input("M", 0, 59, 59, key="iem1")
            if st.form_submit_button("Fetch Impact List"):
                ts_s, ts_e = f"{sd} {sh:02}:{sm:02}:00", f"{ed} {eh:02}:{em:02}:59"
                f_map = IMPACT_MAPPINGS.get(i_cat, "method IS NOT NULL")
                impact_sql = f"WITH summary AS (SELECT GROUPING(business_dba) AS is_total, business_dba, merchant_id, COUNT(id) AS Attempts, COUNT(IF (authorized_at <> 0, id, NULL)) * 100.0 / NULLIF(COUNT(id), 0) AS SR FROM startree.default.sr_view_v9 WHERE {f_map} AND status <> 'created' AND {i_dim} = '{i_val}' AND (created_at + 19800) BETWEEN (to_unixtime(timestamp '{ts_s}')) AND (to_unixtime(timestamp '{ts_e}')) GROUP BY GROUPING SETS ((business_dba, merchant_id), ())) SELECT CASE WHEN is_total = 1 THEN ' GRAND TOTAL ' ELSE business_dba END AS business_dba, COALESCE(merchant_id, '-') AS merchant_id, Attempts, SR FROM summary ORDER BY is_total DESC, Attempts DESC"
                with st.spinner("Executing..."):
                    res = run_trino_query(impact_sql, st.session_state['u'], st.session_state['p'])
                    if not res.empty:
                        res['SR'] = res['SR'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "0.00%")
                        st.dataframe(res, use_container_width=True, hide_index=True)
                    else: st.warning("No data found.")