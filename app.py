import streamlit as st
import pandas as pd
import sqlite3
import trino
from trino.auth import BasicAuthentication
from multiprocessing import Process
import concurrent.futures
import time
import os
import atexit
from datetime import datetime, timedelta
import numpy as np
import warnings
from streamlit_autorefresh import st_autorefresh

# --- 1. GLOBAL CONFIG & CLEANUP ---
warnings.filterwarnings("ignore")
st.set_page_config(page_title="Real Time Monitoring", page_icon="🛰️", layout="wide")
DB_PATH = 'payment_data.db'

def purge_db():
    """Ensures a clean slate on restart by purging the SQLite DB."""
    if os.path.exists(DB_PATH):
        try: 
            os.remove(DB_PATH)
        except Exception: 
            pass

atexit.register(purge_db)

def run_trino_query(query, user, pwd):
    """Executes queries against Trino cluster."""
    try:
        conn = trino.dbapi.connect(
            host='trino-gateway-router-looker.de.razorpay.com', 
            port=443, 
            user=user, 
            http_scheme='https', 
            auth=BasicAuthentication(user, pwd), 
            catalog='startree', 
            schema='default'
        )
        return pd.read_sql(query, conn)
    except Exception as e:
        return pd.DataFrame()

# --- 2. DATABASE LAYER ---
def get_db_conn():
    """Returns a thread-safe connection to local SQLite store."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    """Initializes tables, including the upgraded downtime_logs table."""
    purge_db()
    conn = get_db_conn()
    tables =[
        'upi_data', 'cards_data', 'nb_data', 'emandate_data', 
        'recurring_data', 'downtime_logs', 'system_logs'
    ]
    
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
        
    conn.execute("""
        CREATE TABLE system_logs (
            id INTEGER PRIMARY KEY, 
            timestamp TEXT, 
            message TEXT
        )
    """)
    
    conn.execute("""
        CREATE TABLE downtime_logs (
            id INTEGER PRIMARY KEY, 
            method TEXT, 
            l1_type TEXT, 
            l1_name TEXT, 
            start_time TEXT, 
            end_time TEXT, 
            status TEXT,
            peak_failures INTEGER,
            anomaly_score REAL
        )
    """)
    
    conn.commit()
    conn.close()

def log_event(msg):
    """Helper to log system events."""
    try:
        conn = get_db_conn()
        conn.execute(
            "INSERT INTO system_logs (timestamp, message) VALUES (?, ?)", 
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
        )
        conn.commit()
        conn.close()
    except Exception: 
        pass

# --- 3. BACKGROUND WORKER & ENHANCED DOWNTIME LOGIC ---
def check_downtimes(df, method_name, l1_col, conn, table_name):
    if df.empty or l1_col not in df.columns: return
    latest_dt = df['dt'].max()
    time_str = pd.to_datetime(latest_dt).strftime('%Y-%m-%d %H:%M:%S')
    
    # Current Performance
    recent = df[df['dt'] == latest_dt].groupby(l1_col).agg({'attempts': 'sum', 'success': 'sum'}).reset_index()
    
    # Fetch 2-hour history from SQLite for baseline
    try:
        history_query = f"SELECT {l1_col}, attempts, success FROM {table_name} WHERE dt >= datetime('now', '-2 hours')"
        hist_df = pd.read_sql(history_query, conn)
    except: hist_df = pd.DataFrame()

    for _, row in recent.iterrows():
        name = str(row[l1_col])
        if name.strip() == '' or name.lower() in ['nan', 'none', 'unknown']: continue
            
        cur_att = int(row['attempts'])
        cur_sr = (row['success'] / cur_att * 100) if cur_att > 0 else 0
        status, score = None, 0.0

        if cur_att > 20 and row['success'] == 0:
            status, score = "CRITICAL", 99.0 # Hard downtime
        elif not hist_df.empty and cur_att > 50:
            h = hist_df[hist_df[l1_col] == name].copy()
            if len(h) > 10:
                h['sr'] = (h['success'] / h['attempts'] * 100).fillna(0)
                mean_sr, std_sr = h['sr'].mean(), h['sr'].std()
                z = (cur_sr - mean_sr) / std_sr if std_sr > 0.5 else 0
                if z < -3.0: # 3 Sigma Drop
                    status, score = "DEGRADED", abs(round(z, 2))

        if status:
            exists = pd.read_sql(f"SELECT id FROM downtime_logs WHERE method='{method_name}' AND l1_name='{name}' AND status IN ('CRITICAL', 'DEGRADED')", conn)
            if exists.empty:
                conn.execute("INSERT INTO downtime_logs (method, l1_type, l1_name, start_time, status, peak_failures, anomaly_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
                             (method_name, l1_col, name, time_str, status, cur_att, score))
            else:
                conn.execute("UPDATE downtime_logs SET peak_failures = MAX(peak_failures, ?), anomaly_score = MAX(anomaly_score, ?) WHERE method = ? AND l1_name = ? AND status IN ('CRITICAL', 'DEGRADED')",
                             (cur_att, score, method_name, name))
        elif cur_sr > 0:
            conn.execute("UPDATE downtime_logs SET end_time = ?, status = 'RESOLVED' WHERE method = ? AND l1_name = ? AND status IN ('CRITICAL', 'DEGRADED')",
                         (time_str, method_name, name))

def execute_single_sync(table_name, sql, user, pwd):
    """Executes a sync task and applies domain-specific downtime checks."""
    try:
        df = run_trino_query(sql, user, pwd)
        if not df.empty:
            conn = get_db_conn()
            min_dt = str(df['dt'].min())
            try: conn.execute(f"DELETE FROM {table_name} WHERE dt >= ?", (min_dt,))
            except Exception: pass
                
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
        if table_name == 'upi_data': 
            check_downtimes(df, 'Standard UPI', 'gateway', conn, table_name)
            check_downtimes(df, 'Standard UPI', 'bank', conn, table_name)
            check_downtimes(df, 'Standard UPI', 'cps_route', conn, table_name)
        elif table_name == 'cards_data': 
            check_downtimes(df, 'Cards', 'issuer', conn, table_name)
            check_downtimes(df, 'Cards', 'network', conn, table_name)
            check_downtimes(df, 'Cards', 'gateway', conn, table_name)
        elif table_name == 'nb_data': 
            check_downtimes(df, 'Netbanking', 'bank', conn, table_name)
            check_downtimes(df, 'Netbanking', 'gateway', conn, table_name)
        elif table_name == 'emandate_data':
            check_downtimes(df, 'Emandate', 'bank', conn, table_name)
            
        conn.commit()
        conn.close()
        log_event(f"✅ {table_name} Sync Complete.")
    except Exception as e: 
        log_event(f"❌ {table_name} Failed: {str(e)[:50]}")

def background_engine(user, pwd):
    """Continuous polling loop."""
    log_event("🚀 Engine Online. Starting 24H Backfill...")
    first_run = True
    
    while True:
        lookback = 86400 if first_run else 900 
        t_filter = f"created_at >= CAST(TO_UNIXTIME(CURRENT_TIMESTAMP) AS BIGINT) - {lookback}"
        
        queries = {
            'upi_data': f"""
                SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, 
                method, flow, mode, receiver_type, reference2, gateway, provider, 
                bank, cps_route, internal_error_code, 
                COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success 
                FROM startree.default.sr_view_v9 
                WHERE {t_filter} AND status <> 'created' AND method = 'upi' AND recurring = 0 AND international = 0 
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            """,
            'cards_data': f"""
                SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, 
                method, type, international, recurring, recurring_type, network, issuer, 
                cps_route, gateway, internal_error_code, 
                COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success 
                FROM startree.default.sr_view_v9 
                WHERE {t_filter} AND status <> 'created' AND method IN ('card', 'emi') 
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            """,
            'nb_data': f"""
                SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, 
                method, bank, gateway, provider, 
                COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success 
                FROM startree.default.sr_view_v9 
                WHERE {t_filter} AND status <> 'created' AND method IN ('netbanking', 'wallet', 'app') 
                GROUP BY 1,2,3,4,5
            """,
            'emandate_data': f"""
                SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, 
                method, recurring, recurring_type, bank, auth_type, gateway, 
                COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success 
                FROM startree.default.sr_view_v9 
                WHERE {t_filter} AND status <> 'created' AND method IN ('emandate', 'nach') 
                GROUP BY 1,2,3,4,5,6,7
            """,
            'recurring_data': f"""
                SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, 
                method, recurring, flow, recurring_type, gateway, 
                COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success 
                FROM startree.default.sr_view_v9 
                WHERE {t_filter} AND status <> 'created' AND recurring = 1 AND method = 'upi' 
                GROUP BY 1,2,3,4,5,6
            """
        }
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            for t, s in queries.items(): 
                ex.submit(execute_single_sync, t, s, user, pwd)
                
        if first_run: 
            log_event("INITIAL_SYNC_COMPLETE")
            first_run = False
            
        time.sleep(300)

# --- 4. RENDER UTILS ---
def z_score_styler(df):
    styles = pd.DataFrame('', index=df.index, columns=df.columns)
    sr_cols = [c for c in df.columns if c[1] == 'SR']
    if not sr_cols: return styles
    num_sr = df[sr_cols].astype(float)
    row_means = num_sr.mean(axis=1).values[:, None]
    row_stds = num_sr.std(axis=1).values[:, None]
    z = (num_sr.values - row_means) / np.where(row_stds < 1.0, 1.0, row_stds)
    sr_styles = np.full(num_sr.shape, '', dtype=object)
    sr_styles[z <= -1.0] = 'background-color: #856404; color: white;' 
    sr_styles[z <= -2.0] = 'background-color: #721c24; color: white; font-weight: bold;' 
    styles[sr_cols] = sr_styles
    return styles

def render_pane(df, dimension, title, gran):
    if df.empty or dimension not in df.columns: return
    st.subheader(f"📊 {title}")
    work_df = df.copy()
    work_df[dimension] = work_df[dimension].fillna('Unknown')
    work_df['dt_bucket'] = work_df['dt'].dt.floor(gran)
    
    pivot = work_df.pivot_table(index=dimension, columns='dt_bucket', values=['attempts', 'success'], aggfunc='sum').fillna(0)
    pivot.columns = pivot.columns.swaplevel(0, 1)
    
    frames =[]
    for ts in sorted(work_df['dt_bucket'].dropna().unique()):
        if ts in pivot:
            a = pivot[ts]['attempts'].astype(int)
            s = pivot[ts]['success'].astype(int)
            
            # --- DYNAMIC METRIC SWITCHER ---
            if dimension == 'internal_error_code':
                total_a = a.sum()
                metric_val = (a / total_a * 100).fillna(0) if total_a > 0 else a * 0
                metric_name = '% Share'
            else:
                metric_val = ((s / a.replace(0, np.nan)) * 100).fillna(0)
                metric_name = 'SR'
                
            # Changed 'Vol' to 'Attempts'
            frames.append(pd.DataFrame({(ts.strftime('%H:%M'), 'Attempts'): a, (ts.strftime('%H:%M'), metric_name): metric_val}))
            
    if frames:
        fdf = pd.concat(frames, axis=1)
        last_t = pd.to_datetime(work_df['dt_bucket']).max().strftime('%H:%M')
        
        # Sort by the new 'Attempts' column instead of 'Vol'
        if (last_t, 'Attempts') in fdf.columns: 
            fdf = fdf.sort_values(by=(last_t, 'Attempts'), ascending=False).head(50)
            
        # Format both 'SR' and '% Share' as percentages
        st.dataframe(fdf.style.apply(z_score_styler, axis=None).format({c: '{:.2f}%' if c[1] in ['SR', '% Share'] else '{:,.0f}' for c in fdf.columns}), use_container_width=True)
    st.divider()

# --- 5. MAIN EXECUTION ---
if __name__ == "__main__":
    if 'logged_in' not in st.session_state:
        with st.form("login"):
            st.title("🛡️ Login")
            u = st.text_input("LDAP")
            p = st.text_input("Password", type="password")
            if st.form_submit_button("Launch"):
                if not run_trino_query("SELECT 1", u, p).empty:
                    st.session_state.update({'logged_in': True, 'u': u, 'p': p, 'sync_done': False})
                    init_db()
                    Process(target=background_engine, args=(u, p), daemon=True).start()
                    st.rerun()
                else: st.error("Auth Failed")
        st.stop()

    if not st.session_state.get('sync_done'):
        st.title("⏳ Syncing 24H Backfill...")
        st_autorefresh(interval=2000, key="boot_refresh")
        try:
            conn = get_db_conn()
            logs = pd.read_sql("SELECT message FROM system_logs ORDER BY id DESC", conn)['message'].tolist()
            conn.close()
            st.code("\n".join(logs))
            if any("INITIAL_SYNC_COMPLETE" in m for m in logs): 
                st.session_state['sync_done'] = True
                st.rerun()
        except Exception: pass
        st.stop()

    # --- DASHBOARD UI ---
    st_autorefresh(interval=60000, key="data_refresh") # 60 second ultra-fast refresh
    
    with st.sidebar:
        st.title("⚙️ Command")
        lb = st.selectbox("History",[1, 2, 6, 12], index=1)
        gran = st.selectbox("Granularity",["1min", "5min", "15min", "1h"], index=1)
        
        st.divider()
        with st.expander("📡 Engine Sync Logs", expanded=False):
            try:
                conn = get_db_conn()
                sync_logs = pd.read_sql("SELECT timestamp, message FROM system_logs ORDER BY id DESC LIMIT 25", conn)
                conn.close()
                for _, r in sync_logs.iterrows():
                    st.caption(f"[{r['timestamp']}] {r['message']}")
            except Exception: pass

    def get_data(table):
        try:
            conn = get_db_conn()
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            conn.close()
            df['dt'] = pd.to_datetime(df['dt'])
            return df[df['dt'] >= df['dt'].max() - timedelta(hours=lb)]
        except Exception: return pd.DataFrame()

    st.title("🛡️ Real Time Monitoring")

    # --- ALWAYS VISIBLE DOWNTIME ALERTS UI ---
    try:
        conn = get_db_conn()
        alerts = pd.read_sql("SELECT method, l1_name, start_time, end_time, status, peak_failures, anomaly_score FROM downtime_logs ORDER BY id DESC LIMIT 15", conn)
        conn.close()
    except: alerts = pd.DataFrame()
        
    active = len(alerts[alerts['status'].isin(['CRITICAL', 'DEGRADED'])]) if not alerts.empty else 0
    with st.expander(f"🚨 {'🔴' if active > 0 else '🟢'} LIVE DOWNTIME & ANOMALY CENTER", expanded=True):
        if active > 0: st.error(f"⚠️ {active} Systems showing 0% SR or Statistical Drops!")
        else: st.success("✅ All Core Systems Operating Within Dynamic Baseline.")
        
        if not alerts.empty:
            alerts['start_time'] = pd.to_datetime(alerts['start_time']).dt.strftime('%H:%M')
            alerts['end_time'] = pd.to_datetime(alerts['end_time']).dt.strftime('%H:%M').replace('NaT', '-')
            
            def color_alert(val):
                if val == 'CRITICAL': return 'background-color: #721c24; color: white;'
                if val == 'DEGRADED': return 'background-color: #856404; color: white;' # Yellow for Z-score drops
                if val == 'RESOLVED': return 'background-color: #155724; color: white;'
                return ''
            st.dataframe(alerts.style.applymap(color_alert, subset=['status']), use_container_width=True, hide_index=True)
            
    st.divider()

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📱 Standard UPI", "💳 Cards", "🏦 NB & Affordibility", "🔁 UPI Recurring", "🔄 Emandate & NACH"])

    with tab1:
        d = get_data('upi_data')
        if not d.empty:
            cond =[
                (d['mode'] == 'upi_qr') | (d['receiver_type'] == 'qr_code'),
                (d['reference2'] == 'credit_card') & (d['flow'] == 'collect'),
                (d['reference2'] == 'credit_card') & (d['flow'] == 'intent'),
                (d['flow'] == 'in_app'), 
                (d['flow'] == 'collect'), 
                (d['flow'] == 'intent')
            ]
            vals =[
                'UPI QR', 
                'CC on UPI Collect', 
                'CC on UPI Intent', 
                'UPI Turbo', 
                'UPI Collect', 
                'UPI Intent'
            ]
            d['method_drilled'] = np.select(cond, vals, default='UPI Unknown')
            render_pane(d, 'method_drilled', "UPI Method Trend", gran)
            render_pane(d, 'cps_route', "UPI CPS Performance", gran)
            render_pane(d[d['flow'] == 'intent'], 'gateway', "Intent Gateway Performance", gran)
            render_pane(d[d['flow'] == 'collect'], 'gateway', "Collect Gateway Performance", gran)
            render_pane(d, 'provider', "UPI PSP Provider Trend", gran)
            render_pane(d, 'bank', "TPV Remitter Bank Trend", gran)
            render_pane(d, 'internal_error_code', "UPI Error Diagnostics", gran)

    with tab2:
        d = get_data('cards_data')
        if not d.empty:
            d['lt'] = d['type'].str.lower()
            cond =[
                (d['method']=='card')&(d['lt']=='debit')&(d['recurring']==1)&(d['recurring_type'].isin(['initial','card_change'])),
                (d['method']=='card')&(d['lt']=='debit')&(d['recurring']==1)&(d['recurring_type']=='auto'),
                (d['method']=='card')&(d['lt']=='credit')&(d['recurring']==1)&(d['recurring_type'].isin(['initial','card_change'])),
                (d['method']=='card')&(d['lt']=='credit')&(d['recurring']==1)&(d['recurring_type']=='auto'),
                (d['method']=='card')&(d['lt']=='debit')&(d['international']==1),
                (d['method']=='card')&(d['lt']=='credit')&(d['international']==1),
                (d['method']=='card')&(d['lt']=='debit')&(d['international']==0),
                (d['method']=='card')&(d['lt']=='credit')&(d['international']==0),
                (d['method']=='emi')&(d['lt']=='debit'), 
                (d['method']=='emi')&(d['lt']=='credit')
            ]
            vals =[
                'Debit-Recurring-Initial', 
                'Debit-Recurring-Auto', 
                'Credit-Recurring-Initial', 
                'Credit-Recurring-Auto', 
                'Debit-International', 
                'Credit-International', 
                'Debit-Domestic', 
                'Credit-Domestic', 
                'DC EMI', 
                'CC EMI'
            ]
            d['method_drilled'] = np.select(cond, vals, default='Other Card')
            render_pane(d, 'method_drilled', "Card Method Trend", gran)
            render_pane(d, 'cps_route', "Cards CPS Performance", gran)
            render_pane(d[d['lt']=='credit'], 'network', "CC Network Health", gran)
            render_pane(d[d['lt']=='credit'], 'gateway', "CC Gateway Performance", gran)
            render_pane(d[d['lt']=='credit'], 'issuer', "CC Issuer Performance", gran)
            render_pane(d[d['lt']=='debit'], 'network', "DC Network Health", gran)
            render_pane(d[d['lt']=='debit'], 'gateway', "DC Gateway Performance", gran)
            render_pane(d[d['lt']=='debit'], 'issuer', "DC Issuer Performance", gran)
            render_pane(d, 'internal_error_code', "Cards Error Analytics", gran)

    with tab3:
        d = get_data('nb_data')
        if not d.empty:
            d['method_drilled'] = np.where(d['method'] == 'app', 'Cred Pay', d['method'])
            render_pane(d[d['method'] == 'netbanking'], 'bank', "NB Bank Performance", gran)
            render_pane(d[d['method'] == 'netbanking'], 'gateway', "NB Gateway Performance", gran)
            render_pane(d[d['method'] != 'netbanking'], 'method_drilled', "Apps & Affordability", gran)

    with tab4:
        d = get_data('recurring_data')
        if not d.empty:
            cond =[
                (d['recurring_type'] == 'initial') & (d['flow'] == 'collect'), 
                (d['recurring_type'] == 'initial') & (d['flow'] == 'intent'), 
                (d['recurring_type'] == 'auto')
            ]
            vals =[
                'Autopay-Collect-Initial', 
                'Autopay-Intent-Initial', 
                'Autopay-Collect-Auto'
            ]
            d['method_drilled'] = np.select(cond, vals, default='Autopay Unknown')
            render_pane(d, 'method_drilled', "UPI Recurring Matrix", gran)
            render_pane(d[d['recurring_type'] == 'initial'], 'gateway', "Initial Setup Gateway", gran)

    with tab5:
        d = get_data('emandate_data')
        if not d.empty:
            d['method_drilled'] = d['method'].str.upper() + " - " + d['recurring_type'].str.title()
            render_pane(d, 'method_drilled', "Emandate & NACH Matrix", gran)
            render_pane(d[d['auth_type'] == 'netbanking'], 'bank', "Emandate Bank (Auth: NB)", gran)
            render_pane(d[d['auth_type'] == 'debitcard'], 'bank', "Emandate Bank (Auth: DC)", gran)

    # --- DYNAMIC IMPACT EXPLORER (MAPPED TO EXACT UI SUB-FLOWS) ---
    IMPACT_MAPPINGS = {
        'UPI Intent': "method='upi' AND flow='intent' AND recurring=0 AND international=0",
        'UPI Collect': "method='upi' AND flow='collect' AND recurring=0 AND international=0",
        'UPI QR': "method='upi' AND (mode='upi_qr' OR receiver_type='qr_code') AND recurring=0 AND international=0",
        'UPI Turbo (in_app)': "method='upi' AND flow='in_app' AND recurring=0 AND international=0",
        'CC on UPI Intent': "method='upi' AND reference2='credit_card' AND flow='intent' AND recurring=0 AND international=0",
        'CC on UPI Collect': "method='upi' AND reference2='credit_card' AND flow='collect' AND recurring=0 AND international=0",
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
        'UPI Autopay - Intent Initial': "method='upi' AND recurring=1 AND recurring_type='initial' AND flow='intent'",
        'UPI Autopay - Collect Initial': "method='upi' AND recurring=1 AND recurring_type='initial' AND flow='collect'",
        'UPI Autopay - Auto': "method='upi' AND recurring=1 AND recurring_type='auto'",
        'Emandate - Initial': "method='emandate' AND recurring_type='initial'",
        'Emandate - Auto': "method='emandate' AND recurring_type='auto'",
        'ALL UPI (Combined)': "method='upi'",
        'ALL Cards (Combined)': "method IN ('card', 'emi')"
    }

    st.divider()
    st.subheader("🎯 Dynamic Merchant Impact Explorer")
    with st.expander("Configure Impact Parameters", expanded=True):
        with st.form("impact_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                imp_sub_cat = st.selectbox("Flow Category", list(IMPACT_MAPPINGS.keys()))
                imp_dim = st.selectbox("Dimension (L1)",['gateway', 'bank', 'issuer', 'network', 'provider', 'cps_route'])
            with col2:
                imp_val = st.text_input("Dimension Value (e.g., upi_mindgate)")
                imp_start = st.text_input("Start Time", (datetime.now() - timedelta(minutes=60)).strftime('%Y-%m-%d %H:%M:%S'))
            with col3:
                imp_end = st.text_input("End Time", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                st.write("")
                st.write("")
                submit_impact = st.form_submit_button("Fetch Impact List")

    if submit_impact:
        where_clause = IMPACT_MAPPINGS[imp_sub_cat]
        impact_sql = f"""
        WITH summary_data AS (
            SELECT 
                GROUPING(business_dba) AS is_total,
                business_dba,
                merchant_id,
                COUNT(id) AS Attempts,
                COUNT(IF (authorized_at <> 0, id, NULL)) * 100.0 / NULLIF(COUNT(id), 0) AS SR
            FROM startree.default.sr_view_v9
            WHERE {where_clause}
                AND {imp_dim} = '{imp_val}'
                AND created_at BETWEEN (to_unixtime(timestamp '{imp_start}') - 19800) 
                                   AND (to_unixtime(timestamp '{imp_end}') - 19800)
            GROUP BY GROUPING SETS (
                (business_dba, merchant_id), 
                ()
            )
        )
        SELECT 
            CASE WHEN is_total = 1 THEN ' GRAND TOTAL ' ELSE business_dba END AS business_dba,
            COALESCE(merchant_id, '-') AS merchant_id,
            Attempts,
            SR
        FROM summary_data
        ORDER BY 
            is_total DESC,
            Attempts DESC
        """
        
        with st.spinner(f"Executing direct Trino query for {imp_sub_cat}..."):
            imp_df = run_trino_query(impact_sql, st.session_state['u'], st.session_state['p'])
            if not imp_df.empty:
                imp_df['SR'] = imp_df['SR'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "0.00%")
                st.success(f"Fetched {len(imp_df)-1} impacted merchants.")
                st.dataframe(imp_df, use_container_width=True, hide_index=True)
            else:
                st.warning("No data found for these parameters.")