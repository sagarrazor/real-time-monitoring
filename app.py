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
    if df.empty: return
    latest_dt = df['dt'].max()
    time_str = pd.to_datetime(latest_dt).strftime('%Y-%m-%d %H:%M:%S')
    
    # 1. GLOBAL BLACKOUT CHECK
    total_att = df[df['dt'] == latest_dt]['attempts'].sum()
    total_succ = df[df['dt'] == latest_dt]['success'].sum()
    if total_att > 100 and total_succ == 0:
        exists = pd.read_sql(f"SELECT id FROM downtime_logs WHERE method='{method_name}' AND l1_name='GLOBAL' AND status='SYSTEM-WIDE'", conn)
        if exists.empty:
            conn.execute("INSERT INTO downtime_logs (method, l1_type, l1_name, start_time, status, peak_failures, anomaly_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (method_name, 'ALL', 'GLOBAL', time_str, 'SYSTEM-WIDE', int(total_att), 100.0))
        return 

    # 2. L1 ANOMALY CHECK
    recent = df[df['dt'] == latest_dt].groupby(l1_col).agg({'attempts': 'sum', 'success': 'sum'}).reset_index()
    try:
        hist_df = pd.read_sql(f"SELECT {l1_col}, attempts, success FROM {table_name} WHERE dt >= datetime('now', '-2 hours')", conn)
    except: hist_df = pd.DataFrame()

    for _, row in recent.iterrows():
        name = str(row[l1_col])
        if name.strip() == '' or name.lower() in ['nan', 'none', 'unknown']: continue
        cur_att = int(row['attempts'])
        cur_sr = (row['success'] / cur_att * 100) if cur_att > 0 else 0
        status, score = None, 0.0

        # CRITICAL: Hard 0% SR
        if cur_att > 20 and row['success'] == 0:
            status, score = "CRITICAL", 99.0 
        
        # DEGRADED: Dynamic Anomaly (Hardened)
        elif not hist_df.empty and cur_att > 100: # Only alert on significant volume
            h = hist_df[hist_df[l1_col] == name].copy()
            if len(h) > 15:
                h['sr'] = (h['success'] / h['attempts'] * 100).fillna(0)
                mean_sr, std_sr = h['sr'].mean(), h['sr'].std()
                z = (cur_sr - mean_sr) / (std_sr if std_sr > 1.0 else 1.0)
                
                # REQUIREMENT: Z < -4 (High Confidence) AND Current SR is > 10% lower than Mean
                if z < -4.0 and (mean_sr - cur_sr) > 10.0:
                    status, score = "DEGRADED", abs(round(z, 2))

        if status:
            exists = pd.read_sql(f"SELECT id FROM downtime_logs WHERE method='{method_name}' AND l1_name='{name}' AND status IN ('CRITICAL', 'DEGRADED')", conn)
            if exists.empty:
                conn.execute("INSERT INTO downtime_logs (method, l1_type, l1_name, start_time, status, peak_failures, anomaly_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
                             (method_name, l1_col, name, time_str, status, cur_att, score))
        elif cur_sr > (mean_sr - 2.0 if 'mean_sr' in locals() else 5.0):
            # Resolve if within 2% of mean
            conn.execute("UPDATE downtime_logs SET end_time = ?, status = 'RESOLVED' WHERE method = ? AND l1_name = ? AND status IN ('CRITICAL', 'DEGRADED', 'SYSTEM-WIDE')",
                         (time_str, method_name, name))

def execute_single_sync(table_name, sql, user, pwd):
    start_ts = time.time() # Start Latency Timer
    try:
        df = run_trino_query(sql, user, pwd)
        duration = round(time.time() - start_ts, 1) # Calculate Latency
        
        if not df.empty:
            conn = get_db_conn()
            df['dt'] = pd.to_datetime(df['dt'])
            min_dt = df['dt'].min().strftime('%Y-%m-%d %H:%M:%S')
            
            try: conn.execute(f"DELETE FROM {table_name} WHERE dt >= ?", (min_dt,))
            except: pass
                
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
            elif table_name == 'recurring_data':
                check_downtimes(df, 'UPI Recurring', 'gateway', conn, table_name)
                
            conn.commit()
            conn.close()
            # LOG SUCCESS WITH LATENCY
            log_event(f"✅ {table_name} Integrated: {len(df)} rows (Took {duration}s)")
        else:
            log_event(f"⚠️ {table_name} returned 0 rows (Took {duration}s)")
    except Exception as e: 
        log_event(f"❌ {table_name} Failed after {round(time.time()-start_ts,1)}s: {str(e)[:40]}")
        
def background_engine(user, pwd):
    log_event("🚀 Engine Online. Starting 24H Backfill...")
    first_run = True
    while True:
        lookback = 86400 if first_run else 900 
        t_filter = f"(created_at + 19800) >= CAST(TO_UNIXTIME(CURRENT_TIMESTAMP) AS BIGINT) + 19800 - {lookback}"
        
        # LOG INITIATION
        fire_time = datetime.now().strftime('%H:%M:%S')
        log_event(f"📡 Batch Sync Triggered at {fire_time}")
        
        # ... queries dict remains same ...
        queries = {
            'upi_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, flow, mode, receiver_type, reference2, gateway, provider, bank, cps_route, internal_error_code, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method = 'upi' AND recurring = 0 AND international = 0 GROUP BY 1,2,3,4,5,6,7,8,9,10,11",
            'cards_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, type, international, recurring, recurring_type, network, issuer, cps_route, gateway, internal_error_code, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method IN ('card', 'emi') GROUP BY 1,2,3,4,5,6,7,8,9,10,11",
            'nb_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, bank, gateway, provider, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method IN ('netbanking', 'wallet', 'app') GROUP BY 1,2,3,4,5",
            'emandate_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, recurring, recurring_type, bank, auth_type, gateway, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND method IN ('emandate', 'nach') GROUP BY 1,2,3,4,5,6,7",
            'recurring_data': f"SELECT DATE_TRUNC('minute', FROM_UNIXTIME(created_at + 19800)) AS dt, method, recurring, flow, recurring_type, gateway, COUNT(id) as attempts, SUM(CASE WHEN authorized_at > 0 THEN 1 ELSE 0 END) as success FROM startree.default.sr_view_v9 WHERE {t_filter} AND status <> 'created' AND recurring = 1 AND method = 'upi' GROUP BY 1,2,3,4,5,6"
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            for t, s in queries.items(): ex.submit(execute_single_sync, t, s, user, pwd)
        
        if first_run: log_event("INITIAL_SYNC_COMPLETE"); first_run = False
        
        now = time.time()
        sleep_time = 300 - (now % 300)
        next_run = datetime.fromtimestamp(now + sleep_time).strftime('%H:%M:%S')
        log_event(f"💤 Sleeping. Next Aligned Sync at {next_run}")
        time.sleep(sleep_time)

# --- 4. RENDER UTILS ---
def get_relative_text_color(val, row_min, row_max, is_sr=True):
    """Interpolates text color with a sensitivity dead-zone."""
    try:
        val, r_min, r_max = float(val), float(row_min), float(row_max)
        
        # SENSITIVITY FLOOR: If the total drop in the row is tiny (< 5%), don't show Red.
        sensitivity_threshold = 5.0
        if (r_max - r_min) < sensitivity_threshold:
            if is_sr:
                return 'color: #2e7d32; font-weight: bold;' if r_max > 80 else 'color: #e61919; font-weight: bold;'
            else:
                return 'color: #2e7d32; font-weight: bold;' if r_max < 10 else 'color: #e61919; font-weight: bold;'

        # Calculate relative position
        pos = (val - r_min) / (r_max - r_min)
        if not is_sr: pos = 1 - pos 
        
        # Damping the gradient: Makes the 'Yellow' zone wider so it's less jumpy
        if pos > 0.8: # Top 20% performance -> Solid Green
            r, g = 0, 180
        elif pos < 0.2: # Bottom 20% performance -> Solid Red
            r, g = 230, 0
        elif pos < 0.5: # Red to Yellow
            r, g = 230, int(420 * pos)
        else: # Yellow to Green
            r, g = int(420 * (1 - pos)), 180
            
        return f'color: rgb({r},{g},0); font-weight: bold;'
    except: return ''

def color_grade_styler(row):
    """Applies relative heatmapping to text in a single row."""
    styles = ['' for _ in row.index]
    
    # Apply to SR columns
    sr_indices = [i for i, col in enumerate(row.index) if col[1] == 'SR']
    if sr_indices:
        vals = row.iloc[sr_indices].astype(float)
        r_min, r_max = vals.min(), vals.max()
        for idx in sr_indices:
            styles[idx] = get_relative_text_color(row.iloc[idx], r_min, r_max, is_sr=True)
            
    # Apply to % Share columns
    share_indices = [i for i, col in enumerate(row.index) if col[1] == '% Share']
    if share_indices:
        vals = row.iloc[share_indices].astype(float)
        r_min, r_max = vals.min(), vals.max()
        for idx in share_indices:
            styles[idx] = get_relative_text_color(row.iloc[idx], r_min, r_max, is_sr=False)
            
    return styles

def render_pane(df, dimension, title, gran):
    if df.empty or dimension not in df.columns: return
    st.subheader(f"📊 {title}")
    work_df = df.copy()
    work_df[dimension] = work_df[dimension].fillna('Unknown')
    work_df['dt_bucket'] = work_df['dt'].dt.floor(gran)
    pivot = work_df.pivot_table(index=dimension, columns='dt_bucket', values=['attempts', 'success'], aggfunc='sum').fillna(0)
    pivot.columns = pivot.columns.swaplevel(0, 1)
    
    frames = []
    for ts in sorted(work_df['dt_bucket'].dropna().unique()):
        if ts in pivot:
            a, s = pivot[ts]['attempts'].astype(int), pivot[ts]['success'].astype(int)
            if dimension == 'internal_error_code':
                m_val, m_name = (a / a.sum() * 100).fillna(0) if a.sum() > 0 else a * 0, '% Share'
            else:
                m_val, m_name = ((s / a.replace(0, np.nan)) * 100).fillna(0), 'SR'
            frames.append(pd.DataFrame({(ts.strftime('%H:%M'), 'Attempts'): a, (ts.strftime('%H:%M'), m_name): m_val}))
            
    if frames:
        fdf = pd.concat(frames, axis=1)
        last_t = pd.to_datetime(work_df['dt_bucket']).max().strftime('%H:%M')
        if (last_t, 'Attempts') in fdf.columns: fdf = fdf.sort_values(by=(last_t, 'Attempts'), ascending=False).head(50)
        
        # Applied axis=1 for relative text coloring
        st.dataframe(
            fdf.style.apply(color_grade_styler, axis=1).format({
                c: '{:.2f}%' if c[1] in ['SR', '% Share'] else '{:,.0f}' for c in fdf.columns
            }), 
            use_container_width=True
        )
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
        lb = st.selectbox("History", [1, 2, 6, 12], index=1)
        gran = st.selectbox("Granularity", ["1min", "5min", "15min", "1h"], index=1)
        
        st.divider()
        with st.expander("📡 Engine Sync Logs", expanded=True): # Kept expanded so you see it
            try:
                conn = get_db_conn()
                # Pull last 30 logs
                sync_logs = pd.read_sql("SELECT timestamp, message FROM system_logs ORDER BY id DESC LIMIT 30", conn)
                conn.close()
                for _, r in sync_logs.iterrows():
                    # Color success and fail differently
                    if "✅" in r['message']:
                        st.caption(f"**{r['timestamp']}** :green[{r['message']}]")
                    elif "❌" in r['message']:
                        st.caption(f"**{r['timestamp']}** :red[{r['message']}]")
                    else:
                        st.caption(f"**{r['timestamp']}** {r['message']}")
            except: pass

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
                if val == 'SYSTEM-WIDE': return 'background-color: #ff0000; color: white; font-weight: bold; border: 2px solid white;'
                if val == 'CRITICAL': return 'background-color: #721c24; color: white;'
                if val == 'DEGRADED': return 'background-color: #856404; color: white;'
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
            render_pane(d, 'gateway', "Emandate Gateway", gran)
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
    st.subheader("🎯 Merchant Impact Explorer (Manual Precision)")
    with st.expander("Investigate Incident Window (A to B)", expanded=True):
        with st.form("impact_form"):
            col_setup, col_start, col_end = st.columns([2, 1.5, 1.5])
            with col_setup:
                # Key parameter ensures persistent state
                imp_sub_cat = st.selectbox("Flow Category", list(IMPACT_MAPPINGS.keys()), key="f_cat")
                imp_dim = st.selectbox("Dimension (L1)", ['gateway', 'bank', 'issuer', 'network', 'provider', 'cps_route'], key="f_dim")
                imp_val = st.text_input("Dimension Value (e.g. upi_mindgate)", key="f_val")
            with col_start:
                st.write("**Incident Start**")
                s_date = st.date_input("Date", datetime.now(), key="f_sd")
                s_hour = st.number_input("Hour (0-23)", 0, 23, datetime.now().hour, key="f_sh")
                s_min = st.number_input("Minute (0-59)", 0, 59, 0, key="f_sm")
            with col_end:
                st.write("**Incident End**")
                e_date = st.date_input("Date", datetime.now(), key="f_ed")
                e_hour = st.number_input("Hour (0-23)", 0, 23, datetime.now().hour, key="f_eh")
                e_min = st.number_input("Minute (0-59)", 0, 59, 59, key="f_em")
            submit_impact = st.form_submit_button("Fetch Impact List")

    if submit_impact:
        ts_start = f"{s_date.strftime('%Y-%m-%d')} {s_hour:02}:{s_min:02}:00"
        ts_end = f"{e_date.strftime('%Y-%m-%d')} {e_hour:02}:{e_min:02}:59"
        impact_sql = f"""
        WITH summary_data AS (
            SELECT GROUPING(business_dba) AS is_total, business_dba, merchant_id, COUNT(id) AS Attempts,
            COUNT(IF (authorized_at <> 0, id, NULL)) * 100.0 / NULLIF(COUNT(id), 0) AS SR
            FROM startree.default.sr_view_v9
            WHERE {IMPACT_MAPPINGS[imp_sub_cat]} AND status <> 'created' AND {imp_dim} = '{imp_val}'
            AND (created_at + 19800) BETWEEN (to_unixtime(timestamp '{ts_start}')) AND (to_unixtime(timestamp '{ts_end}'))
            GROUP BY GROUPING SETS ((business_dba, merchant_id), ())
        )
        SELECT CASE WHEN is_total = 1 THEN ' GRAND TOTAL ' ELSE business_dba END AS business_dba,
        COALESCE(merchant_id, '-') AS merchant_id, Attempts, SR FROM summary_data
        ORDER BY is_total DESC, Attempts DESC
        """
        with st.spinner("Executing Trino Query..."):
            imp_df = run_trino_query(impact_sql, st.session_state['u'], st.session_state['p'])
            if not imp_df.empty:
                imp_df['SR'] = imp_df['SR'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "0.00%")
                st.dataframe(imp_df, use_container_width=True, hide_index=True)
            else: st.warning("No data found for this window.")