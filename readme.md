# 🛰️ Ops Command Center Pro

Ops Command Center Pro is a real-time, multi-threaded payment gateway monitoring 
and analytics dashboard. It continuously pulls live transaction data from a Trino 
data warehouse, caches it locally using SQLite (WAL mode) for lightning-fast UI 
rendering, and actively alerts on critical infrastructure downtimes.

## ✨ Key Features
* Real-Time Downtime Alerts: Scans the latest minute for 0% SR anomalies.
* Multi-Threaded Engine: Detached background worker runs 24H backfill.
* Instantaneous UI: Streamlit polls local SQLite cache every 60 seconds.
* Dynamic Merchant Impact Explorer: 1-click parameterized SQL engine.
* % Share Analytics: Automatically swaps SR for % Share on error code panels.

## 🚀 Setup (Mac) - Terminal (zsh)
# 1. Download the code to your laptop
git clone https://github.com/sagarrazor/real-time-monitoring.git

# 2. Go into the folder
cd real-time-monitoring

# 3. Create a safe, isolated environment 
python3 -m venv venv

# 4. Activate the environment
source venv/bin/activate

# 5. Install the required packages
pip3 install -r requirements.txt

# 6. Launch the App!
python3 -m streamlit run app.py

: Log In
A browser window will automatically open to http://localhost:8501.
CRITICAL: Make sure your company VPN is connected!
Enter your standard LDAP credentials to launch the dashboard.
(To close the app later, just go back to your Terminal and press Control + C).
