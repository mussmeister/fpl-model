# Projections Tracker Setup Guide

## System Overview

- **Polling Script**: `scripts/poll_projections.py` 
  - Runs every 2 hours
  - Fetches odds, runs DC projections, stores in SQLite database
  - Uses ~25 API credits per run

- **Database**: `outputs/projections_history.db`
  - SQLite database storing all historical projections
  - Auto-created on first run
  - Grows slowly (~400 bytes per projection snapshot)

- **Streamlit App**: `scripts/view_projections_trends.py`
  - Interactive dashboard to view trends
  - Select match + metric via dropdowns
  - Shows time-series chart of both teams

---

## Installation

### 1. Install Streamlit (if not already installed)

```powershell
cd "G:\My Drive\FPL_Model"
pip install streamlit plotly
```

### 2. Test the system manually

```powershell
cd "G:\My Drive\FPL_Model"
python scripts/poll_projections.py
```

You should see: `[timestamp] [OK] Stored 80 projections`

---

## Windows Task Scheduler Setup (2-hour polling)

### Step 1: Create batch file

Create `G:\My Drive\FPL_Model\poll.bat`:

```batch
@echo off
cd "G:\My Drive\FPL_Model"
C:\Users\dommu\AppData\Local\Programs\Python\Python314\python.exe scripts\poll_projections.py >> logs\polling.log 2>&1
```

(Make sure `logs/` folder exists)

### Step 2: Schedule in Windows Task Scheduler

1. **Open Task Scheduler**
   - Press `Win+R`, type `taskschd.msc`, press Enter

2. **Create Basic Task**
   - Click "Create Basic Task" (right panel)
   - Name: `FPL Projections Poll`
   - Description: `Fetch FPL projections every 2 hours`

3. **Set Trigger**
   - Trigger: "On a schedule"
   - Recurrence: Daily
   - Repeat: Every 2 hours
   - Duration: 24 hours (repeats indefinitely)

4. **Set Action**
   - Action: "Start a program"
   - Program: `C:\Windows\System32\cmd.exe`
   - Arguments: `/c "G:\My Drive\FPL_Model\poll.bat"`
   - Start in: `G:\My Drive\FPL_Model`

5. **Finish**
   - Check "Open the Properties dialog for this task when I click Finish"
   - Go to "Conditions" tab → Uncheck "Start the task only if the computer is on AC power"
   - Click OK

---

## Viewing Results

### Via Streamlit (Recommended)

```powershell
cd "G:\My Drive\FPL_Model"
streamlit run scripts/view_projections_trends.py
```

This opens a browser-based dashboard with:
- Dropdown 1: Select match (e.g., "GW35: Arsenal (H) v Bournemouth")
- Dropdown 2: Select metric (Goals, GC, CS%)
- Interactive time-series chart
- Data tables

### Via SQLite directly

```powershell
cd "G:\My Drive\FPL_Model"
sqlite3 outputs\projections_history.db
# Then query, e.g.:
# SELECT timestamp, team, opponent, g, gc FROM projections WHERE gw=35 AND team='Arsenal' ORDER BY timestamp;
```

---

## Cost Analysis

- **Per poll**: 1 API call (covers all 20 teams × 4 GWs)
- **Per day**: 12 calls (every 2 hours)
- **Per month**: ~360 calls
- **Budget**: 500 calls/month ✓

---

## Troubleshooting

### Database is locked
- Streamlit is reading while polling is writing
- Solution: Streamlit auto-retries; just wait a few seconds

### No data showing in Streamlit
- Run `python scripts/poll_projections.py` manually first to populate DB
- Wait for at least 2 data points before trends are visible

### Task Scheduler not running
- Check logs: `logs/polling.log`
- Verify Python path in batch file: `python -c "import sys; print(sys.executable)"`
- Run batch file manually first to test

---

## Next Steps

1. Create `logs/` folder
2. Create `poll.bat` and schedule it
3. Let it run for 24 hours to get initial trend data
4. Open Streamlit dashboard and explore

Enjoy tracking market moves! 📊
