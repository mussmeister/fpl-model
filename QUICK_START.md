# Quick Start: Projections Tracker

## Files Created

✓ `scripts/poll_projections.py` - Polling script (runs every 2 hours)
✓ `scripts/view_projections_trends.py` - Streamlit dashboard  
✓ `outputs/projections_history.db` - SQLite database (auto-created)
✓ `logs/` - Folder for polling logs

## One-Time Setup

### 1. Install Streamlit
```powershell
cd "G:\My Drive\FPL_Model"
pip install streamlit plotly
```

### 2. Test manually
```powershell
python scripts/poll_projections.py
```
Should output: `[timestamp] [OK] Stored 80 projections`

### 3. View results in Streamlit
```powershell
streamlit run scripts/view_projections_trends.py
```
Opens browser with interactive dashboard.

---

## Schedule Polling (Optional but Recommended)

To run polling automatically every 2 hours:

### Create batch file: `poll.bat`

Save this as `G:\My Drive\FPL_Model\poll.bat`:

```batch
@echo off
cd "G:\My Drive\FPL_Model"
C:\Users\dommu\AppData\Local\Programs\Python\Python314\python.exe scripts\poll_projections.py >> logs\polling.log 2>&1
```

### Schedule in Windows Task Scheduler

1. Press `Win+R`, type `taskschd.msc`, Enter
2. Right panel → **Create Basic Task**
3. Name: `FPL Projections Poll`
4. **Trigger tab**: 
   - On a schedule → Daily
   - Repeat every 2 hours
5. **Action tab**:
   - Program: `C:\Windows\System32\cmd.exe`
   - Arguments: `/c "G:\My Drive\FPL_Model\poll.bat"`
   - Start in: `G:\My Drive\FPL_Model`
6. Finish & edit properties:
   - **Conditions** tab → Uncheck "Start only on AC power"
   - Click OK

---

## Using the Dashboard

1. Run: `streamlit run scripts/view_projections_trends.py`
2. **Dropdown 1**: Select a match
   - Example: `GW35: Arsenal (H) v Fulham`
3. **Dropdown 2**: Select a metric
   - Goals (G)
   - Goals Conceded (GC)
   - Clean Sheet %
4. **View chart**: See how projections changed over time

---

## Cost
- 1 API call per poll = all 4 GWs, 20 teams
- 12 calls/day × 30 days = **360 calls/month** (under 500 limit) ✓

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: streamlit` | Run `pip install streamlit plotly` |
| No data in dashboard | Run `python scripts/poll_projections.py` manually first |
| Task Scheduler won't run | Test batch file manually in PowerShell |
| Database locked | Refresh page; Streamlit auto-retries |

---

See `POLLING_SETUP.md` for detailed setup instructions.
