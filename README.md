# FinHub — Local Finance Portal

## Quick Start

```bash
pip install flask
bash run.sh
```

Open: http://127.0.0.1:5000  
Login: `admin` / `admindr`

## ⚠ After updating the zip — ALWAYS do this:

1. Stop Flask: `Ctrl+C`
2. Replace ALL files with the new zip contents
3. Restart: `bash run.sh`
4. Hard-refresh browser: `Cmd+Shift+R`

### Verify all modules loaded:
The terminal should print ALL of these:
```
🚀 FinHub running at http://127.0.0.1:5000
🔑 Login: admin / admindr
🧺 AutoBasket: 10 routes registered
```

If you see `❌ AutoBasket routes NOT found` — the old app.py is running. Stop and restart.

## Modules
| Module | URL |
|--------|-----|
| Home | /home |
| BudgetCraft | /budget |
| Investment Tracker | /investments |
| Investment Strategies | /strategies |
| Net Worth | /networth |
| **AutoBasket** | **/autobasket** |
