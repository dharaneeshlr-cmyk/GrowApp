# FinHub — Personal Finance Portal

Multi-user edition. Deploy on Render or run locally.

---

## 🚀 Run Locally

```bash
pip install -r requirements.txt
python app.py
```

Open: http://127.0.0.1:5000
Default login: `admin` / `admindr`

---

## ☁️ Deploy to Render (GitHub)

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USER/finhub.git
git push -u origin main
```

### 2. Create Render Web Service
1. Go to [render.com](https://render.com) → **New → Web Service**
2. Connect your GitHub repo
3. Render auto-detects `render.yaml` — click **Apply**

That's it. Render will:
- Install dependencies from `requirements.txt`
- Start with `gunicorn` via `Procfile`
- Mount a 1 GB persistent disk at `/data` for the SQLite database
- Generate a secure `SECRET_KEY` automatically

### 3. Environment Variables (set in Render dashboard)
| Key | Value | Notes |
|-----|-------|-------|
| `SECRET_KEY` | *(auto-generated)* | Don't change after first deploy |
| `RENDER` | `true` | Enables HTTPS cookies |
| `DATA_DIR` | `/data` | Persistent disk path |
| `ALLOW_SIGNUP` | `true` → `false` | Set to `false` after creating your accounts |

### 4. First Login
- Visit your Render URL → `/register` to create your account
- Set `ALLOW_SIGNUP=false` in Render env vars to close public registration

---

## 👥 Multi-User

- Every user has **completely isolated data** — budgets, baskets, Kite config
- Kite API credentials are stored per-user
- Admin account is seeded automatically on first run (`admin` / `admindr`) — **change the password immediately**

---

## Modules
| Module | URL |
|--------|-----|
| Home | /home |
| Budget | /budget |
| Investments | /investments |
| Strategies | /strategies |
| Net Worth | /networth |
| AutoBasket | /autobasket |

---

## After Updating

1. Stop: `Ctrl+C`
2. Replace files with new zip contents
3. Restart: `python app.py`
4. Hard-refresh: `Cmd+Shift+R`
