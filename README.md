# AlgoRhythm Bybit Bot v8

Auto-trading bot for Bybit crypto. Runs 24/7 on Render + GitHub Pages.

---

## Files

| File | Purpose |
|------|---------|
| `bybit_proxy.py` | Python proxy server — runs on Render 24/7 |
| `index.html` | Bot frontend — hosted on GitHub Pages |
| `requirements.txt` | Python dependencies for Render |
| `render.yaml` | Render deployment config |

---

## STEP 1 — Push to GitHub

1. Go to https://github.com and create a **new repository**
   - Name it: `bybit-bot` (or anything you like)
   - Set it to **Public** (required for free GitHub Pages)
   - Do NOT initialise with README (you already have one)

2. Upload all files in this folder to that repository
   - Click **Add file → Upload files**
   - Drag all files in — `bybit_proxy.py`, `index.html`, `requirements.txt`, `render.yaml`, `.gitignore`, `README.md`
   - Click **Commit changes**

---

## STEP 2 — Deploy Proxy on Render

1. Go to https://render.com and sign up (free)

2. Click **New → Web Service**

3. Connect your GitHub account and select your `bybit-bot` repository

4. Render will auto-detect `render.yaml` — settings fill in automatically:
   - **Name:** bybit-proxy
   - **Runtime:** Python
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python bybit_proxy.py`

5. Click **Create Web Service**

6. Wait ~2 minutes for first deploy. You'll see:
   ```
   AlgoRhythm — Bybit Proxy
   Mode: DEMO (Paper Trading)
   Running on http://0.0.0.0:XXXX
   ```

7. **Copy your Render URL** — it looks like:
   `https://bybit-proxy-xxxx.onrender.com`

---

## STEP 3 — Update HTML with Your Render URL

1. Open `index.html` in any text editor

2. Find this line (near the top of the JavaScript):
   ```javascript
   const RENDER_PROXY = 'https://YOUR-APP-NAME.onrender.com';
   ```

3. Replace `YOUR-APP-NAME` with your actual Render app name:
   ```javascript
   const RENDER_PROXY = 'https://bybit-proxy-xxxx.onrender.com';
   ```

4. Save the file and re-upload it to GitHub (or commit the change)

---

## STEP 4 — Enable GitHub Pages

1. In your GitHub repository, click **Settings**

2. Scroll to **Pages** in the left sidebar

3. Under **Source**, select:
   - Branch: `main`
   - Folder: `/ (root)`

4. Click **Save**

5. GitHub will give you a URL like:
   `https://YOUR-USERNAME.github.io/bybit-bot`

6. Open that URL on any device — your bot is now live 24/7

---

## STEP 5 — Switch to Live Trading (when ready)

1. In Render dashboard → your service → **Environment**
2. Find `DEMO_MODE` variable
3. Change value from `true` to `false`
4. Click **Save** — Render restarts automatically

⚠️ Only do this after testing on demo for at least 2 weeks.

---

## Keeping the Render Free Tier Alive

Render free tier spins down after 15 minutes of inactivity.
To keep it always on, use a free uptime monitor:

1. Go to https://uptimerobot.com (free)
2. Add a new monitor:
   - Type: HTTP(s)
   - URL: `https://YOUR-APP.onrender.com/healthz`
   - Interval: every 10 minutes
3. This pings your proxy every 10 min so it never sleeps

---

## Troubleshooting

**Bot shows "Proxy offline"**
→ Check Render dashboard — service may be starting up (takes ~30 sec on free tier after sleep)

**Orders not placing**
→ Check your Bybit API key has Trade permission enabled
→ Make sure IP restriction is OFF on your Bybit API key

**Wrong prices / no data**
→ Check DEMO_MODE setting matches your Bybit account type
