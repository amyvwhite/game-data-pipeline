# Going live on GitHub (about 15 minutes)

These steps put the project on GitHub, make it run every hour by itself, and publish
the dashboard at `https://<your-username>.github.io/game-data-pipeline/`.

## 1. Create the repository and upload the code

1. On github.com, click **+ → New repository**. Name it **game-data-pipeline**, set it
   to **Public**, and **don't** add a README, .gitignore or license. The project already
   has them.
2. In **GitHub Desktop**: **File → Add local repository**, choose the
   `game-data-pipeline` folder, click **create a repository**, then **Publish repository**.
   Untick "Keep this code private". If it says the repo already exists on GitHub, use
   **Repository → Repository settings → Remote** and paste the repo URL instead.

The `.gitignore` keeps out `.env` (your secrets), raw data and logs. Only code, the docs
and `data/warehouse/` (the history) are uploaded.

## 2. Bring over the history you've already collected (optional, recommended)

If your PC has been collecting hourly data in Docker, export it so the live version
starts with it. In the project folder (Docker Desktop terminal):

```
docker compose run --rm pipeline python -m pipeline.warehouse dump
```

This writes `data/warehouse/*.csv`. Commit and push those files in GitHub Desktop
(summary: "Seed history from local collection"). Then double-click
`STOP_HOURLY_COLLECTION.bat`, because GitHub takes over from here.

## 3. Turn on the two GitHub settings

In the repo on github.com:

1. **Settings → Pages → Build and deployment → Source: GitHub Actions**
2. **Settings → Actions → General → Workflow permissions: Read and write permissions → Save**

## 4. Start it

**Actions** tab → **Live pipeline** → **Run workflow**. The first run takes about 3–4
minutes. When it's green, the dashboard link appears on the run page and under
**Settings → Pages**. From then on it runs every hour by itself.

## 5. Optional extras (repo secrets)

**Settings → Secrets and variables → Actions → New repository secret**:

| Secret | What it adds | Where to get it |
|---|---|---|
| `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET` | Twitch viewers per game | https://dev.twitch.tv/console → Register your application (Category: Analytics Tool, OAuth Redirect URL: `http://localhost`) → copy the Client ID, then **New Secret** |
| `SLACK_WEBHOOK_URL` | Alerts in Slack for anomalies and failed checks | Slack → Apps → Incoming Webhooks |

Secrets are encrypted. They never appear in the code or the logs.

## 6. Make the repo look finished

- **About** (gear icon, top right of the repo page): add the dashboard link as the
  website, and the topics `data-engineering`, `pyspark`, `postgresql`, `etl`,
  `github-actions`, `data-quality`.
- **Pin** the repo on your profile.
- After a week, take a screenshot of the dashboard and replace `docs/dashboard.png`.

## If a run fails

Open the run in the **Actions** tab and expand **Run the pipeline**. Exit code 1 means a
data-quality check blocked the load (nothing bad was saved). Exit code 2 means a crash.
The data and dashboard from the last good run stay in place either way.
