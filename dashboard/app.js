// Reads data/summary.json (written by pipeline/export.py) and draws the page.
// Plain JavaScript, no framework. Serve this folder over HTTP, e.g.
//   python -m http.server 8000 --directory dashboard

const SUMMARY_URL = "data/summary.json";

const css = (name) => getComputedStyle(document.querySelector(".viz-root")).getPropertyValue(name).trim();
const fmtInt = (n) => (n == null ? "–" : Math.round(n).toLocaleString("en-CA"));
const fmtMoney = (cents, currency) =>
  cents == null ? "–" : (cents / 100).toLocaleString("en-CA", { style: "currency", currency: currency || "CAD" });

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "style") node.setAttribute("style", v);
    else node[k] = v;
  }
  for (const c of children) node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return node;
}

// Colour = identity. Games are shown one group at a time (Take-Two: 7, competitors: 4),
// so each group gets the 7-colour palette in a fixed order and a game always keeps its
// colour, whichever chart it is in. Never more than 7 lines on one chart.
let GROUPS = {};
const groupOf = (appId) => GROUPS[String(appId)] || "Take-Two";
function colourMap(appIds) {
  const map = new Map();
  const seen = {};
  [...appIds].sort((a, b) => a - b).forEach((id) => {
    const g = groupOf(id);
    seen[g] = (seen[g] || 0) + 1;
    map.set(id, css(`--series-${((seen[g] - 1) % 7) + 1}`));
  });
  return map;
}
let currentGroup = "Take-Two";
const inGroup = (appId) => groupOf(appId) === currentGroup;
const onGroupChange = [];

function groupByApp(rows) {
  const byApp = new Map();
  for (const r of rows) {
    if (!byApp.has(r.app_id)) byApp.set(r.app_id, { name: r.name, rows: [] });
    byApp.get(r.app_id).rows.push(r);
  }
  return byApp;
}

function renderTiles(data, byApp) {
  const latestDate = data.run_date;
  let total = 0;
  let top = null;
  for (const [, app] of byApp) {
    const today = app.rows.find((r) => r.snapshot_date === latestDate);
    if (!today) continue;
    total += today.avg_players;
    if (!top || today.avg_players > top.avg) top = { name: app.name, avg: today.avg_players };
  }
  const onSale = data.latest_prices.filter((p) => p.discount_percent > 0);
  const share = (data.market_share || []).find((r) => r.snapshot_date === latestDate && r.publisher_group === "Take-Two");
  const cov = data.coverage || {};
  const tiles = [
    {
      label: "Take-Two share of players",
      value: share ? `${share.share_pct}%` : "–",
      sub: `across ${byApp.size} tracked games, ${latestDate}`,
    },
    {
      label: "Hourly snapshots collected",
      value: fmtInt(cov.snapshots),
      sub: cov.first_snapshot ? `since ${new Date(cov.first_snapshot).toLocaleDateString("en-CA")} · ${cov.days_collected} days` : "",
    },
    { label: "Avg players across all games", value: fmtInt(total), sub: latestDate },
    { label: "Most played", value: top ? top.name : "–", sub: top ? `${fmtInt(top.avg)} avg players` : "" },
    { label: "On sale now", value: onSale.length, sub: onSale.map((p) => p.name).join(", ") || "none" },
  ];
  const box = document.getElementById("tiles");
  box.replaceChildren(
    ...tiles.map((t) =>
      el("div", { class: "tile" }, el("div", { class: "label" }, t.label), el("div", { class: typeof t.value === "string" && isNaN(t.value.replace(/[,%]/g, "")) ? "value text" : "value" }, t.value),
        el("div", { class: "sub" }, t.sub))
    )
  );
}

function renderPlayers(byApp, colours) {
  const dates = [...new Set([...byApp.values()].flatMap((a) => a.rows.map((r) => r.snapshot_date)))].sort();
  const datasetsFor = (metric) =>
    [...byApp].filter(([id]) => inGroup(id)).map(([id, app]) => {
      const byDate = new Map(app.rows.map((r) => [r.snapshot_date, r[metric]]));
      return {
        label: app.name,
        data: dates.map((d) => byDate.get(d) ?? null),
        borderColor: colours.get(id),
        backgroundColor: colours.get(id),
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 5,
        pointHoverBorderWidth: 2,
        pointHoverBorderColor: css("--surface-1"),
        tension: 0.25,
        spanGaps: false, // show missing days as gaps, don't hide them
      };
    });

  const chart = new Chart(document.getElementById("players-chart"), {
    type: "line",
    data: { labels: dates, datasets: datasetsFor("avg_players") },
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false }, // crosshair-style tooltip for all games on a day
      plugins: {
        legend: { position: "bottom", labels: { usePointStyle: true, pointStyle: "line", color: css("--text-secondary") } },
        tooltip: {
          itemSort: (a, b) => (b.raw ?? 0) - (a.raw ?? 0),
          callbacks: { label: (ctx) => ` ${ctx.dataset.label}: ${fmtInt(ctx.raw)}` },
        },
      },
      scales: {
        x: { grid: { display: false }, ticks: { color: css("--text-muted"), maxTicksLimit: 8 } },
        y: {
          beginAtZero: true,
          grid: { color: css("--grid") },
          border: { display: false },
          ticks: { color: css("--text-muted"), callback: (v) => fmtInt(v) },
        },
      },
    },
  });

  const metricNow = () => (document.getElementById("metric-toggle").checked ? "max_players" : "avg_players");
  const redraw = () => {
    chart.data.datasets = datasetsFor(metricNow());
    chart.update();
  };
  document.getElementById("metric-toggle").addEventListener("change", redraw);
  onGroupChange.push(redraw);

  // Table view of the same numbers (accessible alternative to the chart).
  const table = document.getElementById("players-table");
  const head = el("tr", {}, el("th", {}, "Date"), ...[...byApp.values()].map((a) => el("th", { class: "num" }, a.name)));
  const body = dates
    .slice()
    .reverse()
    .map((d) =>
      el("tr", {}, el("td", {}, d),
        ...[...byApp.values()].map((a) => el("td", { class: "num" }, fmtInt(a.rows.find((r) => r.snapshot_date === d)?.avg_players))))
    );
  table.replaceChildren(el("thead", {}, head), el("tbody", {}, ...body));
}

function renderPrices(prices, colours) {
  const table = document.getElementById("prices-table");
  const head = el("tr", {}, el("th", {}, "Game"), el("th", { class: "num" }, "Price"), el("th", {}, ""));
  const rows = prices.map((p) => {
    const price = el("td", { class: "num" });
    if (p.is_free) price.append("Free");
    else if (p.is_purchasable === false || p.final_price_cents == null) price.append("Not sold separately");
    else {
      if (p.discount_percent > 0) price.append(el("span", { class: "was" }, fmtMoney(p.initial_price_cents, p.currency)));
      price.append(fmtMoney(p.final_price_cents, p.currency));
    }
    const note = el("td");
    if (p.discount_percent > 0) note.append(el("span", { class: "badge" }, `−${p.discount_percent}%`));
    if (p.prev_final_cents != null && p.prev_final_cents !== p.final_price_cents) note.append(" price changed today");
    const name = el("td", {}, p.name, groupOf(p.app_id) === "Competitor" ? el("span", { class: "tag" }, "competitor") : "");
    return el("tr", {}, name, price, note);
  });
  table.replaceChildren(el("thead", {}, head), el("tbody", {}, ...rows));
}

function renderReviews(reviews) {
  const sorted = reviews.filter((r) => r.positive_ratio != null).sort((a, b) => b.positive_ratio - a.positive_ratio);
  new Chart(document.getElementById("reviews-chart"), {
    type: "bar",
    data: {
      labels: sorted.map((r) => r.name),
      datasets: [{
        label: "Positive reviews",
        data: sorted.map((r) => +(r.positive_ratio * 100).toFixed(1)),
        backgroundColor: css("--accent"),
        borderRadius: 4,
        barThickness: 14,
      }],
    },
    options: {
      indexAxis: "y",
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false }, // one series: the card title names it
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const r = sorted[ctx.dataIndex];
              return ` ${ctx.raw}% positive · ${r.review_score_desc} · ${fmtInt(r.total_reviews)} reviews`;
            },
          },
        },
      },
      scales: {
        x: { min: 0, max: 100, grid: { color: css("--grid") }, border: { display: false },
             ticks: { color: css("--text-muted"), callback: (v) => `${v}%` } },
        y: { grid: { display: false }, ticks: { color: css("--text-secondary") } },
      },
    },
  });
}

const LINE_STYLE = (colour) => ({
  borderColor: colour,
  backgroundColor: colour,
  borderWidth: 2,
  pointRadius: 0,
  pointHoverRadius: 5,
  pointHoverBorderWidth: 2,
  pointHoverBorderColor: css("--surface-1"),
  tension: 0.25,
});
const AXIS = (extra = {}) => ({
  grid: { color: css("--grid") },
  border: { display: false },
  ticks: { color: css("--text-muted") },
  ...extra,
});
const hourLabel = (h) => `${((h + 11) % 12) + 1}${h < 12 ? "am" : "pm"}`;
const th = (text, num = false) => el("th", num ? { class: "num" } : {}, text);
const td = (text, num = false) => el("td", num ? { class: "num" } : {}, text);

function fillTable(table, headers, rows) {
  table.replaceChildren(
    el("thead", {}, el("tr", {}, ...headers.map(([t, num]) => th(t, num)))),
    el("tbody", {}, ...rows.map((cells) => el("tr", {}, ...cells.map(([t, num]) => td(t, num)))))
  );
}

function emptyRow(table, cols, message) {
  table.replaceChildren(el("tbody", {}, el("tr", {}, el("td", { colSpan: cols, class: "muted" }, message))));
}

// Players by hour of day, as % of each game's own average: big and small games share one axis.
function renderRhythm(data, colours) {
  const names = new Map(data.daily_players.map((r) => [r.app_id, r.name]));
  const byApp = new Map();
  for (const r of data.hourly_profile || []) {
    if (!byApp.has(r.app_id)) byApp.set(r.app_id, new Map());
    byApp.get(r.app_id).set(r.local_hour, r.avg_players);
  }
  const hours = [...Array(24).keys()];
  const datasets = () =>
    [...byApp]
      .filter(([id]) => inGroup(id))
      .map(([id, byHour]) => {
        const vals = [...byHour.values()];
        const mean = vals.reduce((a, b) => a + b, 0) / (vals.length || 1);
        return {
          label: names.get(id) || String(id),
          data: hours.map((h) => (byHour.has(h) && mean ? +((100 * byHour.get(h)) / mean).toFixed(1) : null)),
          ...LINE_STYLE(colours.get(id)),
          spanGaps: true,
        };
      });
  const chart = new Chart(document.getElementById("rhythm-chart"), {
    type: "line",
    data: { labels: hours.map(hourLabel), datasets: datasets() },
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "bottom", labels: { usePointStyle: true, pointStyle: "line", color: css("--text-secondary") } },
        tooltip: {
          itemSort: (a, b) => (b.raw ?? 0) - (a.raw ?? 0),
          callbacks: { label: (ctx) => ` ${ctx.dataset.label}: ${ctx.raw}% of its average` },
        },
      },
      scales: {
        x: { grid: { display: false }, ticks: { color: css("--text-muted"), maxTicksLimit: 12 } },
        y: AXIS({ ticks: { color: css("--text-muted"), callback: (v) => `${v}%` } }),
      },
    },
  });
  onGroupChange.push(() => {
    chart.data.datasets = datasets();
    chart.update();
  });

  const table = document.getElementById("peak-table");
  const rows = data.peak_hours || [];
  if (!rows.length) return emptyRow(table, 5, "Needs a few hours of hourly snapshots.");
  fillTable(
    table,
    [["Game"], ["Busiest hour"], ["Avg players then", true], ["Quietest hour"], ["Avg players then", true]],
    rows.map((r) => [
      [r.name], [hourLabel(r.peak_hour_toronto)], [fmtInt(r.peak_avg_players), true],
      [hourLabel(r.quiet_hour_toronto)], [fmtInt(r.quiet_avg_players), true],
    ])
  );
}

function renderShare(data) {
  const rows = (data.market_share || [])
    .filter((r) => r.publisher_group === "Take-Two")
    .sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
  new Chart(document.getElementById("share-chart"), {
    type: "line",
    data: {
      labels: rows.map((r) => r.snapshot_date),
      datasets: [
        { label: "Take-Two share", data: rows.map((r) => r.share_pct), ...LINE_STYLE(css("--series-1")), pointRadius: rows.length < 3 ? 4 : 0 },
      ],
    },
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false }, // one series: the card title names it
        tooltip: { callbacks: { label: (ctx) => ` ${ctx.raw}% of players in tracked games` } },
      },
      scales: {
        x: { grid: { display: false }, ticks: { color: css("--text-muted"), maxTicksLimit: 8 } },
        y: AXIS({ min: 0, max: 100, ticks: { color: css("--text-muted"), callback: (v) => `${v}%` } }),
      },
    },
  });
  fillTable(
    document.getElementById("share-table"),
    [["Date"], ["Group"], ["Avg players", true], ["Share", true]],
    (data.market_share || []).slice(0, 30).map((r) => [
      [r.snapshot_date], [r.publisher_group], [fmtInt(r.avg_players), true], [`${r.share_pct}%`, true],
    ])
  );
}

function renderAnomalies(data) {
  const list = document.getElementById("anomaly-list");
  const rows = data.anomalies || [];
  if (!rows.length) {
    list.replaceChildren(
      el("li", {}, el("span", { class: "status ok" }, "All normal"),
        "No unusual player counts so far. Detection starts once a game has about 12 hours of history.")
    );
    return;
  }
  list.replaceChildren(
    ...rows.map((a) => {
      const pct = Math.abs(a.pct_vs_baseline).toFixed(0);
      const when = new Date(a.captured_at).toLocaleString("en-CA", { dateStyle: "medium", timeStyle: "short", timeZone: "America/Toronto" });
      const spike = a.direction === "spike";
      return el("li", {},
        el("span", { class: "status" }, spike ? "Spike" : "Drop"),
        el("strong", {}, a.name),
        ` ${fmtInt(a.player_count)} players at ${when}: ${pct}% ${spike ? "above" : "below"} normal (${fmtInt(a.baseline_avg)}).`);
    })
  );
}

function renderHealth(data) {
  const runs = (data.recent_runs || []).slice().reverse(); // oldest first
  const count = (s) => runs.filter((r) => r.status === s).length;
  const last = runs[runs.length - 1];
  const tiles = [
    ["Last run", last ? new Date(last.finished_at).toLocaleTimeString("en-CA", { hour: "numeric", minute: "2-digit" }) : "–"],
    ["Successful", runs.length ? `${count("success")} / ${runs.length}` : "–"],
    ["Warnings", count("warning")],
    ["Stopped by a check", count("failed")],
  ];
  document.getElementById("health-tiles").replaceChildren(
    ...tiles.map(([l, v]) => el("div", {}, el("div", { class: "label" }, l), el("div", { class: "value" }, v)))
  );
  document.getElementById("run-strip").replaceChildren(
    ...runs.map((r) => el("span", { class: r.status, title: `${new Date(r.finished_at).toLocaleString("en-CA")}: ${r.status}, ${r.rows_loaded} rows` }))
  );
  document.getElementById("run-strip-note").textContent = runs.length
    ? `Last ${runs.length} runs, oldest first. Green = success, amber = loaded with warnings, red = stopped by a quality check. Hover a bar for details.`
    : "";
}

function renderSales(data) {
  const table = document.getElementById("sale-table");
  const rows = data.sale_impact || [];
  if (!rows.length) return emptyRow(table, 6, "No sales seen yet. This fills in once a tracked game goes on sale.");
  fillTable(
    table,
    [["Game"], ["Sale days", true], ["Max discount", true], ["Players on sale", true], ["Players full price", true], ["Difference", true]],
    rows.map((r) => [
      [r.name], [r.sale_days, true], [`${r.max_discount}%`, true], [fmtInt(r.avg_players_on_sale), true],
      [fmtInt(r.avg_players_full_price), true],
      [r.sale_uplift_pct == null ? "–" : `${r.sale_uplift_pct > 0 ? "+" : ""}${r.sale_uplift_pct}%`, true],
    ])
  );
}

function renderTwitch(data) {
  const table = document.getElementById("twitch-table");
  const rows = data.twitch_vs_players || [];
  if (!rows.length) return emptyRow(table, 5, "Twitch isn't connected yet (add TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET).");
  fillTable(
    table,
    [["Game"], ["Avg viewers", true], ["Avg players", true], ["Viewers per player", true], ["Correlation", true]],
    rows.map((r) => [
      [r.name], [fmtInt(r.avg_viewers), true], [fmtInt(r.avg_players), true],
      [r.viewers_per_player ?? "–", true], [r.correlation ?? "–", true],
    ])
  );
}

function renderLaunchWatch(data) {
  const rows = data.launch_watch || [];
  const table = document.getElementById("launch-table");
  const gta6 = (data.twitch_daily || []).filter((r) => r.app_id === -6).sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
  const latest = gta6[gta6.length - 1];
  const peak = gta6.reduce((m, r) => (r.max_viewers > (m?.max_viewers ?? -1) ? r : m), null);
  const gtaV = rows.find((r) => r.app_id === 3240220 && r.change_vs_pre_launch_pct != null);
  const daysToLaunch = Math.ceil((Date.parse("2026-11-19T00:00:00-05:00") - Date.now()) / 86400000);
  const tiles = [
    ["GTA VI launch", daysToLaunch > 0 ? `in ${daysToLaunch} day${daysToLaunch === 1 ? "" : "s"}` : "Out now"],
    ["GTA VI Twitch viewers (avg, latest day)", latest ? fmtInt(latest.avg_viewers) : "–"],
    ["GTA VI peak Twitch viewers", peak ? fmtInt(peak.max_viewers) : "–"],
    ["GTA V Enhanced vs pre-launch week", gtaV ? `${gtaV.change_vs_pre_launch_pct > 0 ? "+" : ""}${gtaV.change_vs_pre_launch_pct}%` : "–"],
  ];
  document.getElementById("launch-tiles").replaceChildren(
    ...tiles.map(([l, v]) => el("div", {}, el("div", { class: "label" }, l), el("div", { class: "value" }, v)))
  );
  const recent = rows.slice(0, 7 * 8);
  if (!recent.length) return emptyRow(table, 5, "Fills in as daily data comes in.");
  fillTable(
    table,
    [["Date"], ["Game"], ["Steam players", true], ["vs week before launch", true], ["GTA VI Twitch viewers", true]],
    recent.map((r) => [
      [r.snapshot_date], [r.name], [fmtInt(r.avg_players), true],
      [r.change_vs_pre_launch_pct == null ? "–" : `${r.change_vs_pre_launch_pct > 0 ? "+" : ""}${r.change_vs_pre_launch_pct}%`, true],
      [fmtInt(r.gta6_twitch_viewers), true],
    ])
  );
}

function setupFilters() {
  const buttons = [...document.querySelectorAll(".seg")];
  for (const b of buttons) {
    b.addEventListener("click", () => {
      currentGroup = b.dataset.group;
      buttons.forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      onGroupChange.forEach((fn) => fn());
    });
  }
}

async function main() {
  const meta = document.getElementById("meta");
  let data;
  try {
    const resp = await fetch(SUMMARY_URL, { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    data = await resp.json();
  } catch (err) {
    meta.textContent = `Could not load ${SUMMARY_URL} (${err.message}). Run the pipeline first, and serve this folder over HTTP.`;
    return;
  }
  meta.textContent = `Data up to ${data.run_date} · generated ${new Date(data.generated_at).toLocaleString("en-CA")}`;
  document.getElementById("sample-banner").hidden = !data.is_sample;

  GROUPS = data.groups || {};
  const byApp = groupByApp(data.daily_players);
  const colours = colourMap([...byApp.keys()]);
  renderTiles(data, byApp);
  renderPlayers(byApp, colours);
  renderRhythm(data, colours);
  renderShare(data);
  renderAnomalies(data);
  renderHealth(data);
  renderLaunchWatch(data);
  renderSales(data);
  renderTwitch(data);
  setupFilters();
  renderPrices(data.latest_prices, colours);
  renderReviews(data.latest_reviews);
}

main();
