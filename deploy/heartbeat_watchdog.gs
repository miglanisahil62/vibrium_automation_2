/**
 * VB Collections — EXTERNAL dead-man's-switch (off-box).
 *
 * Add this as a SEPARATE script file in the SAME Apps Script project that owns
 * the ops heartbeat sheet (the one at OPS_WEBHOOK_URL). It does NOT touch Code.gs
 * or SHARED_SECRET — so there is no risk of overwriting the deployed secret.
 *
 * Why this exists: the on-box email watchdog (workflow/alerts.py) covers every
 * failure EXCEPT total server/cron death — a dead box can't email itself. This
 * runs on Google's infrastructure on a time trigger, reads the heartbeats the
 * AWS jobs push to the sheet, and alerts if those heartbeats go STALE (box dead)
 * or report status=down (pipeline broken). It uses SHEET_ID, a global const
 * already defined in Code.gs (Apps Script shares globals across .gs files).
 *
 * ONE-TIME SETUP (in the Apps Script editor, ~3 clicks):
 *   1. Files → + → Script → name it "heartbeat_watchdog" → paste this whole file.
 *   2. Select the function "setupVibriumWatchdogTrigger" in the toolbar dropdown.
 *   3. Click Run → authorize when prompted (grants Mail + Script-trigger scopes).
 *   That installs a 30-min time trigger and sends a one-time confirmation email.
 * No web-app redeployment is needed (the trigger runs AS your account, not via
 * the web app), so the SHARED_SECRET / oauthScopes-propagation gotchas do NOT
 * apply here.
 */

// --- config ---------------------------------------------------------------
var VIB_WD_RECIPIENTS = "sahil.miglani@stashfin.com,ishita.goyal@stashfin.com";
// Jobs that MUST heartbeat during the call window. If any goes stale, alert.
// wf_pipeline_health is the consolidated health signal (down => P0 active).
var VIB_WD_CRITICAL = ["wf_executor", "wf_scheduler", "wf_ingest", "wf_pipeline_health"];
var VIB_WD_STALE_MIN = 90;          // stale if latest heartbeat older than this (min)
var VIB_WD_WINDOW_START = 8;        // IST hour: only check during the active window
var VIB_WD_WINDOW_END = 20;        // (jobs run 08:00-19:xx; give a margin)
var VIB_WD_COOLDOWN_MS = 2 * 3600 * 1000;   // don't re-email the same problem within 2h
var VIB_WD_TAB = "heartbeats";

// --- the watchdog ---------------------------------------------------------
function checkVibriumHeartbeats() {
  var now = new Date();
  var ist = Utilities.formatDate(now, "Asia/Kolkata", "yyyy-MM-dd HH:mm:ss");
  var hour = parseInt(ist.substring(11, 13), 10);
  if (hour < VIB_WD_WINDOW_START || hour >= VIB_WD_WINDOW_END) return; // off-window

  var sh = SpreadsheetApp.openById(SHEET_ID).getSheetByName(VIB_WD_TAB);
  if (!sh) {
    _vibAlert("heartbeats tab missing", ist, ["The '" + VIB_WD_TAB +
      "' tab does not exist — heartbeats are not landing. Pipeline visibility lost."]);
    return;
  }
  var lastRow = sh.getLastRow();
  if (lastRow < 1) return;
  // Read only the tail (heartbeats are append-only) for efficiency.
  var startRow = Math.max(1, lastRow - 800);
  var data = sh.getRange(startRow, 1, lastRow - startRow + 1, 4).getValues(); // ts, job, status, summary

  var latest = {}; // job_id -> {ts, status, summary}
  for (var i = 0; i < data.length; i++) {
    var ts = data[i][0], job = data[i][1], status = data[i][2], summary = data[i][3];
    if (!job) continue;
    var tsStr = (ts instanceof Date)
      ? Utilities.formatDate(ts, "Asia/Kolkata", "yyyy-MM-dd HH:mm:ss")
      : String(ts);
    if (!latest[job] || tsStr > latest[job].ts) {
      latest[job] = { ts: tsStr, status: String(status), summary: String(summary) };
    }
  }

  var problems = [];
  var nowMs = now.getTime();
  for (var j = 0; j < VIB_WD_CRITICAL.length; j++) {
    var jobId = VIB_WD_CRITICAL[j];
    var e = latest[jobId];
    if (!e) { problems.push(jobId + ": NEVER SEEN (no heartbeat at all)"); continue; }
    var tsMs = new Date(e.ts.replace(" ", "T") + "+05:30").getTime();
    var ageMin = Math.round((nowMs - tsMs) / 60000);
    if (ageMin > VIB_WD_STALE_MIN) {
      problems.push(jobId + ": STALE " + ageMin + "m (last seen " + e.ts + " IST)");
    }
    if (e.status.toLowerCase() === "down") {
      problems.push(jobId + ": status=DOWN — " + e.summary);
    }
  }
  if (!problems.length) return;

  // Cooldown so a persisting problem doesn't email every 30 min.
  var props = PropertiesService.getScriptProperties();
  var last = Number(props.getProperty("vib_wd_last_alert") || 0);
  if (nowMs - last < VIB_WD_COOLDOWN_MS) return;

  var allStale = problems.length >= VIB_WD_CRITICAL.length;
  _vibAlert(allStale ? "ALL jobs silent — SERVER LIKELY DOWN" : "pipeline heartbeat problem",
            ist, problems);
  props.setProperty("vib_wd_last_alert", String(nowMs));
}

function _vibAlert(headline, ist, problems) {
  var body = "VB Collections external watchdog (Google-side) detected a problem.\n\n"
    + headline + "\n\n"
    + problems.join("\n") + "\n\n"
    + "If ALL critical jobs are stale, the AWS instance or its cron is down — "
    + "the on-box email watchdog can't fire in that case, which is why this "
    + "external check exists. Check the EC2 instance + crontab.\n\n"
    + "Checked: " + ist + " IST.";
  MailApp.sendEmail(VIB_WD_RECIPIENTS,
    "[VB Watchdog] " + headline + " — " + ist.substring(0, 10), body);
}

// --- one-time setup -------------------------------------------------------
function setupVibriumWatchdogTrigger() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === "checkVibriumHeartbeats") ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger("checkVibriumHeartbeats").timeBased().everyMinutes(30).create();
  MailApp.sendEmail(VIB_WD_RECIPIENTS,
    "[VB Watchdog] external dead-man's-switch ARMED",
    "The Google-side heartbeat watchdog is now installed (checks every 30 min, "
    + "08:00-20:00 IST). It will email you if the VB Collections pipeline "
    + "heartbeats go stale (server down) or report a problem.\n\n"
    + "Monitored jobs: " + VIB_WD_CRITICAL.join(", ") + "\n"
    + "Stale threshold: " + VIB_WD_STALE_MIN + " min.");
}
