# UI Audit Report — Phase 11 workflow templates + editor (2026-05-31)

## Verdict: NEEDS_FIX

Scope: `workflows_list.html`, `workflow_detail.html`, `workflow_runs.html`,
`workflow_run_detail.html`, `static/workflow_editor.css`, `static/workflow_editor.js`.

---

## P0 — must fix

- **[workflows_list.html:79–100, workflow_detail.html:68–94, workflow_run_detail.html:110–146]
  Non-canonical modal classes.** All four modals use a hand-rolled
  `<div class="modal"> > .modal-backdrop + .modal-body + .modal-hd + .modal-close + .modal-content + .modal-footer`
  set. The canonical (and only sanctioned) pattern in `features.css:247–272` is
  `<div class="modal-overlay"> > .modal-card > .modal-hd + .modal-body`. These
  custom classes have **no CSS definitions** anywhere in `console.css`,
  `features.css`, or `wave2.css` — so they render as unstyled divs (no backdrop
  blur, no scale-in transition, no z-index, no dark-mode coverage). This is a
  forbidden-pattern violation ("Modal/toast/drawer rolled by hand — use the
  existing component or extend it via PR").
  Fix: rename to `modal-overlay` + `modal-card`, drop the unstyled
  `modal-backdrop`/`modal-content`/`modal-footer` wrappers, use `.open` to show
  instead of `style.display`.

- **[workflow_editor.css:118, :180, :241, :250] `var(--bg)` is not a defined
  token.** `console.css` defines `--bg-primary` (and `--bg-elevated`,
  `--bg-sidebar`, `--bg-tile`, etc.) but no bare `--bg`. The four uses fall
  through to the CSS initial value (transparent / inherited), so input fields,
  Drawflow nodes, and the connection-dot interior all render with no
  background in both light and dark themes.
  Fix: replace with `var(--bg-primary)` (or `var(--bg-tile)` for input
  surfaces, matching `.field` at console.css:480).

## P1 — should fix

- **[workflow_editor.css:63, :128, :159, :216, :222; workflows_list.html:48;
  workflow_runs.html:54–55; workflow_run_detail.html:40, 131, 151, 156, 162]
  `var(--font-mono, …)` token does not exist.** The canonical mono token is
  `--mono` (console.css:95). The current code always falls through to the
  hardcoded fallback string. Functional, but bypasses the token layer and
  drifts from the rest of the console.
  Fix: replace `var(--font-mono, ui-monospace, …)` with `var(--mono)`
  consistently.

- **[workflows_list.html:90,92; workflow_detail.html:83; workflow_runs.html:21;
  workflow_run_detail.html:125,130,136] `class="text-input"` is not a defined
  class.** The Ops Centre input class is `.field` (console.css:480). The
  current inputs get only browser-default borders — they will look like a
  different design language than every other v2 page.
  Fix: rename all `text-input` to `field`.

- **[workflow_editor.js:223–228] Phase 0a `coll_bot_calling` guard only fires
  via Apply button.** If the user pastes JSON into the JSON-fallback textarea
  (line 350–375) and exits JSON mode, `editor.import(parsed)` accepts a graph
  containing `coll_bot_calling` SET_CT_PROP nodes without re-running the
  guard. Defense-in-depth gap — Phase 4c runtime still catches it, but the UI
  promise is broken.
  Fix: also walk the imported graph in `toggleJsonView()` and reject if any
  SET_CT_PROP node has `coll_bot_calling` in `data.config.properties`.

- **[workflow_editor.js:404–409] ESC handler only closes activate modal.**
  Detail page now has no other modal, but `wf-toast` swallows ESC silently.
  Repair modal on the run-detail page has its own listener (line 211) — OK.
  Workflows-list create modal also has its own (line 142) — OK. So this is
  scoped correctly, but the comment on line 403 ("any open modal") is wrong.
  Fix: tighten comment, or generalize to scan `.modal[style*="block"]`.

- **[workflow_runs.html:50–53] Row click + Enter handlers bypass keyboard
  modifier semantics.** `onclick="window.location=…"` fires on cmd-click /
  middle-click too, which doesn't open in a new tab. Min-effort: also wire
  `<a class="row-link">` over the customer cell (the dedicated "Open" button
  on line 62 partially mitigates).

- **[workflow_run_detail.html:164,168; workflow_runs.html:93,97]
  `var(--blue, #007aff)` — `--blue` is not in the token list.** The canonical
  accent for "active/running" status is `var(--accent)` or `var(--indigo)`.
  Current fallback hex always wins; theme-accent picker has no effect on the
  ACTIVE badge.
  Fix: switch to `var(--indigo)` (defined in console.css token block).

## P2 — nice to fix

- **[workflows_list.html:65, workflow_runs.html:73] Emoji-like glyph in
  empty-state (`⊟`, `∅`).** Not strictly emoji, but unicode box-drawing as a
  decorative icon drifts from the SVG-only rule. Replace with a 24×24 inline
  feather-style SVG (folder / inbox).

- **[workflow_detail.html:104–105] Two `<script>` tags loaded in `{% block
  content %}` after the main content.** Works, but base templates usually
  expose a `{% block scripts %}` or load before `</body>`. Cosmetic.

- **[workflow_editor.css:268, :281] Custom keyframe `wf-toast-in` uses
  `ease-out` (180ms).** Matches the 120–180ms band but doesn't use the
  `var(--ease-spring)` / `var(--ease-smooth)` tokens, so it won't respond to
  any future tuning of those vars. Cosmetic.

- **[workflow_editor.js:189–190] Inline `onclick="window.wfEditor.…"` on
  generated buttons.** Forbidden-pattern note ("never click handlers on
  divs") technically only bars divs — buttons are fine — but addEventListener
  on the rendered nodes would be cleaner and avoids polluting `window`.

## What I checked

- Design token discipline (raw hex / rgb / rgba grep across CSS + templates)
- All four templates extend `base.html` and use `page-hd` / `page-title` /
  `page-sub`
- Sidebar `/workflows` entry exists at base.html:63 — no edit needed
- Canonical modal / input / button classes vs hand-rolled ones
- ESC handlers on every modal-bearing page
- `<label for>` on every form field
- `role="application"` + aria-label on canvas (workflow_detail.html:36–37)
- `role="region"` + aria-live on config pane (line 54)
- `role="status"` + aria-live on toast (line 97)
- `tabindex="0"` + Enter handler on run rows (workflow_runs.html:52–53)
- JSON-fallback textarea is focusable + keyboard-operable
- Phase 0a `coll_bot_calling` UI guard (editor.js:223–228) — present but
  bypassable via JSON-mode import
- `.wf-canvas-themed` scope on Drawflow overrides — correctly scoped
- Activate modal sends `X-Workflow-Approver` header (editor.js:333) — matches
  Phase 10 API
- Repair modal only renders for `run.status == 'ORPHANED'`
  (workflow_run_detail.html:25, 109)
- Responsive collapse at ≤1100px (workflow_editor.css:287–294) — present
- SOURCES.md provenance — verified file present (1254 bytes, SHA-pinned)

## What I did NOT check

- Phase 10 API contract on `/api/workflows/*/version` (out of scope)
- Drawflow.js library internals
- Cross-page navigation flow from sidebar to workflow editor
- JavaScript runtime behavior in browser (covered by stashfin-qa-ui)
- Actual rendered pixels in light + dark mode (covered by stashfin-qa-ui)
