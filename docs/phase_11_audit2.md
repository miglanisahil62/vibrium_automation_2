# UI Audit Report — Phase 11 Workflow Editor (Re-audit)

**Audited files:**
- `/Users/sahil.m/ops_console_v2/static/workflow_editor.css`
- `/Users/sahil.m/ops_console_v2/static/workflow_editor.js`
- `/Users/sahil.m/ops_console_v2/templates/workflows_list.html`
- `/Users/sahil.m/ops_console_v2/templates/workflow_detail.html`
- `/Users/sahil.m/ops_console_v2/templates/workflow_runs.html`
- `/Users/sahil.m/ops_console_v2/templates/workflow_run_detail.html`

**Date:** 2026-05-31
**Prior verdict:** NEEDS_FIX (phase_11_audit.md)

---

## Verdict: PASS_WITH_NOTES

All P0 and P1 items from the prior audit are confirmed closed. Two new P1 items and two P2 items were found during this pass.

---

## Prior P0 — CLOSED

1. **Modal markup** — All three modals (`#create-modal`, `#wf-activate-modal`, `#wf-repair-modal`) now use `.modal-overlay > .modal-card > .modal-hd / .modal-body / .modal-footer`. Open state toggled via `.classList.add('open')`. Confirmed against `features.css:247–272`. Closed.

2. **`var(--bg)` token** — No occurrences of `var(--bg)` remain in `workflow_editor.css`. All references are now `var(--bg-primary)` or `var(--bg-elevated)`. Confirmed by grep. Closed.

---

## Prior P1 — CLOSED

1. **`var(--font-mono, ...)`** — No occurrences remain. All monospace references in `workflow_editor.css` use `var(--mono)` (lines 63, 128, 159, 216, 222). Closed.

2. **`class="text-input"`** — No occurrences remain in any template. All form inputs in templates use `class="field"`. Closed.

3. **Phase 0a JSON-fallback bypass** — `_graphContainsForbiddenSetCtProp()` is defined at `workflow_editor.js:274–282` and called at the top of `saveVersion()` at `workflow_editor.js:286–289`. The guard runs before the `fetch` call, blocking any save that embeds `coll_bot_calling` in a `SET_CT_PROP` node's config — regardless of whether the canvas or JSON-fallback mode was used to edit. Closed.

4. **`var(--blue, #007aff)` on badge-run-active** — Replaced with `var(--indigo)` in all three style blocks: `workflow_runs.html:93`, `workflow_run_detail.html:164`. Confirmed by grep — no `var(--blue` remaining. Closed.

---

## P1 — should fix

### P1-A: Fallback hex values in `var(--green)` and `var(--red)` toast rules

`workflow_editor.css:272–277`:
```css
.wf-toast-ok    { border-left: 3px solid var(--green, #34c759); }
.wf-toast-error { border-left: 3px solid var(--red, #ff3b30);
                  color: var(--red, #ff3b30); }
```

Both tokens are unconditionally defined in `console.css` for both light and dark themes. The hex fallbacks `#34c759` and `#ff3b30` are therefore never needed and constitute hardcoded color values — the same category of issue as the prior P0 `var(--bg)` fallback. Strip the fallbacks:

```css
.wf-toast-ok    { border-left: 3px solid var(--green); }
.wf-toast-error { border-left: 3px solid var(--red); color: var(--red); }
```

### P1-B: Hand-rolled local toast instead of `window.showToast()`

The design system mandates (forbidden patterns section): "Toast — Managed by `window.showToast(kind, title, msg)` — never hand-roll."

`workflow_editor.css:255–283` defines `.wf-toast`, `.wf-toast-ok`, `.wf-toast-error`, and `@keyframes wf-toast-in`. `workflow_editor.js:393–401` provides its own `toast()` implementation that toggles `display:block` / `display:none` on a `#wf-toast` element stamped in `workflow_detail.html:97`.

This duplicates the system toast entirely. The hand-rolled version also does not respect `--ease-smooth` / `--ease-spring` on its animation (see P2-A below) and has no `prefers-reduced-motion` override of its own (the global rule in `console.css:1483–1490` catches `animation-duration` and `transition-duration` but the `display:none → block` toggle is not a CSS transition at all, so reduced-motion users get no visible signal change).

Switch to `window.showToast('ok' | 'error', title, msg)`. Remove the `.wf-toast*` CSS block, the `#wf-toast` element, and the `toast()` function; replace all five `toast(...)` call sites in `workflow_editor.js` with `window.showToast(...)`.

---

## P2 — nice to fix

### P2-A: Raw `ease` and `ease-out` keywords instead of design token variables

`workflow_editor.css` uses bare CSS easing keywords at several points instead of the `--ease-*` token variables. The design system specifies `var(--ease-spring)`, `var(--ease-smooth)`, `var(--ease-out)` for all transitions/animations so that the global `prefers-reduced-motion` override — which sets `transition-duration: 0.001ms !important` on `*, *::before, *::after` — catches them by property, not by name.

Affected lines:
- `workflow_editor.css:124` — `transition: border-color 120ms ease, box-shadow 120ms ease;` (`.wf-cfg-input, .wf-cfg-textarea`)
- `workflow_editor.css:160` — `transition: border-color 120ms ease, color 120ms ease, background 120ms ease;` (`.wf-palette-btn`)
- `workflow_editor.css:187` — `transition: border-color 120ms ease, box-shadow 120ms ease;` (`.drawflow-node`)
- `workflow_editor.css:230` — `transition: stroke 120ms ease;` (`.main-path`)
- `workflow_editor.css:268` — `animation: wf-toast-in 180ms ease-out;` (`.wf-toast` — moot once P1-B is fixed)

Replace `ease` with `var(--ease-smooth)` and `ease-out` with `var(--ease-out)` at each site. Duration 120 ms is within the allowed 150–320 ms window for short micro-transitions; leaving it as-is is acceptable.

### P2-B: Config-pane inputs use bespoke `.wf-cfg-input` / `.wf-cfg-textarea` instead of `.field`

`workflow_editor.js:175–185` injects `class="wf-cfg-input"` and `class="wf-cfg-textarea"` for the dynamically rendered node config form. The canonical design system class for form inputs is `.field` (`console.css:480–492`). `.wf-cfg-input` is visually close — same padding, same focus ring — but differs in two ways: it adds a `1px solid var(--separator)` border (`.field` is borderless, tile-backed) and uses `var(--bg-primary)` background instead of `var(--bg-tile)`.

The deviation is intentional (the config pane sits on `var(--bg-elevated)`, so a tile background on the input would be nearly invisible), but it means the config-pane inputs are not theme-swappable through the `.field` token path. Consider whether a modifier class `class="field field-bordered"` would serve better, or document the exception. As-is this is a P2 — visually coherent, but not using the canonical component.

### P2-C: Emoji characters used as empty-state icons

`workflows_list.html:65` — `⊟` (U+22DF) rendered at `font-size:48px` as an empty-state graphic.
`workflow_runs.html:73` — `∅` (U+2205) rendered at `font-size:48px` as an empty-state graphic.

The design system forbids emoji icons and requires inline SVG. Mathematical symbols at display size are effectively emoji-style icon usage — they are not theme-aware, do not respect `--label-3` coloring natively (they inherit it via `color:inherit` but vary across OS/browser rendering), and scale differently on different platforms. Replace with a simple inline SVG placeholder (e.g., a 48×48 outlined box or circle from the Feather icon set at `stroke-width:1.5` and `color:var(--label-3)`).

---

## What I checked

- All prior P0 and P1 closures (confirmed closed)
- Color tokens — no raw hex outside `var()` wrappers (except fallbacks in P1-A)
- Component class reuse — modal pattern, `.field`, `.btn`, `.btn-danger`
- Forbidden patterns — hand-rolled toast (P1-B), emoji icons (P2-C), `<div onclick>` (none found in templates — only `<tr onclick>` in `workflow_runs.html:50`, which is a `<tr>` not a `<div>` and has a companion `tabindex + onkeydown` for keyboard parity; borderline but acceptable)
- Icon stroke widths — no new inline SVGs in this diff; palette/toolbar use text labels
- Theme + accent variable awareness — all new color references use tokens
- Animation easing tokens (P2-A)
- Accessibility basics — modals have `role="dialog"`, `aria-modal="true"`, `aria-labelledby`; ESC handling present in all three modal hosts; `.field` focus rings intact; repair-modal focus trap set via `setTimeout` focus on first input

## What I did NOT check

- Backend wiring (out of scope)
- Drawflow library internals (third-party, not subject to design system rules)
- Cross-page navigation flow
- JS interaction correctness (covered by stashfin-qa-ui)
