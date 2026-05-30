# UI Audit Report — Phase 11 re-audit #3 (toast P1 fixes)

Audited files:
- `/Users/sahil.m/ops_console_v2/static/workflow_editor.css`
- `/Users/sahil.m/ops_console_v2/static/workflow_editor.js`
- `/Users/sahil.m/ops_console_v2/templates/workflow_detail.html`

Prior audit: PASS_WITH_NOTES (phase_11_audit2.md) — two P1s outstanding (P1-A hand-rolled toast CSS, P1-B local toast() function + #wf-toast DOM element).

---

## Verdict: PASS_WITH_NOTES

All P0s and both P1s from the prior audit are confirmed closed. Two pre-existing P2s survive (carried over, unchanged). No new P0 or P1 introduced by the fix.

---

## P0 — must fix

None.

---

## P1 — should fix

None.

### Confirmed closed from prior audit

**P1-A (CLOSED):** `.wf-toast`, `.wf-toast-ok`, `.wf-toast-error`, and `@keyframes wf-toast-in` are fully absent from `workflow_editor.css`. No residual hex fallbacks `var(--green, #34c759)` or `var(--red, #ff3b30)` survive anywhere in the file. Grep clean.

**P1-B (CLOSED):** The local `toast()` function and `toastTimer` variable have been fully removed from `workflow_editor.js`. All 14 call-sites now use `window.showToast(kind, title, msg)` with kind mapped correctly (`'error'` → `'err'`). The `#wf-toast` DOM element is absent from `workflow_detail.html`. Three-file grep returns zero hits for `wf-toast`, `toastTimer`, `function toast`, `toast(msg`.

---

## P2 — nice to fix

**[workflow_editor.css:186, 194, 200] Shadow fallback values do not match the token definitions.**

The `var(--shadow-sm, …)` and `var(--shadow-md, …)` fallback literals in the Drawflow node overrides are stale approximations:

| Line | Written fallback | Canonical token (light) |
|---|---|---|
| 186 | `0 1px 2px rgba(0,0,0,0.06)` | `0 1px 2px rgba(0,0,0,0.04), 0 0 0 0.5px rgba(0,0,0,0.04)` |
| 194, 200 | `0 4px 12px rgba(0,0,0,0.08)` | `0 4px 12px rgba(0,0,0,0.06), 0 0 0 0.5px rgba(0,0,0,0.04)` |

The fallback fires only if `--shadow-sm`/`--shadow-md` are undefined (i.e., console.css is not loaded), so this is cosmetically inert in production. However the fallback alphas are slightly too heavy and the token's signature hairline ring (`0 0 0 0.5px`) is dropped. The cleanest fix is to omit the fallback entirely since console.css is always present: `box-shadow: var(--shadow-sm)`.

**[workflow_editor.js:190] `btn-sm` class is not part of the design system; inline style used for button gap.**

`workflow_editor.js:190` emits:
```
<button class="btn btn-danger btn-sm" style="margin-left:6px;" ...>Delete node</button>
```

Two sub-issues:
1. `.btn-sm` has no definition in `console.css`, `features.css`, or `wave2.css`. The class is silently ignored today. If sizing is needed, the canonical approach is to set `padding` on the element or add a scoped `.wf-cfg-btn` rule in `workflow_editor.css`.
2. `margin-left:6px` is off the 8 pt grid (should be `4px` or `8px`). Move the spacing into a CSS class to keep it on-grid.

Note: `.btn-danger` itself is canonical (defined at `console.css:538`), so the button's visual treatment is correct. Only the extra modifier class and the inline gap margin need addressing.

---

## What I checked

- P1-A toast CSS block removal — confirmed absent, zero grep hits
- P1-B local toast() JS function and #wf-toast DOM element removal — confirmed absent, zero grep hits
- All `window.showToast(kind, title, msg)` call-sites — correct kind mapping, correct argument order
- Raw hex / non-token colors in all three files
- Inline style attributes in `workflow_detail.html` — all use `var(--…)` tokens or layout-only properties (display, width, margin); no bare hex
- `btn-danger`, `btn-sm`, `btn-secondary`, `btn-primary` class validity against design system
- Modal overlay pattern — `#wf-activate-modal` uses `.modal-overlay > .modal-card > .modal-hd + .modal-body`, matching the canonical pattern in `base.html`; `.modal-footer` and `.modal-close` are defined in the page `<style>` block using only tokens (no hex)
- `.badge` + variant classes (`.badge-active`, `.badge-paused`, `.badge-draft`, `.badge-archived`) — defined in the page `<style>` block using only `color-mix()` with tokens; no hex
- `.divider` in `page-sub` — canonical (defined in `console.css:475`)
- Shadow token fallback accuracy
- `onclick` on interactive elements — all `onclick` attributes are on `<button>` elements; the `onclick` on `div.modal-overlay` at `workflow_detail.html:69` matches the established convention in `base.html` (backdrop-dismiss pattern; not a forbidden div-onclick case)
- Icon stroke widths — no new SVG icons introduced in this diff
- `prefers-reduced-motion` — no new animations introduced
- `wf-canvas-wrap card` and `wf-config-pane card` use canonical `.card` class

## What I did NOT check

- Backend wiring (out of scope)
- JS interactions beyond toast call-site correctness (covered by stashfin-qa-ui)
- Cross-page navigation flow
- Drawflow library internals
