# Phase 10 Close — Ops Console v2 Routes + API

**Status:** Implemented. Awaiting master-auditor + `/stashfin-qa-backend` PASS.

## What shipped

### vibrium-workflow

| File | Purpose |
|---|---|
| `workflow/validation.py` | Pure-function `validate_graph(graph_json)` → `ValidationResult`. No I/O. Re-usable by `seed_workflow.py` (Phase 12). |
| `workflow/tests/test_validation.py` | 12 tests — one per rule (valid + invalid case). Runs in <1s. |

### ops_console_v2 (edited in place; non-git repo)

| File | Change |
|---|---|
| `services_workflow.py` | New. DB wrappers around `state/workflow.db` (configurable via `WORKFLOW_DB_PATH` env). |
| `app.py` | 9 routes registered. STUB_ROUTES unchanged (no `/workflows*` was listed). |
| `templates/base.html` | Added `{{ nav('/workflows', 'Workflows', ...) }}` under the Automations section. |

## Validation rules enforced

| Code | Rule |
|---|---|
| `E_NO_ENROLL` / `E_MULTIPLE_ENROLL` | Exactly one ENROLL node. |
| `E_MISSING_EDGE` | Non-terminal nodes have all required outgoing edges populated. Required-edge table mirrors handler `execute()` return values. |
| `E_UNKNOWN_NODE_TYPE` | Node type is in `workflow_handlers.REGISTRY`. |
| `E_BAD_UUID` | Every `node_id` parses via `uuid.UUID`. |
| `E_DUPLICATE_NODE_ID` | No node_id appears twice. |
| `E_EDGE_TO_MISSING_NODE` | Every edge target is a node in the graph. |
| `E_CYCLE` | No cycles unless every cycle passes through a `WAIT_UNTIL` or `AWAIT_DISPOSITION` node (those park the run; safe to revisit). |
| `E_BAD_CONDITION_EXPR` | `CONDITION.config["expr"]` parses (simpleeval if available, else `ast.parse(mode='eval')`). |
| `E_UNKNOWN_ACTION_CLASS` | `BRANCH_ON_DISPOSITION.config["cases"]` keys ⊆ `CANONICAL_ACTION_CLASSES` (imported from `branch_on_disposition.py` — single source of truth). |
| `E_MISSING_DEFAULT` | `BRANCH_ON_DISPOSITION` has a `default` edge (matching the handler's runtime contract). |

## Routes

### Page placeholders (HTMLResponse — Phase 11 lands the UI)
- `GET /workflows`
- `GET /workflows/{id}`
- `GET /workflows/{id}/runs`
- `GET /workflows/{id}/runs/{run_id}`

### JSON API (real surface)
- `POST /api/workflows` — create draft (name, created_by)
- `POST /api/workflows/{id}/version` — save graph_json, validates via `workflow.validation.validate_graph`; rejects with `validation_errors[]` array if invalid
- `POST /api/workflows/{id}/activate` — activates a version; requires `X-Workflow-Approver` header (approval gate)
- `POST /api/workflows/{id}/preview-enrollment` — sampled projection (placeholder shape; Phase 12 wires CT props)
- `POST /api/workflows/{id}/runs/{run_id}/repair` — operator-driven advance of an ORPHANED run

## Sidebar nav

Added to `templates/base.html` under the **Automations** section, immediately after `/strategy` and before `/calendar`. SVG is a workflow-diagram glyph (3 connected nodes).

## Not in scope (Phase 11+)

- Drawflow vendoring (`static/drawflow/`).
- `templates/workflows_*.html` files.
- `workflow_editor.js` / `workflow_editor.css`.
- `/api/workflows/{id}/preview-enrollment` real CT-property sampling — currently returns a deterministic stub `{enrolled_count, sample}` shape so the frontend can wire against it.

## Dependencies confirmed

- Phase 1 schema: `workflows`, `workflow_versions`, `workflow_runs`, `workflow_admin_log`, `workflow_node_log` all read by `services_workflow.py`.
- Phase 4b `branch_on_disposition.CANONICAL_ACTION_CLASSES` imported by validation (no duplication).
- Phase 5 executor `ctx` shape — repair API constructs a minimal patch and writes via `workflow_admin_log`; full executor re-tick is deferred to operator's next natural scheduler sweep.
