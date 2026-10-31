# hvat_backend Examples

These examples use the REST-only backend helper APIs. The current product
runtime is `hvat_backend` in `full_sam` mode.

## `project_state_downloads_backend`

Shows a lightweight backend with project-state persistence and image downloads,
without inference:

- `GET /api/info` and `GET /api/images`
- `GET/PUT /api/project-state`
- `GET /api/images/download`
- `GET /api/images/{id}/meta`, `/thumbnail`, and `/raw`

Run:

```bash
cargo run -p hvat_backend --example project_state_downloads_backend -- ./data
```
