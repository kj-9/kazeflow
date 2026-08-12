# Exit codes and automation

| Status | Meaning |
| --- | --- |
| `0` | Successful inspection or run, including deliberately declined confirmation. |
| `1` | A confirmed run completed with an asset failure. |
| `2` | Invalid syntax/configuration, missing non-interactive `--yes`, ambiguous selection, or unknown run ID. |
| `3` | The entry could not be loaded or resolved. |
| `4` | Execution infrastructure, requested TUI/store adapter, or history database failed. |

## Stream separation

- Successful machine-readable output: standard output.
- Preflight, confirmation, progress, and diagnostics: standard error.
- Configuration, entry, and infrastructure errors: no successful JSON document.

An adapter failure after a terminal asset failure exits `4` and suppresses the final
result document because the explicitly requested overall operation did not complete.

## Compatibility

Documented command names, options, exit statuses, and JSON schemas are public CLI
interfaces. A future compatible release will publish a deprecation and migration
path before removing or renaming them. Human text remains stable in meaning but can
receive layout improvements.
