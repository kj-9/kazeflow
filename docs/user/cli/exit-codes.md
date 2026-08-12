# Exit codes and automation

| Status | Meaning |
| --- | --- |
| `0` | Successful inspection or run, including deliberately declined confirmation. |
| `1` | A confirmed run completed with an asset failure. |
| `2` | Invalid syntax/configuration, missing non-interactive `--yes`, ambiguous selection, or unknown run ID. |
| `3` | The entry could not be loaded or resolved. |
| `4` | Execution infrastructure, requested TUI/store adapter, or history database failed. |

## Stream separation

- A completed JSON outcome: exactly one typed document on standard output.
- Preflight, confirmation, progress, diagnostics, and ordinary user-Python stdout:
  standard error in JSON mode.
- Configuration, entry, and infrastructure errors: no successful JSON document.

An interactive declined JSON run exits `0` and emits
`kazeflow.run-declined`; a completed asset failure exits `1` and emits
`kazeflow.run-result`. Text remains a terminal-review format, not an automation
format.

An adapter failure after a terminal asset failure exits `4` and suppresses the final
result document because the explicitly requested overall operation did not complete.

## Compatibility

Documented command names, options, exit statuses, and typed JSON schemas are public
CLI interfaces. See the [JSON automation contract](json.md) for the full exit and
document matrix, field definitions, schema links, and alpha evolution policy. Human
text remains stable in meaning but can receive layout improvements.
