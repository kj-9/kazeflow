## Context

See `proposal.md` for motivation and the `cli-development-instructions` delta for user-visible behavior. `src/kazeflow/cli.py` dispatches through stdlib `argparse`; current commands either load a Python entry or inspect history. `docs/user/` is the authoritative detailed guide. The wheel is built from `src/kazeflow/` and has no required runtime dependency.

## Goals / Non-Goals

**Goals:** Keep the guide available before any application import, deliver the same bytes from the installed wheel, and detect drift in documented command names and options.

**Non-Goals:** Generate instructions dynamically, inject them into agents, add a JSON variant, or treat the guide as a new source of API truth.

## Decisions

1. Store one UTF-8 Markdown file inside `src/kazeflow/` and read it with `importlib.resources`. This ties its bytes to the installed package version and needs no network or new dependency. A web fetch would be stale or unavailable offline; generating prose from help text would omit the trust and discovery rules.
2. Dispatch `instructions` immediately after argument parsing and before entry validation/loading. `argparse` rejects unknown arguments with the existing exit `2`; a missing or unreadable bundled resource follows the existing infrastructure exit `4` with stderr only. No optional adapters are imported.
3. Keep the Markdown concise and link to the hosted CLI, partition, and trust references. Help and README carry one-line discovery paths; the hosted CLI reference explains the command's scope without duplicating the full guide.
4. Test command behavior without any entry, compare output to the bundled resource, assert key published command/option tokens exist in parser help, and run the command from the installed core-only wheel. A test cannot prove every future prose claim, so review of guide updates remains part of CLI contract changes.

Only CLI dispatch and its resource need runtime changes. Documentation, tests, and the wheel smoke script have separate file ownership boundaries if parallel work is later requested; `pyproject.toml` is left unchanged unless wheel verification shows the package resource is omitted.

## Risks / Trade-offs

- [Packager omits Markdown] → Inspect the built wheel and execute the installed command in the core-only smoke check.
- [Guide drifts from CLI] → Test advertised command and option names against parser help and keep detailed explanations linked to the hosted documentation.
- [Users assume safety] → State the import/factory side effects and no-sandbox boundary in the guide and public reference.

## Migration Plan

This is an additive subcommand. Existing scripts and JSON documents require no migration. Reverting the feature removes the command and resource without changing persisted data.
