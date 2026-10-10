## 1. Bundled guide and CLI

- [x] 1.1 Add the concise version-matched Markdown resource under `src/kazeflow/`; verify it covers discovery, explicit Flow/factory, import effects, partition selection, and existing inspection commands.
- [x] 1.2 Add `kazeflow instructions` dispatch in `src/kazeflow/cli.py`; verify an entry-free invocation prints the resource and invalid arguments exit `2` without stdout.

## 2. Documentation and behavior evidence

- [x] 2.1 Add one-line discovery to README and hosted CLI reference/nav; verify the strict docs build exposes a command page and local links resolve.
- [x] 2.2 Add focused CLI tests for stdout, no entry load, help, unsupported arguments, and guide/option drift; verify the focused test module passes.

## 3. Installed package and release checks

- [x] 3.1 Extend the core-only installed-wheel smoke test to run `instructions` and verify its bundled output; verify wheel metadata remains dependency-free.
- [x] 3.2 Run `make test`, `make ci-check`, `make docs-check`, `make package-check`, `openspec doctor`, and `openspec validate --all --strict`; verify all succeed and check the final diff for unrelated edits.
