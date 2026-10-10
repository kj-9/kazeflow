## Why

Coding agents and developers need a reliable way to check the flow-definition and CLI rules of the kazeflow version they have installed. The hosted documentation is authoritative, but an installed package currently cannot explain these rules offline before an agent writes a flow. This Issue #3 change complements the planned M18 documentation checks by making a concise, version-matched guide available from the CLI.

## What Changes

- Add `kazeflow instructions`, an entry-free command that prints bundled Markdown to stdout.
- Include concise guidance for discoverable asset definitions, explicit flows and factories, Python import effects, partition selection, the required environment, and non-executing validation commands.
- Make the command discoverable in CLI help, the README, and hosted CLI reference; retain hosted documentation as the authoritative detailed guide.
- Verify the command in a core-only installed wheel and check that its advertised commands and options remain valid.

Scope is the single Markdown command in Issue #3. Agent-specific installation, automatic instruction injection, machine-readable variants, static analysis, and sandboxing are out of scope.

## Capabilities

### New Capabilities

- `cli-development-instructions`: An offline, version-matched, side-effect-free Markdown guide retrievable from the public CLI.

### Modified Capabilities

None. Existing flow loading, plan/run, JSON, and documentation contracts remain intact.

## Impact

The public `kazeflow` command gains one additive subcommand. The wheel gains a small Markdown package resource; CLI dispatch, tests, wheel smoke checks, README, and hosted CLI documentation change. Core execution and Python APIs do not change. The command uses only the Python standard library and adds no runtime dependency.
