# `kazeflow assets`

List assets discovered while loading a trusted entry without invoking decorated
asset bodies.

## Synopsis

```console
kazeflow assets ENTRY [--format text|json]
```

## Examples

```console
kazeflow assets daily.py
kazeflow assets package.module:flow --format json
```

For a bare script, discovery is limited to registrations made while that script
loads. A declared module-level `flow` supplies its registry. Text output is for
terminal review; JSON writes one document for automation.

Loading the entry still executes Python import-time code. Load only trusted source.
