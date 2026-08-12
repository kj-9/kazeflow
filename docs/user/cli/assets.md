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
terminal review; JSON writes one typed document for automation. See the
[JSON automation contract](json.md#version-1-data-fields) for its field shape and
schema.

Loading the entry still executes Python import-time code. Load only trusted source.
