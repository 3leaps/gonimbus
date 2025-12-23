# Gonimbus Provenance

Gonimbus was bootstrapped using the Fulmen CDRL (Clone → Degit → Refit → Launch) workflow.

## Source

- **Template**: [forge-workhorse-groningen](https://github.com/fulmenhq/forge-workhorse-groningen) v0.1.19
- **Process**: [Fulmen Template CDRL Standard](https://github.com/fulmenhq/crucible/blob/main/docs/architecture/fulmen-template-cdrl-standard.md)
- **Date**: December 2025

## Ecosystem

Gonimbus uses infrastructure from the [Fulmen ecosystem](https://github.com/fulmenhq):

- **gofulmen** - Go helper library (config, logging, schema validation)
- **Crucible** - SSOT schemas and standards (via gofulmen shim)

The Fulmen ecosystem provides invisible infrastructure; developers interact with gonimbus directly.

## Ownership

Gonimbus is a [3 Leaps](https://3leaps.net) product (`github.com/3leaps/gonimbus`).
