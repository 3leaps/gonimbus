# Vendored agentic schemas

`role-prompt.schema.json` is vendored from
[3leaps/crucible](https://github.com/3leaps/crucible/tree/main/schemas/agentic/v0),
which publishes it at the `$schema` URL the role files declare
(`https://schemas.3leaps.dev/agentic/v0/role-prompt.schema.json`).

It is vendored so `make validate-roles` runs hermetically — validation must not
depend on network reachability, and a validator that silently skips when a
remote fetch fails would be a gate that cannot fail.

Note the split lineage: the **role files** in `config/agentic/roles/` are an
intentional fork of [fulmenhq/crucible](https://github.com/fulmenhq/crucible/tree/main/config/agentic/roles)
(see that directory's README), while the **schema** they validate against is
3leaps'. Refresh this copy deliberately, not on a schedule.
