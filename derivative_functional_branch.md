# Derivative Functional Branch Plan

## Goal

Keep the current open PR (`split-functional-arch-interp-spec-bench`) stable while continuing new work on derivative-related functionals in a separate branch.

This branch (`derivative-functional-branch`) is the working branch for:

- New derivative functionals
- Related unit tests
- Related docs/benchmarks for those new derivative functionals

## Branch Strategy

1. Treat `split-functional-arch-interp-spec-bench` as mostly frozen.
2. Do new derivative-functional development only on `derivative-functional-branch`.
3. Open a separate PR from `derivative-functional-branch`.
4. If needed, stack that PR on top of `split-functional-arch-interp-spec-bench`.
5. After the original PR merges, rebase this branch onto `main` and retarget PR base to `main`.

## Daily Workflow

From this repository root:

```bash
git switch derivative-functional-branch
git fetch origin
git rebase origin/split-functional-arch-interp-spec-bench
```

Do work, then:

```bash
git add <files>
git commit -m "Add <derivative-functional-name> functional"
git push -u origin derivative-functional-branch
```

## After Base PR Merges

When `split-functional-arch-interp-spec-bench` merges to `main`:

```bash
git fetch origin
git switch derivative-functional-branch
git rebase origin/main
git push --force-with-lease
```

Then update PR base to `main` (if it was stacked).

## Scope Guidance For This Branch

Recommended to include:

- Functional implementation (torch + warp where applicable)
- `FunctionSpec` integration (`make_inputs_forward`, and backward hooks only when meaningful)
- Unit tests following current functional test structure
- Docs API entry and benchmark registration updates

Avoid in this branch:

- Unrelated refactors
- Broad cleanup not tied to derivative-functional scope

## Notes

- Keep commits focused by functional (one functional per commit when possible).
- If a functional is non-differentiable, do not add backward benchmark hooks.
- Prefer explicit implementation over extra helper abstraction unless reuse is clear.
