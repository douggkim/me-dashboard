---
name: linting
description: Format code and check for linting errors using Ruff.
---

# Linting Skill

This skill provides instructions on how to format code and check for linting errors using `ruff`.

## Running Linting and Formatting

To check for linting errors and format code, execute the following commands from the project root:

```bash
# Check for linting errors
uv run ruff check .

# Format code
uv run ruff format .
```

To fix fixable linting errors automatically:

```bash
uv run ruff check --fix .
```

## Handling Linting Errors

If `ruff` reports errors that are irrelevant or "false positives" for a specific context, you have two options:

### 1. Ignore for a specific line
Add a `# noqa: <RULE_CODE>` comment to the end of the line, **followed by a reason**.

**Requirement**: You must safeguard against suppressing valid errors. If you are unsure, ask the user.

```python
x = 1  # noqa: F841  # Variable is required for side effects
```

### 2. Ignore globally (Update Configuration)
If a rule is generally not applicable to the project, add it to the `extend-ignore` list in `ruff.toml`.

**CRITICAL REQUIREMENT**: You **MUST** ask for user validation and approval before globally ignoring a rule. Explain why the rule is not relevant to the project context.

```toml
[lint]
extend-ignore = [
    # ... existing ignores ...
    "RULE_CODE",  # Reason for ignoring
]
```

## Configuration
The `ruff` configuration is located in `ruff.toml`. It is currently configured to:
-   Select ALL rules.
-   Use 120 character line length.
-   Follow NumPy docstring convention.
-   Ignore specific rules listed in `extend-ignore`.
