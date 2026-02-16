---
name: run_unit_tests
description: Run unit tests for the project using pytest.
---

# Unit Testing Skill

This skill provides instructions on how to run and add unit tests in the `doug-dashboard` project.

## Running Tests

To run all unit tests, execute the following command from the project root:

```bash
uv run python -m pytest src/tests
```

To run a specific test file:

```bash
uv run python -m pytest src/tests/path/to/test_file.py
```

## Creating New Tests

When adding new functionality, you must create unit tests to verify it.

### 1. Directory Structure & Naming
Mirror the `src/` directory structure within `src/tests/`.
- Source: `src/assets/work/github/github.py`
- Test: `src/tests/assets/work/github/test_github.py`

### 2. Test Data Management (Fixtures)
Do **not** hardcode large data structures in test functions. Use `pytest` fixtures.
- **Global Fixtures** (`src/tests/conftest.py`): Place mocks for shared resources (e.g., `dagster.AssetExecutionContext`, `s3fs`).
- **Domain Fixtures** (`src/tests/.../conftest.py`): Place dataset-specific fixtures (e.g., sample GitHub events, Spotify API responses) in a `conftest.py` within the closest relevant subdirectory.

### 3. Mocking External Dependencies
Use `unittest.mock` to mock external API calls and resources.
- Avoid making real network requests during tests.
- Mock Dagster resources (`GithubResource`, `SpotifyResource`) to return the fixture data.

### 4. Example Pattern
```python
def test_process_event(mock_resource, mock_context, sample_data):
    # Setup
    mock_resource.get_item.return_value = sample_data
    
    # Execution
    result = process_event(mock_resource, mock_context)
    
    # Verification
    assert result["status"] == "success"
    mock_context.log.info.assert_called()
```
