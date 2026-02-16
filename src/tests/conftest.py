
import pytest
from unittest.mock import MagicMock
from dagster import AssetExecutionContext

@pytest.fixture
def mock_context():
    """
    Fixture for mocking Dagster's AssetExecutionContext.
    """
    context = MagicMock(spec=AssetExecutionContext)
    return context
