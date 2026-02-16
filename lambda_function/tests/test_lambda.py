"""
This module tests the Lambda handler function.
"""

from unittest.mock import patch
import importlib.util
import sys
import os

# Import the lambda module using importlib since 'lambda' is reserved keyword
# Construct path relative to this test file
test_dir = os.path.dirname(os.path.abspath(__file__))
lambda_path = os.path.join(test_dir, "..", "src", "lambda.py")

spec = importlib.util.spec_from_file_location(
    "lambda_module", os.path.abspath(lambda_path)
)
lambda_module = importlib.util.module_from_spec(spec)
sys.modules["lambda_module"] = lambda_module
spec.loader.exec_module(lambda_module)


class TestLambdaHandler:
    """Tests for the Lambda handler function."""

    @patch("lambda_module.executor.handle_event")
    def test_handler_success(self, mock_handle_event):
        """Test handler function successfully proxies to handle_event."""
        # Setup
        mock_handle_event.return_value = {
            "statusCode": 200,
            "body": '{"message": "Success"}',
        }
        event = {"test": "data"}
        context = {"test": "context"}

        # Execute
        response = lambda_module.handler(event, context)

        # Assert
        assert response["statusCode"] == 200
        mock_handle_event.assert_called_once_with(event, context)

    @patch("lambda_module.executor.handle_event")
    def test_handler_error(self, mock_handle_event):
        """Test handler function handles errors from handle_event."""
        # Setup
        mock_handle_event.return_value = {
            "statusCode": 500,
            "body": '{"error": "Test error"}',
        }
        event = {"test": "data"}
        context = {"test": "context"}

        # Execute
        response = lambda_module.handler(event, context)

        # Assert
        assert response["statusCode"] == 500
        mock_handle_event.assert_called_once_with(event, context)
