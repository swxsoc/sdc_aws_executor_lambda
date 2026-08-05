# AGENTS.md

This file helps AI coding agents understand the repository structure, build/test conventions, and key architecture decisions for the **Space Weather SOC (SWSOC) AWS Lambda Executor Function**.

## Project Overview

This is an AWS Lambda function that implements an executor pattern to run scheduled tasks via CloudWatch Events/EventBridge rules. Each rule name maps directly to a corresponding executor function, enabling modular management of space weather data processing tasks.

The function:
- Routes incoming CloudWatch/EventBridge events to appropriate handler functions
- Executes scheduled data collection and processing tasks
- Integrates with AWS Secrets Manager for credential management
- Handles multiple satellite/instrument data sources (GOES, STIX, PADRE, UDL REACH)
- Stores processed data in Amazon Timestream and S3
- Creates Grafana annotations for solar events

See [README.rst](README.rst) for detailed architecture and implementation details.

## Essential Commands

### Testing
```bash
# Run all tests with coverage
pytest --pyargs lambda_function/tests --cov=lambda_function/src --cov-report=html

# Run a specific test file
pytest lambda_function/tests/test_executor.py -v

# Run tests matching a pattern
pytest lambda_function/tests -k "import_goes" -v
```

### Linting & Code Style
```bash
# Check code with ruff
ruff check lambda_function/src lambda_function/tests

# Format with ruff
ruff format lambda_function/src lambda_function/tests
```

### Docker & Local Lambda Testing
```bash
# Build Docker container for Lambda runtime
cd lambda_function && docker build -t sdc_aws_executor_lambda:latest .

# Run Lambda locally and test with a sample event
docker run -p 9000:8080 sdc_aws_executor_lambda:latest

# In another terminal, invoke the function with a test event
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d @lambda_function/tests/test_data/test_executor_event.json
```

### CodeBuild Deployment
```bash
# Build process defined in buildspec.yml
# Builds Docker image, pushes to ECR, prepares for Lambda deployment
```

## Project Structure

```
lambda_function/
├── src/
│   ├── lambda.py              # Handler entry point for Lambda
│   └── executor/
│       ├── __init__.py
│       ├── executor.py        # Core Executor class with all functions
│       │                       # - handle_event() - main dispatcher
│       │                       # - import_GOES_data_to_timestream()
│       │                       # - create_GOES_data_annotations()
│       │                       # - import_STIX_to_timestream()
│       │                       # - get_PADRE_orbit_data()
│       │                       # - import_UDL_REACH_to_s3()
│       │                       # - generate_loc_report()
│       └── config/
│           └── ccsdspy/
│               └── config.yml # CCSDSPY configuration for data parsing
├── tests/
│   ├── conftest.py            # pytest fixtures (fixtures, mocking setup)
│   ├── test_executor.py       # Main test suite
│   └── test_data/
│       └── test_executor_event.json  # Sample EventBridge event
├── Dockerfile                 # Lambda container image (uses base from PADRE)
└── requirements.txt           # Production dependencies

Root project files:
├── buildspec.yml              # AWS CodeBuild configuration
├── ruff.toml                  # Linting rules
├── requirements.dev.txt       # Development dependencies (testing, linting)
└── README.rst                 # Full project documentation
```

## Key Technical Details

**Python Version**: 3.9+ (Lambda runtime compatibility)

**AWS Services**:
- **EventBridge/CloudWatch Events**: Triggers Lambda via rule names
- **Secrets Manager**: Stores credentials (Grafana API key, UDL auth, etc.)
- **Timestream**: Time-series database for satellite data
- **S3**: File storage for REACH and LOC report data
- **Lambda**: Function runtime environment

**Environment Variables**:
- `SECRET_ARN_GRAFANA`: ARN for Grafana API credentials secret
- `SECRET_ARN_UDL`: ARN for UDL authentication credentials secret
- Fetched credentials are injected as environment variables in constructor

**Core Dependencies** (from `requirements.txt`):
- `swxsoc`: SWxSOC core library (S3, Slack, logging, config utilities)
- `swxsoc_reach`: REACH data download utilities
- `padre_craft`: PADRE satellite orbit utilities
- `stixdcpy`: STIX X-ray data access
- `pandas`: Data manipulation
- AWS SDK (`boto3`): Built-in to Lambda

**Development Dependencies** (from `requirements.dev.txt`):
- `pytest`, `pytest-astropy`, `pytest-cov`: Testing framework
- `moto==5.0.15`: AWS service mocking for tests
- `ruff`: Code linting and formatting

**Linting Configuration** (see [ruff.toml](ruff.toml)):
- Ignores specific rules (EXE002, BLE001, TRY201, etc.)
- Applied to both `src/` and `tests/` directories

## Testing Conventions

- **Fixtures**: `conftest.py` provides:
  - `isolate_executor_env`: Auto-applied fixture that clears credential-related env vars before each test
  - `aws_credentials`: Provides fake AWS credentials for boto3
  - `mock_aws`: Mocks all AWS services with moto

- **Mocking Strategy**: 
  - Use `moto` to mock AWS Secrets Manager, Timestream, S3, etc.
  - Example: Create mock secrets in Secrets Manager and set `SECRET_ARN_*` env vars before instantiating `Executor`
  - Patch external dependencies (e.g., `stixdcpy.LightCurves.from_sdc`) to avoid downloading large datasets during tests

- **Test File Naming**: Place tests in `lambda_function/tests/test_*.py`
- **Running Subsets**: Use `-k` flag to run tests matching a pattern (e.g., `-k "import_"`)

## CI/CD Workflows

CI/CD is configured in [.github/workflows/](.github/workflows/):
- **testing.yml**: Runs pytest with coverage on PR and scheduled basis
- **codestyle.yml**: Runs ruff linting checks

## Adding New Executor Functions

1. Define a new method in the `Executor` class in [lambda_function/src/executor/executor.py](lambda_function/src/executor/executor.py)
2. The method name should match the EventBridge rule name (e.g., `my_new_function`)
3. Create a CloudWatch/EventBridge rule named `my_new_function` with desired schedule
4. Add that rule as a trigger to the executor Lambda function in AWS
5. Add unit tests in [lambda_function/tests/test_executor.py](lambda_function/tests/test_executor.py)
6. Document the function in [README.rst](README.rst)

Key pattern for functions:
```python
@staticmethod  # or instance method if state needed
def my_new_function() -> None:
    """Brief description of what the function does."""
    # Implementation here
    # Use swxsoc utilities for common operations
```

## Key Decisions & Patterns

1. **Rule Name → Function Mapping**: The EventBridge rule name is extracted from the event and used to dispatch to a corresponding executor method. This decouples scheduling configuration from code.

2. **Secrets Manager Integration**: Credentials are loaded in the `Executor` constructor and injected as environment variables. This avoids hardcoding secrets and supports multiple deployment environments.

3. **Modular Data Handling**: All AWS service interactions (S3, Timestream, Secrets) and data source APIs are abstracted via utility libraries (`swxsoc`, `swxsoc_reach`, etc.), keeping executor functions focused on business logic.

4. **Comprehensive Test Coverage**: Tests use `moto` to mock AWS services and avoid requiring actual AWS credentials or external data sources during CI/CD.

5. **Docker for Local Testing**: The Dockerfile ensures the exact Lambda runtime environment can be tested locally before deployment.

6. **Static Methods**: Most executor functions are static methods since they don't maintain state, simplifying testing and invocation.

## Deployment

The function is deployed via AWS CodeBuild using [buildspec.yml](buildspec.yml):
- Builds a Docker image with the Lambda function
- Pushes to private ECR (tagging by git tag or timestamp)
- Updates Lambda function configuration to use the new image

## Common Development Tasks

| Task | Command/Approach |
|------|------------------|
| Run all tests | `pytest --pyargs lambda_function/tests --cov=lambda_function/src --cov-report=html` |
| Check linting | `ruff check lambda_function/src lambda_function/tests` |
| Format code | `ruff format lambda_function/src lambda_function/tests` |
| Build Lambda image locally | `cd lambda_function && docker build -t sdc_aws_executor_lambda:latest .` |
| Test Lambda locally | See Docker commands above; use `test_executor_event.json` as sample |
| Add new executor function | Add method to `Executor` class, create EventBridge rule, add unit test |
| Update dependencies | Edit `lambda_function/requirements.txt` (prod) or `requirements.dev.txt` (dev); rebuild Docker image |
| View test coverage | After running pytest with `--cov-report=html`, open `htmlcov/index.html` |
| Run a single test | `pytest lambda_function/tests/test_executor.py::test_constructor_loads_secrets_from_moto -v` |

## Questions or Issues?

- For architecture questions, refer to [README.rst](README.rst#Architecture)
- For swxsoc library details, see the [swxsoc repository](https://github.com/swxsoc/swxsoc)
- For swxsoc_reach details, see the [swxsoc_reach repository](https://github.com/swxsoc/swxsoc_reach)
- For CI/CD configuration, check [.github/workflows/](.github/workflows/)
- For AWS Lambda deployment details, see [buildspec.yml](buildspec.yml)
