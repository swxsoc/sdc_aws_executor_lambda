# Executor Lambda Function

Lambda function implementing an executor pattern to run scheduled tasks via CloudWatch Events/EventBridge rules. Each rule's name maps directly to its corresponding function, enabling independent scheduling.

## Architecture

- CloudWatch Events/EventBridge rules trigger the Lambda
- Rule name pattern: `<function_name>`
- Function mapping handled by Executor class
- Modular design for easy addition of new functions

## Setup

### Requirements
- AWS Lambda
- CloudWatch Events/EventBridge
- AWS Secrets Manager for credentials
- Python 3.9+

### Environment Variables
- `SECRET_ARN`: Secrets Manager ARN containing required credentials

## Implementation

### Adding New Functions
1. Add function to Executor class
2. Map function in `function_mapping` dictionary
3. Create corresponding CloudWatch rule which matches the function name and a schedule
4. Add rule as trigger to executor lambda function

## Included Functions

### import_GOES_data_to_timestream
Processes GOES X-ray satellite data:
- Fetches 3-day X-ray flux data from NOAA
- Filters last 24 hours
- Handles both wavelength channels (0.05-0.4nm, 0.1-0.8nm)
- Stores in Amazon Timestream

### create_GOES_data_annotations
Manages solar flare annotations:
- Processes 7-day GOES flare data
- Creates Grafana annotations for flare events
- Marks start, peak, and end times
- Tags events for filtering

## Error Handling
- HTTP 200: Successful execution
- HTTP 500: Execution failure with error details
- Comprehensive logging via swxsoc

