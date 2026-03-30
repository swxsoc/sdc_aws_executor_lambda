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

### Generate lines of code report and upload


### import_UDL_REACH_to_timestream
Likely a temporary addition. Gets REACH data from the UDL. Grabs data from 2 hours ago to 1 hour ago.

### download_UDL_REACH_to_file
Downloads REACH data from UDL in chunked requests, combines all records, and writes a single output file to Lambda storage for upload.

Suggested daily pattern:
- Set `REACH_WINDOW_SECONDS=86400` for one day per run
- Schedule one daily EventBridge trigger
- Upload the single combined artifact produced by each run

Recommended environment variables for this function:
- `REACH_SENSOR_ID` (default `ALL`)
    - Defines the Sensors to query from UDL
- `REACH_DESCRIPTOR` (default `QUICKLOOK`)
    - Defined the data product to query from UDL
- `REACH_FILE_FORMAT` (default `json`)
    - Defines the File format to save/upload data as. Options are `json` and `csv`. 
- `REACH_DELAY_SECONDS` (default `7200`)
    - Time offset from `datetime.now(timezone.utc)` to *end* data download
- `REACH_WINDOW_SECONDS` (default `600`)
    - Time wondow from end. 
    - The start ends up being `datetime.now(timezone.utc) - REACH_DELAY_SECONDS - REACH_WINDOW_SECONDS`
- `REACH_OUTPUT_DIR` (default `/tmp`)
    - Where in the Lambda Container to save the file. We should not need to change this at all. 
- `REACH_DESTINATION_BUCKET` (default `dev-swxsoc-pipeline-incoming`)
    - Bucket name to copy the file to. 
- `REACH_UDL_MAX_CONCURRENT_REQUESTS` (default `8`)
    - Maximum number of cuncurrent workers to pull data from UDL.

### import_stix_to_timestream
Gets solar orbiter stix quicklook lightcurve data.

## Buildig & Running Locally

The image can be built and run locally. You can specify the build base image at runtime. The base image, at the time of writing this, defaults to the `padre-swsoc-docker-lambda-base:latest` in AWS. 

```sh
export BASE_IMAGE=public.ecr.aws/w5r9l1c8/padre-swsoc-docker-lambda-base:latest
export IMAGE_NAME=swxsoc_sdc_aws_executor_lambda
export VERSION=`date -u +"%Y%m%d%H%M%S"`

# Build the Image
docker build --no-cache --build-arg BASE_IMAGE=$BASE_IMAGE -t $IMAGE_NAME:latest lambda_function/.
# Tag the Image with a Version
docker tag $IMAGE_NAME:latest $IMAGE_NAME:$VERSION
```

You can run the image, specifying access tokens to connedted services as needed. 
You can get the Grafafana and UDL ARN from AWS.

```sh
docker run -p 9000:8080 \
  -e REACH_DESTINATION_BUCKET="dev-swxsoc-pipeline-incoming" \
  -e SECRET_ARN_GRAFANA=$SECRET_ARN_GRAFANA$ \
  -e SECRET_ARN_UDL=$SECRET_ARN_UDL \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
  swxsoc_sdc_aws_executor_lambda:latest
```

Finally, you can invoke the executor lambda to run locally, from a separate terminal. 
You can customize the executor function you wish to run within the JSON payload. 

```sh
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d @lambda_function/tests/test_data/test_executor_event.json
```

## Error Handling
- HTTP 200: Successful execution
- HTTP 500: Execution failure with error details
- Comprehensive logging via swxsoc

