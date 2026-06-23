========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - build status
      - |testing| |codestyle| |coverage|

.. |testing| image:: https://github.com/swxsoc/sdc_aws_executor_lambda/actions/workflows/testing.yml/badge.svg
    :target: https://github.com/swxsoc/sdc_aws_executor_lambda/actions/workflows/testing.yml
    :alt: testing status

.. |codestyle| image:: https://github.com/swxsoc/sdc_aws_executor_lambda/actions/workflows/codestyle.yml/badge.svg
    :target: https://github.com/swxsoc/sdc_aws_executor_lambda/actions/workflows/codestyle.yml
    :alt: codestyle and linting

.. |coverage| image:: https://codecov.io/gh/swxsoc/sdc_aws_executor_lambda/graph/badge.svg
    :target: https://codecov.io/gh/swxsoc/sdc_aws_executor_lambda
    :alt: code coverage

.. end-badges

This repository defines the code for the SWxSOC executor Lambda function.
The function implements an executor pattern to run scheduled tasks via
CloudWatch Events/EventBridge rules, where each rule name maps directly to
a corresponding executor function.

Architecture
------------

- CloudWatch Events/EventBridge rules trigger the Lambda.
- Rule name pattern: ``<function_name>``.
- Function mapping is handled by the ``Executor`` class.
- Modular design supports adding new functions independently.

Setup
-----

Requirements
^^^^^^^^^^^^

- AWS Lambda
- CloudWatch Events/EventBridge
- AWS Secrets Manager for credentials
- Python 3.9+

Environment Variables
^^^^^^^^^^^^^^^^^^^^^

- ``SECRET_ARN``: Secrets Manager ARN containing required credentials.

Implementation
--------------

Adding New Functions
^^^^^^^^^^^^^^^^^^^^

1. Add a function to the ``Executor`` class.
2. Map the function in the ``function_mapping`` dictionary.
3. Create a CloudWatch/EventBridge rule whose name matches the function name and desired schedule.
4. Add that rule as a trigger to the executor Lambda function.

Included Functions
------------------

import_GOES_data_to_timestream
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Processes GOES X-ray satellite data:

- Fetches 3-day X-ray flux data from NOAA.
- Filters the last 24 hours.
- Handles both wavelength channels (0.05-0.4nm, 0.1-0.8nm).
- Stores records in Amazon Timestream.

create_GOES_data_annotations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Manages solar flare annotations:

- Processes 7-day GOES flare data.
- Creates Grafana annotations for flare events.
- Marks start, peak, and end times.
- Tags events for filtering.

Generate lines of code report and upload
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Generates a lines-of-code report and uploads the output artifact.


import_UDL_REACH_to_s3
^^^^^^^^^^^^^^^^^^^^^^^

Downloads REACH data from UDL in chunked requests, combines all records,
and writes a single output file to Lambda storage for upload.

The query window is snapped to UTC midnight boundaries so each run always
covers a whole midnight-to-midnight day, independent of the exact second
the Lambda executes. The window is driven by two day-based offsets
measured from UTC midnight of the current run day:

- ``REACH_WINDOW_END_DAYS_AGO``: number of days before today's UTC
  midnight at which the window ends.
- ``REACH_WINDOW_DAYS``: length of the window in whole days.

Suggested daily pattern:

- Schedule one daily EventBridge trigger, e.g. ``cron(0 6 * * ? *)`` UTC.
- Keep ``REACH_WINDOW_END_DAYS_AGO=1`` and ``REACH_WINDOW_DAYS=1`` to pull
  the full day before yesterday (54 hours ago to 30 hours ago at run
  time), e.g. the window ``[today 00:00 - 2 days, today 00:00 - 1 day)``.
- Upload the single combined artifact produced by each run.

Recommended environment variables:

- ``REACH_SENSOR_ID`` (default: ``ALL``): sensors to query from UDL.
- ``REACH_DESCRIPTOR`` (default: ``QUICKLOOK``): data product to query from UDL.
- ``REACH_FILE_FORMAT`` (default: ``json``): output format (``json`` or ``csv``).
- ``REACH_WINDOW_END_DAYS_AGO`` (default: ``1``): days before today's UTC midnight at which the window ends.
- ``REACH_WINDOW_DAYS`` (default: ``1``): length of the query window in whole days.
- ``REACH_OUTPUT_DIR`` (default: ``/tmp``): Lambda container directory for output files.
- ``REACH_DESTINATION_BUCKET_DEV`` (default: ``dev-swxsoc-pipeline-incoming``): bucket to upload output files.
- ``REACH_DESTINATION_BUCKET_PROD`` (default: ``swxsoc-pipeline-incoming``): bucket to upload output files.
- ``REACH_UDL_MAX_CONCURRENT_REQUESTS`` (default: ``8``): max concurrent workers for UDL pulls.
- ``REACH_UDL_INITIAL_RATE`` (default: ``5.0``): AIMD starting request rate (requests/second).
- ``REACH_UDL_ADDITIVE_INCREASE`` (default: ``1.0``): AIMD additive increase after successful requests.
- ``REACH_UDL_MULTIPLICATIVE_DECREASE`` (default: ``0.5``): AIMD decrease factor after HTTP 429 responses.
- ``REACH_UDL_MIN_RATE`` (default: ``5.0``): AIMD floor.
- ``REACH_UDL_MAX_RATE`` (default: ``25.0``): AIMD ceiling.

import_stix_to_timestream
^^^^^^^^^^^^^^^^^^^^^^^^^

Gets Solar Orbiter STIX quicklook lightcurve data.

Running Unit Tests
------------------

.. code-block:: sh

    pytest --pyargs lambda_function/tests --cov=lambda_function/src --cov-report=html

Building and Running Locally
----------------------------

The container image can be built and run locally. You can specify the base image at runtime.
At the time of writing, the base image defaults to ``padre-swsoc-docker-lambda-base:latest`` in AWS.

.. code-block:: sh

    export BASE_IMAGE=public.ecr.aws/w5r9l1c8/padre-swsoc-docker-lambda-base:latest
    export IMAGE_NAME=swxsoc_sdc_aws_executor_lambda
    export VERSION=$(date -u +"%Y%m%d%H%M%S")

    # Build the image
    docker build --no-cache --build-arg BASE_IMAGE=$BASE_IMAGE -t $IMAGE_NAME:latest lambda_function/.

    # Tag the image with a version
    docker tag $IMAGE_NAME:latest $IMAGE_NAME:$VERSION

Run the image and pass access tokens/secrets for connected services as needed.
You can retrieve the Grafana and UDL ARNs from AWS.

.. code-block:: sh

    docker run -p 9000:8080 \
      -e REACH_DESTINATION_BUCKET_DEV="dev-swxsoc-pipeline-incoming" \
      -e REACH_DESTINATION_BUCKET_PROD="swxsoc-pipeline-incoming" \
      -e SECRET_ARN_GRAFANA=$SECRET_ARN_GRAFANA \
      -e SECRET_ARN_UDL=$SECRET_ARN_UDL \
      -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
      -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
      -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
      -e LAMBDA_ENVIRONMENT="PRODUCTION" \
      swxsoc_sdc_aws_executor_lambda:latest

From a separate terminal, invoke the Lambda locally. The executor function can be
customized in the JSON payload.

.. code-block:: sh

    curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
      -d @lambda_function/tests/test_data/test_executor_event.json

Error Handling
--------------

- HTTP 200: successful execution.
- HTTP 500: execution failure with error details.
- Comprehensive logging via ``swxsoc``.

Acknowledgements
----------------

The package template used by this package is based on the one developed by the
`NASA Space Weather Science Operations Center (SWxSOC) <https://swxsoc.github.io>`_ which is based on those provided by
`OpenAstronomy community <https://openastronomy.org>`_ and the `SunPy Project <https://sunpy.org/>`_.

This project makes use of the `NASA Space Weather Science Operations Center (SWxSOC) <https://swxsoc.github.io>`_.
