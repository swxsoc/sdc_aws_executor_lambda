"""
This Module contains the Exector class that determines
which function to execute based on the event rule.
"""

import os
import json
import re
from typing import Any, Dict

from astropy import units as u
from astropy.table import Table
from astropy.time import Time, TimeDelta
from astropy.timeseries import TimeSeries
import pandas as pd
import boto3
from swxsoc import log
from swxsoc.util import util




def handle_event(event: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles the event passed to the Lambda function to initialize the FileProcessor.

    :param event: Event data passed from the Lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Success) or 500 (Error) HTTP response
    :rtype: dict
    """
    log.info("Received event", extra={"event": event, "context": context})

    try:
        # Validate event structure
        if not event.get('resources'):
            raise ValueError("Event is missing 'resources' key.")

        # Extract the rule ARN
        rule_arn = event['resources'][0]
        log.debug("Extracted rule ARN", extra={"rule_arn": rule_arn})

        # Use regex to extract the rule name
        rule_name_match = re.search(r'rule/(.+)', rule_arn)
        if not rule_name_match:
            raise ValueError("Invalid rule ARN format. Could not extract rule name.")
        function_name = rule_name_match.group(1)
        log.info(f"Rule Name Extracted: {function_name}")

        # Execute the corresponding function
        executor = Executor(function_name)
        executor.execute()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Execution completed successfully."}),
        }

    except Exception as e:
        log.error("Error handling event", exc_info=True, extra={"event": event})
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }


class Executor:
    """
    Executes the appropriate function based on the event rule.

    :param function_name: The name of the function to execute based on the event
    :type function_name: str
    """

    def __init__(self, function_name: str) -> None:
        self.function_name = function_name
        self.function_mapping = {
            "import_GOES_data_to_timestream": self.import_GOES_data_to_timestream,
            "create_GOES_data_annotations": self.create_GOES_data_annotations,
        }
        try:
            # Initialize Grafana API Key
            session = boto3.session.Session()
            secret_arn = os.getenv("SECRET_ARN", None)
            client = session.client(service_name="secretsmanager")
            response = client.get_secret_value(SecretId=secret_arn)
            secret = json.loads(response["SecretString"])
            # Set Grafana API Key environment variable
            os.environ["GRAFANA_API_KEY"] = secret["grafana_api_key"]
            log.info("Grafana API Key loaded")
        except Exception as e:
            log.error("Error initializing Grafana API Key", exc_info=True)
            

    def execute(self) -> None:
        """
        Executes the mapped function.
        """
        if self.function_name not in self.function_mapping:
            raise ValueError(f"Function '{self.function_name}' is not recognized.")
        log.info(f"Executing function: {self.function_name}")
        self.function_mapping[self.function_name]()

    @staticmethod
    def import_GOES_data_to_timestream() -> None:
        """
        Imports GOES data to Timestream.
        """

        log.info("Importing GOES data to Timestream")
        try:
            goes_json_data = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/xrays-3-day.json")
            last_day = Time((Time.now() - TimeDelta(1 * u.day)).iso[0:10])

            goes_short = goes_json_data[goes_json_data["energy"] == "0.05-0.4nm"]
            goes_long = goes_json_data[goes_json_data["energy"] == "0.1-0.8nm"]
            time_tags = Time([str(t)[:-1] for t in goes_short["time_tag"].values], format="isot")

            tsa = TimeSeries(time=time_tags, data={"xrsa": goes_short["flux"].values * u.W / u.m**2})
            tsb = TimeSeries(time=time_tags, data={"xrsb": goes_long["flux"].values * u.W / u.m**2})

            tsa_lastday = tsa.loc[last_day:last_day + TimeDelta(1 * u.day)]
            tsb_lastday = tsb.loc[last_day:last_day + TimeDelta(1 * u.day)]

            if len(tsa_lastday) > 0:
                util.record_timeseries(tsa_lastday, ts_name="GOES", instrument_name="goes xrsa")
                util.record_timeseries(tsb_lastday, ts_name="GOES", instrument_name="goes xrsb")
            log.info("GOES data imported successfully")
        except Exception as e:
            log.error("Error importing GOES data to Timestream", exc_info=True)
            raise

    @staticmethod
    def create_GOES_data_annotations() -> None:
        """
        Creates annotations for GOES data.
        """
        log.info("Creating GOES data annotations")
        try:
            flare_events = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/xray-flares-7-day.json")
            goes_class = flare_events["max_class"].astype(str).tolist()
            start_time = Time(flare_events["begin_time"].values)
            end_time = Time(flare_events["end_time"].values)
            peak_time = Time(flare_events["max_time"].values)
            last_day = Time((Time.now() - TimeDelta(1 * u.day)).iso[0:10])

            event_list = TimeSeries(
                time=start_time,
                data={"class": goes_class, "peak_time": peak_time, "end_time": end_time},
            )
            event_list_lastday = event_list.loc[last_day:last_day + TimeDelta(1 * u.day)]

            dashboard_name = "Context Observations"
            panel_name = "GOES XRS"

            for this_event in event_list_lastday:
                annotation_text = this_event["class"]
                tags = ["GOES XRS", "flare"]

                util.create_annotation(
                    start_time=this_event["time"],
                    end_time=this_event["end_time"],
                    text=annotation_text,
                    tags=tags,
                    dashboard_name=dashboard_name,
                    panel_name=panel_name,
                    mission_dashboard="padre",
                    overwrite=True,
                )

                tags.append("peak")
                util.create_annotation(
                    start_time=this_event["peak_time"],
                    text=annotation_text,
                    tags=tags,
                    dashboard_name=dashboard_name,
                    panel_name=panel_name,
                    mission_dashboard="padre",
                    overwrite=True,
                )
                log.info("GOES data annotations created successfully")
        except Exception as e:
            log.error("Error creating GOES data annotations", exc_info=True)
            raise
