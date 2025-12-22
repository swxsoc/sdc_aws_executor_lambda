"""
This Module contains the Exector class that determines
which function to execute based on the event rule.
"""

import os
import json
import re
import csv
import shutil
import subprocess
import tempfile
import time
from turtle import st
import requests
import logging
import traceback
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
            "generate_cloc_report_and_upload": self.generate_cloc_report_and_upload,
            "import_UDL_REACH_to_timestream": self.import_UDL_REACH_to_timestream,
            "get_padre_orbit_data": self.get_padre_orbit_data,
        }
        try:
            # Initialize Grafana API Key
            session = boto3.session.Session()
            env_to_ids = {"SECRET_ARN_GRAFANA": "grafana_api_key", "SECRET_ARN_UDL": "basicauth"}
            for key, value in env_to_ids.items():
                secret_arn = os.getenv(key, None)
                client = session.client(service_name="secretsmanager")
                response = client.get_secret_value(SecretId=secret_arn)
                secret = json.loads(response["SecretString"])
                os.environ[value.upper()] = secret[value]
                log.info(f"{value} API Key loaded")
        except Exception as e:
            log.error("Error reading secrets", exc_info=True)
            

    def execute(self) -> None:
        """
        Executes the mapped function.
        """
        if self.function_name not in self.function_mapping:
            raise ValueError(f"Function '{self.function_name}' is not recognized.")
        log.info(f"Executing function: {self.function_name}")
        self.function_mapping[self.function_name]()


    @staticmethod
    def get_padre_orbit_data() -> None:
        from padre_craft.orbit import PadreOrbit
        from padre_craft.io.aws_db import record_orbit

        # get 3 days of data to ensure coverage
        # run it every day so get 2 changes to fix any dropouts
        dt = TimeDelta(3 * u.day)
        delay = TimeDelta(0 * u.day)  # TLEs should always be current so no delay
        now = Time.now()
        tr = [now - delay - dt, now - delay]
        padre_orbit = PadreOrbit()  # gets the latest tle from celetrak
        time_resolution = 10 * u.s
        log.info(f"Calculating Padre orbit from {tr[0].iso} to {tr[1].iso} every {time_resolution.to(u.s)}")
        padre_orbit.calculate(tstart=tr[0], tend=tr[1], dt=time_resolution)
        if padre_orbit.timeseries is not None:
            if len(padre_orbit.timeseries) > 0:
                record_orbit(padre_orbit.timeseries)
                log.info(f"Recorded padre orbit from {tr[0].iso} to {tr[1].iso} every {time_resolution.to(u.s)}")
        else:
            log.warning("No Padre orbit data to record")


    @staticmethod
    def import_UDL_REACH_to_timestream() -> None:
        """
        Imports data from UDL, grabs some REACH data and imports to Timestream
        """
        basicAuth = os.environ['basicauth'.upper()]
        baseurl = 'https://unifieddatalibrary.com/udl/spaceenvobservation'

        tdelay = TimeDelta(2 * u.hour)
        dt = TimeDelta(10 * u.minute)
        start_time = (Time.now() - tdelay)
        end_time = start_time + dt
        obtime = start_time.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z..'
        obtime += end_time.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
        sensor = 'REACH-171'

        url = f'{baseurl}?obTime={obtime}&idSensor={sensor}&source=Aerospace&dataMode=REAL&descriptor=QUICKLOOK&sort=obTime'
        log.info(f"Requesting REACH data from UDL at {url}")
        response = requests.get(url, headers={'Authorization':basicAuth}, verify=False)
        if response:
            json_data = response.json()
            log.info(f"Received {len(json_data)} entries.")
            available_obs = set([t['seoList'][0]['obDescription'] for t in json_data])
            for this_ob in available_obs:
                times = [Time(t['obTime']) for t in json_data if t['seoList'][0]['obDescription'] == this_ob]
                ts = TimeSeries(time = times)
                ts.meta['obDescription'] = this_ob
                ob_value = [t['seoList'][0]['obValue'] for t in json_data if t['seoList'][0]['obDescription'] == this_ob]
                ts['value'] = ob_value
                key_list = ['lat', 'lon', 'alt', 'observatoryName', 'idSensor']
                for this_key in key_list:
                    ts[this_key] = [t[this_key] for t in json_data if t['seoList'][0]['obDescription'] == this_ob]
                if len(ts) > 0:
                    instr_name = str(this_ob).split(')')[0] + ')'
                    util.record_timeseries(ts, ts_name="REACH", instrument_name=instr_name)
        else:
            log.info(f"No response received from {url}")

    @staticmethod
    def import_GOES_data_to_timestream():
        """
        Imports GOES data to Timestream.
        """

        log.info("Importing GOES data to Timestream")
        try:
            goes_json_data = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json")
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
    
    
    @staticmethod        
    def generate_cloc_report_and_upload() -> str:
        orgs_or_users = os.environ.get('GITHUB_ORGS_USERS', '').split(',')
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_key = os.environ.get('S3_KEY', 'combined_cloc_report.csv')

        if not orgs_or_users or not orgs_or_users[0]:
            raise ValueError('GITHUB_ORGS_USERS environment variable not set')
        if not s3_bucket:
            raise ValueError('S3_BUCKET environment variable not set')

        tmp_dir = tempfile.mkdtemp(dir='/tmp', prefix='repo-loc-')
        final_csv_path = os.path.join(tmp_dir, 'combined_cloc_report.csv')
        headers = {}
        base_url = 'https://api.github.com'

        try:
            with open(final_csv_path, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                header_written = False

                for target in orgs_or_users:
                    target = target.strip()
                    repos = []

                    # Try organization
                    org_url = f'{base_url}/orgs/{target}/repos?type=sources'
                    response = requests.get(org_url, headers=headers)

                    if response.status_code == 200:
                        repos = response.json()
                    elif response.status_code == 404:
                        # Try user
                        user_url = f'{base_url}/users/{target}/repos?type=sources'
                        user_response = requests.get(user_url, headers=headers)
                        if user_response.status_code == 200:
                            repos = user_response.json()
                        else:
                            print(f"⚠️ Could not find user/org: {target}")
                            continue
                    else:
                        print(f"⚠️ Failed to fetch repos for {target}: {response.status_code}")
                        continue

                    print(f"📦 Found {len(repos)} repos for {target}")

                    for repo in repos:
                        if repo['fork'] or repo['archived']:
                            continue

                        repo_name = repo['name']
                        destination = os.path.join(tmp_dir, repo_name)
                        report_file = os.path.join(tmp_dir, f"{repo_name}.csv")

                        try:
                            # Clone repo
                            clone_url = repo['clone_url']  # Use public HTTPS URL
                            clone_process = subprocess.run(
                                ['git', 'clone', '--depth', '1', '--quiet', clone_url, destination],
                                capture_output=True, text=True
                            )
                            if clone_process.returncode != 0:
                                print(f"❌ Failed to clone {repo_name}: {clone_process.stderr}")
                                continue

                            # Run cloc
                            cloc_process = subprocess.run(
                                ['cloc', destination, '--quiet', '--csv', f"--report-file={report_file}"],
                                capture_output=True, text=True
                            )
                            if os.path.exists(report_file) and cloc_process.returncode == 0:
                                with open(report_file, 'r') as repo_csv:
                                    csv_reader = csv.reader(repo_csv)
                                    for row in csv_reader:
                                        if row and not row[0].startswith('#'):
                                            if not header_written:
                                                csv_writer.writerow(['org_name', 'repo_name'] + row)
                                                header_written = True
                                            else:
                                                csv_writer.writerow([target, repo_name] + row)
                            else:
                                print(f"⚠️ cloc failed on {repo_name}")
                        finally:
                            shutil.rmtree(destination, ignore_errors=True)

            # Upload to S3
            s3 = boto3.client('s3')
            s3.upload_file(final_csv_path, s3_bucket, s3_key)
            print(f"✅ Uploaded CSV to s3://{s3_bucket}/{s3_key}")
            return f"s3://{s3_bucket}/{s3_key}"

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
