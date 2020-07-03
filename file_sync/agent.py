"""Syncs new files found in the specified file source.

Usage:
    filesync [--config-file=<cf>] [--file-source=<fs>]

Options:
    --config-file=<cf>     Configuration file path
    --file-source=<fs>     Type of persistent storage, e.g. s3 or ftp
"""

import configparser
from datetime import datetime
from dateutil import parser
import docopt
import json
import logging.config
import os
import pkg_resources
import requests
import sys

from .geotiff_processing_utils import get_job_config_map as get_geotiff_job_config_map, get_workflow_display_info as get_geotiff_workflow_display_info

logging.config.fileConfig("config/logging_config.ini")
LOG = logging.getLogger("root")


def main():
    LOG.info("Launching run ..")

    # Get command line arguments
    args = docopt.docopt(__doc__)
    config_file = args["--config-file"] or "/var/cron.d/filesync.ini"
    file_source = args["--file-source"] or "s3"

    LOG.info(f"* config file: {config_file}")
    LOG.info(f"* file source: {file_source}")

    # Read configuration
    config = configparser.ConfigParser()
    config.read(config_file)
    agent_config = config["agent"]
    plugin_config = config[file_source] if file_source in config else None

    # File that holds the watermark (file timestamp until which jobs have been created)
    watermark_file = agent_config["watermark_file"] or "/var/data/filesync/watermark"
    # Read watermark timestamp (timestamp of last media file or folder for which processing job was created in the backend)
    watermark_ts = read_watermark(watermark_file)
    if watermark_ts is None:
        LOG.info("Found no watermark!")
    else:
        LOG.info("Watermark: {}".format(watermark_ts.isoformat()))

    # Type of workflow that this agent this configured to create in the backend for every media asset it discovers
    workflow_type = agent_config["workflow"] or "geotiff_image_processing"
    LOG.info(f"Agent is configured to create workflow: {workflow_type}")

    # Get auth token from backend
    try:
        auth_token = get_auth_token(agent_config)
    except requests.RequestException:
        LOG.exception("Failed to get auth token from backend!")
        sys.exit(1)

    # Load file-sync plugin for specified file source
    LOG.info("Searching for file-sync plugins ..")
    file_sync_plugin = None
    for entry_point in pkg_resources.iter_entry_points("file_sync_plugins"):
        LOG.info(f"Plugin: {entry_point}")
        if entry_point.name == file_source:
            LOG.info("Found match!")
            file_sync_plugin = entry_point.load()()
            LOG.info(f"Loaded {entry_point.name} plugin.")
            break

    if file_sync_plugin is None:
        LOG.error("No file-sync plugin found for specified file source! Exiting ..")
        sys.exit(1)

    # Discover files
    try:
        ts_uniqueid_config_tuples = file_sync_plugin.discover(watermark_ts, plugin_config)
    except Exception:
        LOG.exception("Error while discovering new media content!")
        sys.exit(1)

    # Order tuples in ascending order of timestamp
    ts_uniqueid_config_tuples.sort(key=lambda tup: tup[0])

    # Create a unique id for this run, that will be used as the batch id for the jobs created in this run.
    batch_id = str(datetime.now().timestamp())

    # For each tuple ..
    move_watermark = True
    for tup in ts_uniqueid_config_tuples:
        timestamp = tup[0]
        workflow_id = tup[1]
        media_path_details = tup[2]

        # Check if job exists in the backend for the media path. If it does, then ignore this media path
        try:
            if not workflow_exists(workflow_id, auth_token, agent_config):
                ts_str = timestamp.isoformat()
                LOG.info(f"Creating workflow for media asset with unique id '{workflow_id}' and timestamp {ts_str} ..")

                job_ids = create_worklow(workflow_id, batch_id, workflow_type,
                                         get_job_config_map(media_path_details, workflow_type),
                                         get_workflow_display_info(media_path_details, workflow_type),
                                         auth_token, agent_config)
                LOG.info(f"Created jobs: {json.dumps(job_ids)}")

        except Exception:
            move_watermark = False  # Make sure this media path is not ignored in next run
            LOG.exception("Failed to create workflow!")

        if move_watermark and (watermark_ts is None or timestamp.timestamp() > watermark_ts.timestamp()):
            update_watermark(timestamp, watermark_file)


if __name__ == "__main__":
    main()


def read_watermark(watermark_file):
    if os.path.exists(watermark_file):
        with open(watermark_file, "r") as f:
            return parser.parse(f.readline())
    return None


def update_watermark(watermark_ts, watermark_file):
    with open(watermark_file, "w") as f:
        f.write(watermark_ts.isoformat())


def get_job_config_map(media_path_details, workflow_type):
    if workflow_type == "geotiff_image_processing":
        return get_geotiff_job_config_map(media_path_details)
    else:
        raise Exception(f"Workflow type '{workflow_type} not supported!")


def get_workflow_display_info(media_path_details, workflow_type):
    if workflow_type == "geotiff_image_processing":
        return get_geotiff_workflow_display_info(media_path_details)
    else:
        raise Exception(f"Workflow type '{workflow_type} not supported!")


def get_auth_token(agent_config):
    vault_file = agent_config["vault_file"]
    config = configparser.ConfigParser()
    with open(vault_file, "r") as f:
        config.read_string("[vault]\n" + f.read())
        vault = config["vault"]
        username = vault["USERNAME"]
        password = vault["PASSWORD"]
    backend_url = agent_config["backend_url"]

    response = requests.post(
        f"{backend_url}/api/token-auth/",
        json={"username": username, "password": password},
        verify=False,
    )
    if response.status_code == 200:
        return response.json()["token"]
    else:
        error_message = response.content.decode("utf-8")
        raise requests.RequestException("({}) {}".format(response.status_code, error_message))


def workflow_exists(workflow_id, auth_token, agent_config):
    backend_url = agent_config["backend_url"]
    headers = {"authorization": "Token " + auth_token}
    response = requests.get(
        f"{backend_url}/api/jobs/?job_group_id={workflow_id}",
        headers=headers,
        verify=False,
    )
    return response.status_code == 200


def create_worklow(workflow_id, batch_id, workflow_type, job_config_map, workflow_display_info, auth_token, agent_config):
    backend_url = agent_config["backend_url"]
    headers = {"authorization": "Token " + auth_token}
    post_body = {
        "workflow": workflow_type,
        "jobGroupId": workflow_id,
        "batchId": batch_id,
        "jobConfig": job_config_map,
        "displayInfo": workflow_display_info,
    }
    response = requests.post(
        f"{backend_url}/api/jobs/",
        json=post_body,
        headers=headers,
        verify=False)

    if response.status_code == 201:
        return response.json()["ids"]
    else:
        error_message = response.content.decode("utf-8")
        raise requests.RequestException("({}) {}".format(response.status_code, error_message))
