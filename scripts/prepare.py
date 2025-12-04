"""
Script to generate the Kubernetes manifests for the log-consolidator-checker jobs, from the template and a range of dates and hours.

Dirty but does the job, and no cue involved.

!! Make sure the template match your needs (e.g. image tag, resources, configmap...)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from time import strptime
import os
import shutil
from pathlib import Path
import argparse

repo_root = Path(__file__).parent.parent
job_template_path = repo_root / "infra" / "k8s" / "job_template.yaml"
jobs_tmp_dir = repo_root / "infra" / "k8s" / "jobs"
with job_template_path.open() as file:
    base_manifest = file.read()
base_job_name = "infra-log-consolidator-checker-test-manual"


def generate_datetime_tuples(start: str, end: str | None = None) -> list[tuple[int, int, int, int]]:
    """
    Generate a list of datetime tuples (year, month, day, hour) from start to end.

    :param start: Start date in 'YYYY-MM-DD' format.
    :param end: End date in 'YYYY-MM-DD' format. If None, only the start date will be used.
    :return: List of tuples containing (year, month, day, hour).
    """
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d') if end else start_dt

    # Ensure end_dt is always after or equal to start_dt
    if end_dt < start_dt:
        raise ValueError("End date must be after or equal to start date.")

    dt_tuples = []
    current_dt = start_dt

    while current_dt <= end_dt:
        for hour in range(24):
            dt_tuples.append((current_dt.year, current_dt.month, current_dt.day, hour))
        current_dt += timedelta(days=1)

    return dt_tuples

def get_git_commit_8chars_sha():
    return os.popen('git rev-parse HEAD').read().strip()[0:8]


def get_parser():
    parser = argparse.ArgumentParser(description="Generate Kubernetes manifests for log-consolidator-checker jobs.")
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format (fallback to environment variable START_DATE)',
        default=os.environ.get('START_DATE', None)
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYY-MM-DD format (fallback to environment variable END_DATE)',
        default=os.environ.get('END_DATE', None)
    )
    parser.add_argument(
        '--configmap-name',
        type=str,
        help='ConfigMap name (fallback to environment variable CONFIGMAP_NAME)',
        default=os.environ.get('CONFIGMAP_NAME', None)
    )
    parser.add_argument(
        '--cpu-request',
        type=str,
        help='CPU Request',
        default=os.environ.get('CPU_REQUEST', None)
    )
    parser.add_argument(
        '--retry-file',
        type=str,
        help='Textfile in "retryfile" format (or ".elie" format), i.e. lines of "YYYYMMDD\tHH"',
        default=None
    )
    return parser


def get_params():
    parser = get_parser()
    args = parser.parse_args()
    input_start_date = args.start_date
    input_end_date = args.end_date
    input_configmap_name = args.configmap_name
    input_cpu_request = args.cpu_request
    retry_file = args.retry_file
    retry_tuples = None

    if retry_file:
        if input_start_date or input_end_date:
            raise ValueError("You cannot give a retryfile and specify a date range at the same time.")
        retry_tuples = load_retryfile(retry_file)
    else:
        if not input_start_date:
            raise ValueError("Start date is required, either with --start-date or START_DATE environment variable.")
        if not input_end_date:
            print("End date is not provided, using start date as end date.")

    if not input_configmap_name:
        raise ValueError(
            "ConfigMap name is required, either with --configmap-name or CONFIGMAP_NAME environment variable.")
    if not input_cpu_request:
        raise ValueError(
            "CPU Request is required, either with --cpu-request or CPU_REQUEST environment variable.")

    print("Params:")
    print(f" - Start date: {input_start_date}")
    print(f" - End date: {input_end_date}")
    print(f" - ConfigMap name: {input_configmap_name}")
    print(f" - CPU Request: {input_cpu_request}")

    return input_start_date, input_end_date, input_configmap_name, input_cpu_request, retry_tuples


def load_retryfile(path: str) -> list[tuple[int, int, int, int]]:
    retry_contents = []
    try:
        with open(path) as f:
            retry_contents = f.readlines()
    except FileNotFoundError as e:
        raise ValueError("Problem loading retryfile: {}".format(retry_file)) from e

    results = []
    for line in retry_contents:
        t = strptime(line.strip(), '%Y%m%d\t%H')
        results.append((t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour))
    return results


def template_manifests(dts: list[tuple[int, int, int, int]], base_manifest: str, base_job_name: str,
                       input_configmap_name: str, input_cpu_request: str) -> dict[str, str]:
    manifests = {}
    for year, month, day, hour in dts:
        job_name = f"{base_job_name}-{year:04d}{month:02d}{day:02d}hour{hour:02d}"

        # Modify other fields as needed for each hour

        # Create the modified Kubernetes manifest
        modified_manifest = base_manifest.replace(base_job_name, job_name)

        # Replace {START} with ISO 8601 formatted date and hour (UTC)
        modified_manifest = modified_manifest.replace("{DATE}", f"{year:04d}{month:02d}{day:02d}")
        modified_manifest = modified_manifest.replace("{HOUR}", f"{hour:02d}")
        # Replace {CONFIGMAP_NAME} with the input configmap name
        modified_manifest = modified_manifest.replace("{CONFIGMAP_NAME}", input_configmap_name)
        modified_manifest = modified_manifest.replace("{CPU_REQUEST}", input_cpu_request)
        # Add the modified manifest to the list
        manifests[f"{year:04d}{month:02d}{day:02d}{hour:02d}"] = modified_manifest
    return manifests


def save_manifests(manifests: dict[str, str], jobs_tmp_dir: Path, *, clear_folder: bool = False) -> None:
    # Clear directory
    if clear_folder:
        shutil.rmtree(jobs_tmp_dir, ignore_errors=True)
        os.makedirs(jobs_tmp_dir)

    # Write manifests to files
    for key, manifest in manifests.items():
        with open(jobs_tmp_dir / f"job_manifest_{key}.yaml", "w") as file:
            file.write(manifest)

def main():
    input_start_date, input_end_date, input_configmap_name, input_cpu_request, retryfile_tuples = get_params()
    
    dts: list[tuple[int, int, int, int]] = generate_datetime_tuples(input_start_date, input_end_date) if not retryfile_tuples else retryfile_tuples

    print(f"Templating {len(dts)} jobs, from {dts[0]} to {dts[-1]}")
    manifests = template_manifests(dts, base_manifest, base_job_name, input_configmap_name, input_cpu_request)
    save_manifests(manifests, jobs_tmp_dir, clear_folder=True)


if __name__ == "__main__":
    main()
