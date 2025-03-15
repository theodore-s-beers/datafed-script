import os
import sys
from dataclasses import dataclass
from typing import Optional, cast

from datafed.CommandLib import API  # type: ignore
from dotenv import load_dotenv


@dataclass
class ProjectItem:
    id: str
    title: str
    owner: str
    creator: Optional[str] = None


@dataclass
class ProjectListMessage:
    item: list[ProjectItem]
    offset: int
    count: int
    total: int


# Load environment variables
load_dotenv()
USERNAME = os.getenv("DATAFED_USERNAME")
PASSWORD = os.getenv("DATAFED_PASSWORD")
PROJECT_ID = os.getenv("DATAFED_PROJECT_ID")


# Ensure environment variables are non-null
required_env_vars = {
    "DATAFED_USERNAME": USERNAME,
    "DATAFED_PASSWORD": PASSWORD,
    "DATAFED_PROJECT_ID": PROJECT_ID,
}
for k, v in required_env_vars.items():
    if not v:
        print(f"Error: Missing environment variable {k}", file=sys.stderr)
        exit(1)


# Initialize API client
api = API()


# Step 1: Authenticate
try:
    api.loginByPassword(USERNAME, PASSWORD)
    print("Successfully logged into DataFed")
except Exception as e:
    print(f"Login failed: {e}")
    exit(1)


# Step 2: Validate project ID and set context
try:
    project_list_response = api.projectList()
    project_list_message = cast(ProjectListMessage, project_list_response[0])
    project_ids = [item.id for item in project_list_message.item]

    if PROJECT_ID not in project_ids:
        raise ValueError(f"Project {PROJECT_ID} not found")
    print(f"Project {PROJECT_ID} found")

    api.setContext(PROJECT_ID)
    print("Context set")
except Exception as e:
    print(f"Error selecting project: {e}", file=sys.stderr)
    exit(1)


slide_ids: dict[str, Optional[str]] = {
    "GC101777A_40x_BF_19z": None,
    "GC101778A_40x_BF_18z": None,
    "GC101780A_40x_BF_15z": None,
    "GC101781A_40x_BF_15z": None,
    "GC101783A_40x_BF_17z": None,
    "GC101784A_40x_BF_15z": None,
    "GC101786A_40x_BF_18z": None,
    "GC101787A_40x_BF_21z": None,
    "GC101789A_40x_BF_25z": None,
    "GC101790A_40x_BF_18z": None,
}


# Step 3: Create a new data record under the selected project
try:
    for id in slide_ids.keys():
        zip_path = f"data/{id}/{id}.zip"
        if not os.path.exists(zip_path):
            raise ValueError(f"File '{zip_path}' not found")

        json_path = f"data/{id}/{id}.json"
        if not os.path.exists(json_path):
            raise ValueError(f"File '{json_path}' not found")

        response = api.dataCreate(title=id, metadata_file=json_path)
        response_message = response[0]
        print(response_message)
except Exception as e:
    print(f"Error creating data record: {e}")
    exit(1)


# # Step 6: Attach a predefined file to the data record
# file_path = "/path/to/your/file.txt"  # Change this to the actual file path

# try:
#     api.dataPut(record_id, file_path)
#     print(f"File '{file_path}' successfully attached to record {record_id}")
# except Exception as e:
#     print(f"Error attaching file: {e}")
