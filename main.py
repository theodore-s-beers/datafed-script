import os
import sys
from dataclasses import dataclass
from typing import Optional, cast

from datafed.CommandLib import API  # type: ignore
from dotenv import load_dotenv

#
# Types
#


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


@dataclass
class RecordData:
    id: str
    title: str
    repo_id: str
    size: int
    ext_auto: bool
    ct: int
    ut: int
    owner: str  # i.e., project ID
    creator: str  # i.e., user ID
    parent_id: str  # i.e., collection ID


@dataclass
class DataCreateMessage:
    data: list[RecordData]


@dataclass
class DataPutTask:
    id: str
    type: str
    status: str
    client: str
    step: int
    steps: int
    msg: str
    ct: int
    ut: int
    source: str
    dest: str


@dataclass
class DataPutMessage:
    task: list[DataPutTask]


#
# Constants
#

slides: list[str] = [
    "GC101765A_40x_BF_22z",
    "GC101767A_40x_BF_27z",
    "GC101772A_40x_BF_15z",
    "GC101775A_40x_BF_18z",
    # "GC101777A_40x_BF_19z",  # This block uploaded already
    # "GC101778A_40x_BF_18z",
    # "GC101780A_40x_BF_15z",
    # "GC101781A_40x_BF_15z",
    # "GC101783A_40x_BF_17z",
    # "GC101784A_40x_BF_15z",
    # "GC101786A_40x_BF_18z",
    # "GC101787A_40x_BF_21z",
    # "GC101789A_40x_BF_25z",
    # "GC101790A_40x_BF_18z",
    "GC101792A_40x_BF_21z",
    "GC101793A_40x_BF_22z",
    "GC101795A_40x_BF_20z",
    "GC101796A_40x_BF_17z",
    "GC101799A_40x_BF_19z",
    "GC101801A_40x_BF_21z",
    "GC101802A_40x_BF_17z",
    "GC101803A_40x_BF_17z",
    "GC101804B_40x_BF_18z",
    "GC101805A_40x_BF_13z",
    "GC101806A_40x_BF_24z",
    "GC101807A_40x_BF_27z",
]

slides_mapping: dict[str, Optional[str]] = {slide: None for slide in slides}

#
# Environment variables
#

# Load environment variables
load_dotenv()
USERNAME = os.getenv("DATAFED_USERNAME")
PASSWORD = os.getenv("DATAFED_PASSWORD")
PROJECT_ID = os.getenv("DATAFED_PROJECT_ID")
ENDPOINT_ID = os.getenv("GLOBUS_ENDPOINT_ID")

# Ensure environment variables are non-null
required_env_vars = {
    "DATAFED_USERNAME": USERNAME,
    "DATAFED_PASSWORD": PASSWORD,
    "DATAFED_PROJECT_ID": PROJECT_ID,
    "GLOBUS_ENDPOINT_ID": ENDPOINT_ID,
}
for k, v in required_env_vars.items():
    if not v:
        print(f"Error: Missing environment variable {k}", file=sys.stderr)
        exit(1)

#
# Main
#

# Initialize API client
df_api = API()

# Step 1: Authenticate
try:
    df_api.loginByPassword(USERNAME, PASSWORD)
    print("Successfully logged into DataFed")
except Exception as e:
    print(f"Login failed: {e}", file=sys.stderr)
    exit(1)

# Step 2: Validate project ID and set context
try:
    project_list_response = df_api.projectList()
    project_list_message = cast(ProjectListMessage, project_list_response[0])
    project_ids = [item.id for item in project_list_message.item]

    if PROJECT_ID not in project_ids:
        raise ValueError(f"Project {PROJECT_ID} not found")

    print(f"Project {PROJECT_ID} found")
    df_api.setContext(PROJECT_ID)
    print("Project context set")

    df_api.endpointSet(ENDPOINT_ID)
    print(f"Globus endpoint set to {ENDPOINT_ID}")
except Exception as e:
    print(f"Error selecting project: {e}", file=sys.stderr)
    exit(1)

# Step 3: Create new data records
try:
    for slide in slides_mapping.keys():
        zip_path = f"/var/gcp/data/{slide}/{slide}.zip"
        if not os.path.exists(zip_path):
            raise ValueError(f"File '{zip_path}' not found")

        json_path = f"/var/gcp/data/{slide}/{slide}.json"
        if not os.path.exists(json_path):
            raise ValueError(f"File '{json_path}' not found")

        data_create_response = df_api.dataCreate(title=slide, metadata_file=json_path)
        data_create_message = cast(DataCreateMessage, data_create_response[0])

        record_data = data_create_message.data
        assert len(record_data) == 1

        record_id = data_create_message.data[0].id
        slides_mapping[slide] = record_id
        print(f"Data record created with ID {record_id}")
except Exception as e:
    print(f"Error creating data record: {e}", file=sys.stderr)
    exit(1)

# Step 4: Attach files to data records
try:
    for slide, record in slides_mapping.items():
        if record is None:
            raise ValueError(f"No record ID found for slide {slide}")

        print(f"Attaching file {slide}.zip to record {record}")
        zip_path = f"/var/gcp/data/{slide}/{slide}.zip"
        if not os.path.exists(zip_path):
            raise ValueError(f"File '{zip_path}' not found")

        data_put_response = df_api.dataPut(data_id=record, path=zip_path, wait=True)
        data_put_message = cast(DataPutMessage, data_put_response[0])
        data_put_tasks = data_put_message.task

        assert len(data_put_tasks) == 1
        assert data_put_tasks[0].msg == "Finished"
        print(f"File transfer {data_put_tasks[0].id} complete")
except Exception as e:
    print(f"Error attaching file: {e}", file=sys.stderr)
    exit(1)
