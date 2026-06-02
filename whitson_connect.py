import decimal
import http.client
import json
import math
import os
import random
import smtplib
import subprocess
import sys
import time
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np

# Third-party library imports
import pandas as pd
import pyodbc
import requests
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


class WhitsonConnection:
    def __init__(
        self, client_name=None, client_id=None, client_secret=None, audience=None
    ):
        self.client_name = client_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def get_access_token(self, audience=None):
        """
        Get a access token for a given work session.
        """
        conn = http.client.HTTPSConnection("whitson.eu.auth0.com")
        if audience == None:
            audience = f"https://{self.client_name}.whitson.com/"
        else:
            audience = audience
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": audience,
            "grant_type": "client_credentials",
        }

        headers = {"content-type": "application/json"}
        conn.request("POST", "/oauth/token", json.dumps(payload), headers)
        res = conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8")).get("access_token")

    def _read_token_from_file(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                try:
                    data = json.load(file)
                    return (
                        data.get("client_id"),
                        data.get("access_token"),
                        data.get("timestamp"),
                    )
                except json.JSONDecodeError:
                    return None, None, None
        else:
            return None, None, None

    def _write_token_to_file(self, file_path, token, timestamp):
        data = {
            "client_id": self.client_id,
            "access_token": token,
            "timestamp": timestamp,
        }
        with open(file_path, "w") as file:
            json.dump(data, file)

    def get_access_token_smart(self, audience=None):
        """
        Get an access token for a given work session.
        Does not request a new one if the previous token has been requested within the last 24 hrs.
        """
        token_file_path = "access_token.txt"
        stored_client_id, stored_token, stored_timestamp = self._read_token_from_file(
            token_file_path
        )

        current_time = time.time()

        # Check if a token was previously requested within the last 24 hours
        if (
            stored_client_id == self.client_id
            and stored_token
            and (current_time - stored_timestamp) < (24 * 60 * 60)
        ):
            return stored_token
        else:
            if audience == None:
                audience = f"https://{self.client_name}.whitson.com/"
            else:
                audience = audience
            conn = http.client.HTTPSConnection("whitson.eu.auth0.com")
            payload = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "audience": audience,
                "grant_type": "client_credentials",
            }

            headers = {"content-type": "application/json"}
            conn.request("POST", "/oauth/token", json.dumps(payload), headers)
            res = conn.getresponse()
            data = res.read()
            new_token = json.loads(data.decode("utf-8")).get("access_token")

            # Update the stored token and timestamp
            self._write_token_to_file(token_file_path, new_token, current_time)

            return new_token

    def get_valid_or_default(self, value: float, default: float = None) -> float:
        """
        Returns the given value if it is not NaN; otherwise, returns the specified default value.

        Args:
            value (float): The value to be checked for NaN.
            default (float, optional): The value to return if the input is NaN. Defaults to None.

        Returns:
            float: The original value if it is not NaN, otherwise the default value.
        """
        return value if not pd.isna(value) else default

    def get_well_id_by_uwi_api(self, wells: list[dict], uwi_api: str):
        """
        Get the well_id of a given uwi_api.

        lookup_key is the database value in whitson+ where the uwi_api is stored, typically uwi_api or external_id.

        """
        return next(
            (well["id"] for well in wells if well.get("uwi_api") == uwi_api),
            None,  # Default value if no match is found
        )

    def get_well_id_by_propnum(
        self, wells: list[dict], propnum: str, lookup_key: str = "uwi_api"
    ):
        """
        Get the well_id of a given propnum.

        lookup_key is the database value in whitson+ where the propnum is stored, typically uwi_api or external_id.

        """
        return next(
            (well["id"] for well in wells if well.get(lookup_key) == propnum),
            None,  # Default value if no match is found
        )

    def get_well_id_by_wellname(self, wells: list[dict], wellname: str):
        """
        Get the well_id of a given wellname.
        """
        return next(
            (well["id"] for well in wells if well.get("name") == wellname),
            None,  # Default value if no match is found
        )

    def get_fields(self):
        """
        Get all fields on domain.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/"
        response = requests.get(
            base_url + "fields",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res:
            raise Exception("no existing fields")
        return res

    def get_wells(self, project_id: int):
        """
        Get a list of wells in a project.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={
                "project_id": project_id,
            },
        )
        res = response.json()
        if not res:
            return []
        return res

    def get_well_from_well_id(self, well_id: int):
        """
        Get the well info, given a well ID.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={
                "well_id": well_id,
            },
        )
        res = response.json()
        if not res:
            return []
        return res

    def get_wells_from_projects(self, project_ids: list[int], page_size: int = 1000):
        """
        Get a list of wells from projects with project_id given in list.

        Example: whitson_wells = whitson_connection.get_wells_from_project([1, 2, 3])

        Lower the page size if 502 Error
        """
        all_wells = []
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells_paginated"
        )

        for project_id in project_ids:
            page = 1  # Start with the first page

            while True:
                response = requests.get(
                    base_url,
                    headers={
                        "content-type": "application/json",
                        "Authorization": f"Bearer {self.access_token}",
                    },
                    params={
                        "project_id": project_id,
                        "page": page,
                        "page_size": page_size,  # Lower this if Error 502
                    },
                )
                res = response.json()

                retries = 0
                while response.status_code >= 500 and retries < 3:
                    retries += 1
                    print(f"Error occured Error {response.status_code} encountered.")
                    print(response.text)
                    print(f"Retrying {retries}")
                    response = requests.get(
                        base_url,
                        headers={
                            "content-type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                        },
                        params={
                            "project_id": project_id,
                            "page": page,
                            "page_size": page_size,  # Lower this if Error 502
                        },
                    )
                    res = response.json()

                if response.status_code >= 400:
                    print(f"Error occured Error {response.status_code} encountered.")
                    print(response.text)
                    break

                if (
                    not res or res == []
                ):  # If the response is empty, there are no more wells for this project
                    break

                all_wells.extend(
                    res
                )  # Append the wells from this page to the list of all wells
                page += 1  # Move to the next page

        return all_wells

    def get_wells_from_projects_old(
        self, project_ids: list[int], page_size: int = 1000
    ):
        """
        Get a list of wells from projects with project_id given in list.

        Example: whitson_wells = whitson_connection.get_wells_from_project([1, 2, 3])

        Lower the page size if 502 Error
        """
        all_wells = []
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/old_wells_paginated"

        for project_id in project_ids:
            page = 1  # Start with the first page

            while True:
                response = requests.get(
                    base_url,
                    headers={
                        "content-type": "application/json",
                        "Authorization": f"Bearer {self.access_token}",
                    },
                    params={
                        "project_id": project_id,
                        "page": page,
                        "page_size": page_size,  # Lower this if Error 502
                    },
                )
                res = response.json()

                retries = 0
                while response.status_code >= 500 and retries < 3:
                    retries += 1
                    print(f"Error occured Error {response.status_code} encountered.")
                    print(response.text)
                    print(f"Retrying {retries}")
                    response = requests.get(
                        base_url,
                        headers={
                            "content-type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                        },
                        params={
                            "project_id": project_id,
                            "page": page,
                            "page_size": page_size,  # Lower this if Error 502
                        },
                    )
                    res = response.json()

                if response.status_code >= 400:
                    print("Internal Server Error (500) encountered.")
                    break

                if (
                    not res or res == []
                ):  # If the response is empty, there are no more wells for this project
                    break

                all_wells.extend(
                    res
                )  # Append the wells from this page to the list of all wells
                page += 1  # Move to the next page

        return all_wells

    def copy_wells(self, payload: dict):
        """
        Copy multiple wells to and existing project

        Example payload:
        ----------
        >>>
            payload = {
                "project_id": 0,
                "name_suffix": "string",
                "uwi_suffix": "string",
                "well_ids": [
                    0
                ],
                "linked_production_data": true,
                "linked_bhp_data": true
            }
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.copy_wells(payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/post_api_external_v1_wells_copy_wells
        """
        base_url = (
            f"http://{self.client_name}.whitson.com/api-external/v1/wells/copy_wells"
        )
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print("Success copy wells")
        else:
            print(response.text)

    def get_well_id_name_external_id_uwi_api(self, well_ids: list[int]):
        """
        Get a list of projects in field.
        """
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/data_fields"
        )
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={
                "well_ids": well_ids,
                "data_fields": ["id", "name", "uwi_api", "external_id"],
            },
        )
        return response.json()

    def get_external_id_dict_from_project(
        self,
        project_ids: list[int],
        page_size: int = 3000,
        remove_substring: str = None,
    ) -> dict[str, int]:
        """
        Retrieves a dictionary mapping well external IDs to their whitson+ IDs
        for wells in the specified projects.

        Args:
            project_ids (list[int]): A list of whitson+ project IDs to retrieve wells from.
            page_size (int, optional): The number of wells to fetch per request. Defaults to 3000.
            remove_substring (str, optional): A substring to remove from each
                                          `external_id` if present. Defaults to None.

        Returns:
            dict[str, int]: A dictionary where the keys are the well external IDs
                            (non-None) and the values are the corresponding
                            internal IDs (non-None).

        Raises:
            ValueError: If the `project_ids` list is empty.
        """
        if not project_ids:
            raise ValueError("The 'project_ids' list cannot be empty.")

        wells = self.get_wells_from_projects(project_ids, page_size)

        return {
            (
                item["external_id"].replace(remove_substring, "")
                if remove_substring
                else item["external_id"]
            ): item["id"]
            for item in wells
            if item["external_id"] is not None and item["id"] is not None
        }

    def get_wells_and_scenarios_from_projects(
        self, project_ids: list[int], page_size: int = 1000
    ):
        """
        Get 2 lists - one for all wells, one for all scenarios (except those created by @whitson.com users)
        - from multiple projects with each project_id given in list.

        Example payload:
        project_ids = [1,2,3]

        Example function call:
        whitson_wells = whitson_connection.get_wells_and_scenarios_from_projects(project_ids)

        Lower the page size if 502 Error
        """
        all_wells = []

        all_scenarios = []
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells_paginated"
        )

        # print("Collecting Wells for Projects - ", project_ids)
        for project_id in project_ids:
            page = 1  # Start with the first page

            while True:
                response = requests.get(
                    base_url,
                    headers={
                        "content-type": "application/json",
                        "Authorization": f"Bearer {self.access_token}",
                    },
                    params={
                        "project_id": project_id,
                        "page": page,
                        "page_size": page_size,  # Lower this if Error 502
                    },
                )
                res = response.json()

                if response.status_code >= 400:
                    print(f"Error occured Error {response.status_code} encountered.")
                    print(response.text)
                    raise

                if (
                    not res or res == []
                ):  # If the response is empty, there are no more wells for this project
                    break

                all_wells.extend(
                    res
                )  # Append the wells from this page to the list of all wells
                page += 1  # Move to the next page

            # print("Found - ", len(all_wells), "wells in total for all projects", project_ids)

            # Collecting Scenarios for Project
            # print("Collecting Scenarios for Project - ", project_id)
            base_url_scenario = (
                f"https://{self.client_name}.whitson.com/api-external/v1/scenario"
            )
            response = requests.get(
                base_url_scenario,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                params={
                    "project_id": project_id,
                },
            )

            res = response.json()

            # keeping only external scenarios
            external_scenarios = []
            # print("Found ", len(res), " scnenarios in project id - ", project_id)
            for i, scenario in enumerate(res.copy()):
                if "@whitson.com" not in scenario["owner"]:
                    scenario["project_id"] = project_id
                    scenario["name"] = next(
                        (
                            well["name"]
                            for well in all_wells
                            if well.get("id") == scenario["main_well_id"]
                        ),
                        None,
                    )
                    scenario["id"] = scenario[
                        "scenario_id"
                    ]  # Consistent with get wells
                    external_scenarios.append(scenario)
            # print("Found ", len(external_scenarios)," external scenarios in project", project_id)
            all_scenarios.extend(external_scenarios)  # keeping only external scenarios

        return all_wells, all_scenarios

    def get_projects(self, field_id: int):
        """
        Get a list of projects in field.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/fields/{field_id}/projects"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res:
            raise Exception("no existing wells")
        return res

    def get_well_groups(self, project_id):
        """
        Get all group tags in a project.

        Example function call:
        ----------
        well_group_list = whitson_connection.get_well_groups(project_id)

        Return:
        ----------
        [
            {
                "group_name": "string",
                "well_ids": [0],
                "company_wide": true,
                "owner": "string"
            }
        ]

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/get_api_external_v1_projects__project_id__well_groups
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/projects/{project_id}/well_groups"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully get well groups in project ID {project_id}")
        else:
            print(response.text)
        return res

    def create_well_groups(self, project_id, payload: dict) -> requests.Response:
        """
        Create a new group tag for wells in a project.

        Example function call:
        ----------
        response = whitson_connection.create_well_groups(project_id, payload)

        Payload Example:
        ----------
        payload = {
            "group_name": "string",
            "well_ids": [0],
            "company_wide": true,
            "owner": "string"
        }

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/post_api_external_v1_projects__project_id__well_groups
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/projects/{project_id}/well_groups"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully created well groups in project ID {project_id}")
        else:
            print(response.text)
        return response

    def edit_well_groups(self, project_id, payload: dict) -> requests.Response:
        """
        Edit group tag configuration in a project.

        Example function call:
        ----------
        response = whitson_connection.edit_well_groups(project_id, payload)

        Payload Example:
        ----------
        payload = {
            "group_name": "string",
            "well_ids": [0],
            "company_wide": true,
            "owner": "string"
        }

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/put_api_external_v1_projects__project_id__well_groups
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/projects/{project_id}/well_groups"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully edited well groups in project ID {project_id}")
        else:
            print(response.text)
        return response

    def delete_well_groups(self, project_id, payload: dict) -> requests.Response:
        """
        Delete group tag in a project.

        Example function call:
        ----------
        response = whitson_connection.delete_well_groups(project_id, payload)


        Payload Example:
        ----------
        payload = {
            "group_name": "string"
        }

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/delete_api_external_v1_projects__project_id__well_groups
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/projects/{project_id}/well_groups"
        response = requests.delete(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully deleted well groups {payload['group_name']}")
        else:
            print(response.text)
        return response

    def create_well(
        self, payload: dict, add_default_wellbore=True
    ) -> requests.Response:
        """
        Create a new well on a domain.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
            params={"add_default_wellbore": add_default_wellbore},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully created well {payload['name']}")
        else:
            print(response.text)
        return response

    def delete_well(self, well_id: int) -> requests.Response:
        """
        Delete a well based on well id.
        """
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}"
        )
        response = requests.delete(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully deleted well id {well_id}")
        else:
            print(response.text)
        return response

    def create_well_batch(
        self, payload: list, batch_size=100, add_default_wellbore=True
    ) -> requests.Response:
        """
        Create new wells on a domain in batches
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bulk"

        for i in range(0, len(payload), batch_size):
            response = requests.post(
                base_url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json=payload[i : i + batch_size],
                params={"add_default_wellbore": add_default_wellbore},
            )

            if 200 <= response.status_code < 300:
                print(f"Successfully created batch {i // batch_size + 1}.")
            else:
                print(f"Error in batch {i // batch_size + 1}: {response.text}")

    def edit_well_info(self, payload: dict) -> requests.Response:
        """
        Edit well info for one or more wells at the same time.

        Example payload:
        well_info = [{'id': 10, 'l_w': 5000}, {'id': 11, 'l_w': 10000}]

        Example function call:
        whitson_connection.edit_well_info(well_info)

        More info about endpoint here: https://internal.whitson.com/api-external/swagger/#/Base%20Data/patch_api_external_v1_wells
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited well(s).")
        else:
            print(response.text)
        return response

    def edit_well_info_batch(self, payload: list, batch_size=1000) -> list:
        """
        Edit well info for one or more wells in batches.

        This function splits the payload into batches of 5000 wells at a time.

        Example payload:
        well_info = [{'id': 10, 'l_w': 5000}, {'id': 11, 'l_w': 10000}, ...]

        Example function call:
        whitson_connection.edit_well_info(well_info)

        More info about endpoint here:
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/patch_api_external_v1_wells
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells"
        responses = []

        for i in range(0, len(payload), batch_size):
            batch_payload = payload[i : i + batch_size]
            response = requests.patch(
                base_url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json=batch_payload,
            )
            if 200 <= response.status_code < 300:
                print(f"Successfully edited batch {i // batch_size + 1}.")
            else:
                print(f"Error in batch {i // batch_size + 1}: {response.text}")
            responses.append(response)

        return responses

    def edit_well_info_by_id(self, well_id: int, payload: dict) -> requests.Response:
        """
        Edit well info for one well_id at the time.
        """
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}"
        )
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited well(s).")
        else:
            print(response.text)
        return response

    def edit_well_info_in_chunks(self, wells_to_edit_payload_list, chunk_size=5000):
        """
        Process the wells_to_edit_payload_list in chunks and call edit_well_info for each chunk.

        Args:
            wells_to_edit_payload_list (list): The list of payloads to be processed.
            chunk_size (int): The size of each chunk. Default is 5000.
        """
        total_rows = len(wells_to_edit_payload_list)
        chunks = -(
            -total_rows // chunk_size
        )  # Alternative to math.ceil for positive numbers

        for i in range(chunks):
            start_index = i * chunk_size
            end_index = start_index + chunk_size

            # Extract the chunk
            payload_chunk = wells_to_edit_payload_list[start_index:end_index]

            # Call the function for the current chunk
            self.edit_well_info(payload=payload_chunk)

            print(f"Processed chunk {i + 1} of {chunks} with {len(payload_chunk)} rows")

    def create_project(self, field_id: int, payload: dict) -> requests.Response:
        """
        Create a new project on a domain.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/fields/{field_id}/projects"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully created project {payload['name']}")
        else:
            print(response.text)
        return response

    def upload_production_to_well(
        self,
        well_id: int,
        payload: list[dict],
        append_only: bool = False,
    ) -> requests.Response:
        """
        Upload production data to well.

        Parameters:
        well_id (int): The ID of the well to update.
        payload (list[dict]): A list of dictionaries containing the production data.
        append_only (bool): Determines the behavior for handling existing data.
            - False: Replaces existing data for matching dates with payload data. For a given matching date, the entire dataset will be replaced with the payload data (not merged). Appends new data if the date does not exist. Does not affect old data not in the payload.
            - True: Appends new data if the date does not exist. Rejects payload data if the date exists. Does not affect old data not in the payload.

        Returns:
        requests.Response: The response from the API after attempting to upload the production data.
        """
        response = requests.post(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/production_data",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
            params={"append_only": append_only},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully updated production data on well {well_id}")
        else:
            print(response.text)
        return response

    def insert_production_to_well(
        self,
        well_id: int,
        payload: list[dict],
    ) -> requests.Response:
        """
        Inserts partial production data into an existing well record.
        Only the fields included in the payload will be updated; all other data remains unchanged.

        Parameters:
        well_id (int): The ID of the well to update.
        payload (list[dict]): A list of dictionaries containing the production data.

        Returns:
        requests.Response: The response from the API after attempting to update the production data.
        """
        prod_payload = {"production_data": payload}
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/production_data",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=prod_payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully updated production data on well {well_id}")
        else:
            print(response.text)
        return response

    def bulk_upload_production_to_well(
        self, payload: list[dict], append_only=False
    ) -> requests.Response:
        """
        Uploads production data to a specified well in bulk.

        This function sends a POST request to the Whitson API to upload a list of production data records associated with a well.
        Each production data entry must include the `well_id` and the `date` of the production.

        Parameters:
        - payload (list[dict]): A list of dictionaries containing production data entries.
        Each dictionary should have the following keys:
            - well_id (int): The ID of the well that holds the production record. (required)
            - date (str): The date of the production in ISO 8601 format (e.g., "2024-09-14T21:07:08.556Z"). (required)
            - Additional optional fields may include:
            - qo_sc, qw_sc, qg_sc, qo_se, qw_se, qg_se, qo_sep, qw_sep, qg_sep, p_sep, t_sep, p_wf_measured,
                p_tubing, p_casing, p_gas_lift, liquid_level, choke_size, line_pressure, etc., as defined by the API.

        Returns:
        - requests.Response: The response object from the POST request.
        - If the request is successful (status code 200-299), "success" is printed.
        - If the request fails, the error response text is printed.

        Example:
        >>> payload = [
                {
                    "well_id": 123,
                    "date": "2024-09-14T21:07:08.556Z",
                    "qo_sc": 100.0,
                    "qw_sc": 200.0,
                    # Additional production data fields...
                }
            ]
        >>> response = bulk_upload_production_to_well(payload)
        >>> if response.status_code == 200:
                print("Production data uploaded successfully.")
            else:
                print("Failed to upload production data.")
        """
        response = None
        i = 0

        while response is None or (response.status_code == 400 and i <= 2):
            i += 1
            response = requests.post(
                f"https://{self.client_name}.whitson.com/api-external/v1/wells/production_data",
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json=payload,
                params={"append_only": append_only},
            )
            if response.status_code >= 200 and response.status_code < 300:
                print("success")
                break
            elif i <= 2 and response.status_code == 400:
                print(response.text)
                print(f"Retrying {i}...")
            else:
                print(response.text)
        return response

    def bulk_upload_monthly_production_to_well(
        self, payload: list[dict]
    ) -> requests.Response:
        """
        Uploads monthly supplemental production data to a specified well in bulk.

        This function sends a POST request to the Whitson API to upload a list of monthly production data records associated with a well.
        Each production data entry must include the `well_id` and the `date` of the production.

        Parameters:
        - payload (list[dict]): A list of dictionaries containing production data entries.
        Each dictionary should have the following keys:
            - well_id (int): The ID of the well that holds the production record. (required)
            - date (str): The date of the production in ISO 8601 format (e.g., "2024-09-14T21:07:08.556Z"). (required)
            - Additional optional fields may include:
            - qo_sc, qw_sc, qg_sc, days_on as defined by the API.

        Returns:
        - requests.Response: The response object from the POST request.
        - If the request is successful (status code 200-299), "success" is printed.
        - If the request fails, the error response text is printed.

        Example:
        >>> payload = [
                    {
                        "well_id": 0,
                        "date": "2025-08-13T15:41:04.480Z",
                        "qo_sc": null,
                        "qg_sc": null,
                        "qw_sc": null,
                        "days_on": 16
                    }
                ]
        >>> response = bulk_upload_monthly_production_to_well(payload)
        >>> if response.status_code == 201:
                print("Production data uploaded successfully.")
            else:
                print("Failed to upload production data.")
        """
        response = None
        i = 0

        while response is None or (response.status_code == 400 and i <= 2):
            i += 1
            response = requests.post(
                f"https://{self.client_name}.whitson.com/api-external/v1/wells/monthly_production_data",
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json=payload,
            )
            if response.status_code >= 200 and response.status_code < 300:
                print("success")
                break
            elif i <= 2 and response.status_code == 400:
                print(response.text)
                print(f"Retrying {i}...")
            else:
                print(response.text)
        return response

    def bulk_insert_production_to_well(self, payload: list[dict]) -> requests.Response:
        """
        Inserts partial and Edits production data into an existing well record in bulk.

        This function sends a PUT request to the Whitson API to upload a list of production data records.
        Each production data entry must include the `well_id` and the `date` of the production.

        Parameters:
        - payload (list[dict]): A list of dictionaries containing production data entries.
        Each dictionary should have the following keys:
            - well_id (int): The ID of the well that holds the production record. (required)
            - date (str): The date of the production in ISO 8601 format (e.g., "2024-09-14T21:07:08.556Z"). (required)
            - Additional optional fields may include:
            - qo_sc, qw_sc, qg_sc, qo_se, qw_se, qg_se, qo_sep, qw_sep, qg_sep, p_sep, t_sep, p_wf_measured,
                p_tubing, p_casing, p_gas_lift, liquid_level, choke_size, line_pressure, etc., as defined by the API.

        Returns:
        - requests.Response: The response object from the POST request.
        - If the request is successful (status code 200-299), "success" is printed.
        - If the request fails, the error response text is printed.

        Example:
        >>> payload = [
                {
                    "well_id": 123,
                    "date": "2024-09-14T21:07:08.556Z",
                    "qo_sc": 100.0,
                    "qw_sc": 200.0,
                    # Additional production data fields...
                }
            ]
        >>> response = bulk_insert_production_to_well(payload)
        """
        response = None
        i = 0

        while response is None or (response.status_code == 400 and i <= 2):
            i += 1
            response = requests.put(
                f"https://{self.client_name}.whitson.com/api-external/v1/wells/production_data",
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json={"production_data": payload},
            )
            if response.status_code >= 200 and response.status_code < 300:
                print("success")
                break
            elif i <= 2 and response.status_code == 400:
                print(response.text)
                print(f"Retrying {i}...")
            else:
                print(response.text)
        return response

    def convert_dataframe_to_prod_payload(
        self,
        df,
        columns_to_drop: list[str] = None,
        convert_to_float: bool = False,
        clip_pressure: bool = True,
    ) -> list[dict]:
        """
        Converts a DataFrame into a production data payload suitable for the Whitson+ API.

        This function processes a DataFrame by dropping specified columns, removing rows where the well ID is not found,
        and converting any NaN values to None to ensure JSON compatibility. The 'date' column is formatted to ISO 8601 format.

        **Assumptions:**
        - The DataFrame is expected to contain the columns as shown in the provided schema, including:
        'well_id', 'date', 'qo_sc', 'qg_sc', 'qw_sc', 'qo_sep', 'qg_sep', 'qw_sep', 'p_sep', 't_sep',
        'p_wf_measured', 'p_tubing', 'p_casing', 'qg_gas_lift', 'liquid_level', 'choke_size', 'line_pressure'.
        - The 'date' column must be present and contain date values that can be converted to ISO 8601 format.
        - The DataFrame is assumed to be in the format shown in the provided image, where certain values may be NaN.

        Parameters:
        - df (pd.DataFrame): The input DataFrame containing production data.
        - columns_to_drop (list[str]): A list of column names to be dropped from the DataFrame.

        Returns:
        - list[dict]: A list of dictionaries where each dictionary represents a row of the production data payload
        formatted for the Whitson+ API.

        Example:
        >>> payload = convert_dataframe_to_prod_payload(df, ['insert_date'])
        >>> print(payload)
        [
            {
                "well_id": 0,
                "date": "2024-09-14T21:07:08.556Z",
                "qo_sc": None,
                "qg_sc": None,
                "qw_sc": None,
                "qo_sep": None,
                "qg_sep": None,
                "qw_sep": None,
                "p_sep": None,
                "t_sep": None,
                "p_wf_measured": None,
                "p_tubing": None,
                "p_casing": None,
                "qg_gas_lift": None,
                "liquid_level": None,
                "choke_size": None,
                "line_pressure": None
            }
        ]
        """
        # Drop the specified columns from the DataFrame
        if columns_to_drop != None:
            df = df.drop(columns=columns_to_drop)

        # Record the original number of rows
        original_row_count = len(df)

        # Drop rows where 'well_id' is NaN after mapping
        df = df.dropna(subset=["well_id"])

        # Record the new number of rows
        new_row_count = len(df)

        # Print a message if rows were removed
        if original_row_count > new_row_count:
            print(
                f"Removed {original_row_count - new_row_count} rows where UWI was not found in the dictionary."
            )

        # Convert the 'date' column to ISO 8601 format
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        if convert_to_float:
            columns_to_convert = [
                "qo_sc",
                "qw_sc",
                "qg_sc",
                "qo_sep",
                "qg_sep",
                "qw_sep",
                "p_sep",
                "t_sep",
                "p_wf_measured",
                "p_tubing",
                "p_casing",
                "qg_gas_lift",
                "liquid_level",
                "choke_size",
                "line_pressure",
                "qsand",
            ]

            # Retain only the columns that exist in the dataframe
            columns_to_convert = df.columns.intersection(columns_to_convert)

            # Convert the specified columns to float
            df[columns_to_convert] = df[columns_to_convert].astype(float)

            # Set pressure columns to 14.7 if they are less than or equal to 14.7
            pressure_columns = [
                "p_sep",
                "p_wf_measured",
                "p_tubing",
                "p_casing",
                "line_pressure",
            ]
            if clip_pressure:
                pressure_columns = df.columns.intersection(pressure_columns)
                df[pressure_columns] = df[pressure_columns].clip(lower=14.7)

            # Set other specified columns to 0 if they are less than 0
            non_negative_columns = [
                "qo_sc",
                "qw_sc",
                "qg_sc",
                "qo_sep",
                "qg_sep",
                "qw_sep",
                "t_sep",
                "qg_gas_lift",
                "liquid_level",
                "choke_size",
                "qsand",
            ]
            non_negative_columns = [
                col for col in non_negative_columns if col in df.columns
            ]
            df[non_negative_columns] = df[non_negative_columns].clip(lower=0)

        # Replace NaN values with None for JSON compatibility
        df = df.replace({np.nan: None})

        # # Remove rows where all columns except 'date' are NaN
        # df = df.dropna(subset=df.columns.difference(['date']), how='all')

        # df.to_excel(f'example-{counter}.xlsx')

        # Record the original number of rows
        original_row_count = len(df)

        # Remove duplicate dates
        df_unique = df.drop_duplicates(subset=["well_id", "date"])

        # Record the new number of rows
        new_row_count = len(df_unique)

        # Print a message if rows were removed
        if original_row_count > new_row_count:
            print(
                f"Removed {original_row_count - new_row_count} rows where duplicate dates were found."
            )

        # Convert the DataFrame to a list of dictionaries for API upload
        return df_unique.to_dict("records")

    def get_production(
        self, well_id: int, from_date=None, end_date=None
    ) -> requests.Response:
        """
        Get production data.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/production_data",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"from_date": from_date, "end_date": end_date},
        )
        res = response.json()
        # if not res and response.status_code < 300:
        #     raise Exception("no production data")
        if response.status_code >= 400:
            raise Exception("Error fetching data")
        return res

    def get_prod_from_projects(
        self,
        project_ids: list[int],
        from_date: str = None,
        page_size: int = 1000,
        end_date: str = None,
    ) -> requests.Response:
        """
        Get production data in bulk using project_id
        """
        all_wells = []
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/production_data"
        for project_id in project_ids:
            page = 1  # Start with the first page
            while True:
                try:
                    response = requests.get(
                        base_url,
                        headers={
                            "content-type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                        },
                        params={
                            "project_id": project_id,
                            "page": page,
                            "from_date": from_date,
                            "page_size": page_size,  # Lower this if Error 502
                            "end_date": end_date,
                        },
                    )
                    res = response.json()
                    if (
                        not res
                    ):  # If the response is empty, there are no more wells for this project
                        break
                    all_wells.extend(
                        res
                    )  # Append the wells from this page to the list of all wells
                except:
                    print("Something went wrong")
                page += 1  # Move to the next page
                print(page)
        return all_wells

    def delete_prod_data(self, well_id: int | None = None):
        """
        Delete production to well.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/production_data"
        response = requests.delete(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully deleted production data for well {well_id}")
        else:
            print(response.text)
        return response

    def delete_prod_data_bulk(self, payload: list[dict]):
        """
        Delete production in bulk.

        Parameters:
        - payload (list[dict]): A list of dictionaries containing well_id and date entries.
        Each dictionary should have the following keys:
            - well_id (int): The ID of the well that holds the production record. (required)
            - date (str): The date of the production in ISO 8601 format (e.g., "2024-09-14T21:07:08.556Z"). (required)

        Returns:
        - requests.Response: The response object from the POST request.
        - If the request is successful (status code 200-299), "success" is printed.
        - If the request fails, the error response text is printed.

        Example:
        >>> payload =   [
                    {
                        "well_id": 1,
                        "date": "2021-01-01T00:00:00"
                    }
                ]
        >>> response = delete_prod_data_bulkl(payload)
        >>> if response.status_code == 204:
                print("Production data deleted successfully.")
            else:
                print("Failed to delete production data.")
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/production_data"
        response = requests.delete(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully deleted production data")
        else:
            print(response.text)
        return response

    def delete_prod_data_between_dates(
        self, well_id: int, start_date: str, end_date: str
    ):
        """
        Deletes production data for a specified well within a given date range.

        This function sends a DELETE request to the specified API endpoint to remove
        production data associated with a well for the given start and end dates.
        If the deletion is successful, a success message is printed; otherwise,
        the response's error message is printed.

        Args:
            well_id (int): The unique identifier for the well.
            start_date (str): The start date for the range of data to delete, formatted as 'YYYY-MM-DD'.
            end_date (str): The end date for the range of data to delete, formatted as 'YYYY-MM-DD'.

        Returns:
            requests.Response: The response object returned by the DELETE request.
                This can be used for further inspection of the request's result.

        Raises:
            ValueError: If the response status code indicates failure, an error message
            will be printed detailing the issue.

        Example:
            delete_prod_data_between_dates(well_id=123, start_date="2023-01-01", end_date="2023-01-31")
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/production_data"
        response = requests.delete(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"start_date": start_date, "end_date": end_date},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully deleted production data for well {well_id}")
        else:
            print(response.text)
        return response

    def get_well_deviation_data(self, well_id: int) -> requests.Response:
        """
        Get well deviation data of a well in the database.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input/well_deviation_survey"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res and response.status_code != 200:
            raise Exception("Something went wrong")
        return res

    def get_max_md_well_deviation_data(self, well_id: int) -> requests.Response:
        """
        Get the max md of a well deviation survey.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input/well_deviation_survey"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()

        if not res:
            raise Exception("Something went weong")
        else:
            if not res or not all("md" in item for item in res):
                return None
            else:
                return max(item["md"] for item in res)

    def clean_early_prod(self, payload: dict) -> requests.Response:
        """
        Clean non-producing data on early date
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/clean_early_production"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"successfully clean early production data for {len(payload['well_ids'])} wells"
            )
        else:
            print(response.text)
        return response

    # ---------------------------------------------------------------------------------------------------------
    # PVT Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def get_eos(self, field_id: int) -> requests.Response:
        """
        Get EOS type that used in certain field by field_id

        Example function call:
        ----------
        whitson_wells = whitson_connection.get_eos(field_id)

        Example return:
        ----------
        >>>
            return = [
                {
                    "id": 0,
                    "name": "string"
                }
            ]
        <<<


        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/get_api_external_v1_fields__field_id__eos
        """

        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/fields/{field_id}/eos",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully get eos for field {field_id}")
        else:
            print(response.text)
        return response.json()

    def get_surface_processes(self) -> requests.Response:
        """
        Get all processes on domain.

        Example function call:
        ----------
        whitson_wells = whitson_connection.get_surface_processes()

        Example return:
        ----------
        >>>
            return = [
                {
                        "id": 0,
                        "name": "string",
                        "owner": "string",
                        "company_wide": true,
                        "date_updated": "2026-04-27T06:43:25.076Z",
                        "updated_by": "string",
                        "stages": [
                        {
                            "id": 0,
                            "process_id": 0,
                            "stage_index": 0,
                            "pressure": 0,
                            "temperature": 0
                        }
                    ]
                }
            ]
        <<<

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/get_api_external_v1_processes

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/processes"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res:
            raise Exception("no existing processes")
        return res

    def create_surface_processes(self, payload: dict) -> requests.Response:
        """
        Upload new processes on domain.

        Example payload:
        ----------
        >>>
            payload = {
                "name": "string",
                "company_wide": true,
                "stages": [
                    {
                    "stage_index": 0,
                    "pressure": 0,
                    "temperature": 0
                    }
                ]
            }
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.create_surface_processes(payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/post_api_external_v1_processes

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/processes"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully upload new processes.")
        else:
            print(response.text)
        return response

    def edit_surface_processes(
        self, process_id: int, payload: dict
    ) -> requests.Response:
        """
        Edit the process with id <process_id> in the database.

        Example payload:
        ----------
        >>>
            payload = {
                "name": "string",
                "company_wide": true,
                "stages": [
                    {
                    "stage_index": 0,
                    "pressure": 0,
                    "temperature": 0
                    }
                ]
            }
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.edit_surface_processes(process_id, payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/put_api_external_v1_processes__process_id_

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/processes/{process_id}"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edit processes {process_id}.")
        else:
            print(response.text)
        return response

    def delete_surface_processes(self, process_id: int) -> requests.Response:
        """
        Delete the process with id <process_id> in the database.

        Example function call:
        ----------
        whitson_wells = whitson_connection.delete_surface_processes(process_id, payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/delete_api_external_v1_processes__process_id_

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/processes/{process_id}"
        response = requests.delete(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully delete processes {process_id}.")
        else:
            print(response.text)
        return response

    def get_pvt_process(self, well_id: int) -> requests.Response:
        """
        Get PVT processes for a well.

        Example function call:
        ----------
        whitson_wells = whitson_connection.get_pvt_processes()

        Example return:
        ----------
        >>>
            return = {
                "process_id": 0,
                "process_specific": {
                    "stages": [
                        {
                            "stage_index": 0,
                            "pressure": 0,
                            "temperature": 0
                        }
                    ]
                }
            }
        <<<

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/get_api_external_v1_wells__well_id__processes

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/processes"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res:
            raise Exception("no pvt processes existed")
        return res

    def edit_pvt_process(self, well_id: int, payload: dict) -> requests.Response:
        """
        Edit PVT processes for a well.

        Example payload:
        ----------
        >>>
            payload = {
                "process_id": 0,
                "process_specific": {
                    "stages": [
                        {
                            "stage_index": 0,
                            "pressure": 0,
                            "temperature": 0
                        }
                    ]
                }
            }
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.edit_pvt_processes()

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/put_api_external_v1_wells__well_id__processes

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/processes"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited PVT process for well {well_id}")
        else:
            print(response.text)
        return response

    def get_pvt_simple_dry_gas_input(self, well_id: int) -> requests.Response:
        """
        Get PVT simplified dry gas input of a well.

        Example function call:
        ----------
        whitson_wells = whitson_connection.get_pvt_simple_dry_gas_input(well_id)

        Example return:
        ----------
        >>>
            return = {
                "well_id": 0,
                "input_type": "gor",
                "date_updated": "2026-04-27T07:35:07.776Z",
                "updated_by": "string",
                "note": "string",
                "gor": 0,
                "api": 0,
                "psat": 0,
                "use_gor_sep": true,
                "set_non_hc": true,
                "z_n2": 0,
                "z_co2": 0,
                "z_h2s": 0,
                "process": {},
                "yi": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "mw": 0,
                "sg": 0,
                "mw_c7p_oil": 0,
                "mw_c7p_gas": 0,
                "mw_c7p_flashed_oil": 0,
                "flashed_oil_density": 0,
                "flash_temperature": 0,
                "use_api": true,
                "gor_sep": 0,
                "p_sep": 0,
                "t_sep": 0,
                "yi_sep": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "xi_sep": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "yi_flashed_gas": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "xi_flashed_oil": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "zi": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "use_gor_recombination": true,
                "use_sep_gor": true,
                "psat_gor": 0,
                "gor_only": true,
                "MW_c7p_gas": 0,
                "MW_c7p_oil": 0,
                "xi": {
                    "additionalProp1": 0,
                    "additionalProp2": 0,
                    "additionalProp3": 0
                },
                "T_sep": 0
            }
        <<<

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/get_api_external_v1_wells__well_id__input_simplified_dry_gas

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/input_simplified_dry_gas"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res:
            raise Exception("no pvt processes existed")
        return res

    def edit_pvt_simple_dry_gas_input(
        self, well_id: int, payload: dict
    ) -> requests.Response:
        """
        Edit PVT simplified dry gas input of a well.

        Example payload:
        ----------
        >>>
            payload = {
            "input_type": "gor",
            "note": "string",
            "gor": 0,
            "api": 0,
            "psat": 0,
            "use_gor_sep": true,
            "set_non_hc": true,
            "z_n2": 0,
            "z_co2": 0,
            "z_h2s": 0,
            "process": {},
            "yi": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "mw": 0,
            "sg": 0,
            "mw_c7p_oil": 0,
            "mw_c7p_gas": 0,
            "mw_c7p_flashed_oil": 0,
            "flashed_oil_density": 0,
            "flash_temperature": 0,
            "use_api": true,
            "gor_sep": 0,
            "p_sep": 0,
            "t_sep": 0,
            "yi_sep": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "xi_sep": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "yi_flashed_gas": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "xi_flashed_oil": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "zi": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "use_gor_recombination": true,
            "use_sep_gor": true,
            "psat_gor": 0,
            "gor_only": true,
            "MW_c7p_gas": 0,
            "MW_c7p_oil": 0,
            "xi": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "T_sep": 0
        }
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.edit_pvt_simple_dry_gas_input(well_id, payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/put_api_external_v1_wells__well_id__input_simplified_dry_gas

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/input_simplified_dry_gas"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"successfully edited PVT simplified dry gas input for well {well_id}"
            )
        else:
            print(response.text)
        return response

    def get_input_quick(self, well_id: int) -> requests.Response:
        """
        Get the input quick (PVT) property of a well.

        Example function call:
        ----------
        whitson_wells = whitson_connection.get_input_quick()

        Example return:
        ----------
        >>>
            return = {
            "well_id": 0,
            "input_type": "gor",
            "date_updated": "2026-04-27T07:40:17.543Z",
            "updated_by": "string",
            "note": "string",
            "gor": 0,
            "api": 0,
            "psat": 0,
            "use_gor_sep": true,
            "set_non_hc": true,
            "z_n2": 0,
            "z_co2": 0,
            "z_h2s": 0,
            "process": {},
            "yi": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "mw": 0,
            "sg": 0,
            "mw_c7p_oil": 0,
            "mw_c7p_gas": 0,
            "mw_c7p_flashed_oil": 0,
            "flashed_oil_density": 0,
            "flash_temperature": 0,
            "use_api": true,
            "gor_sep": 0,
            "p_sep": 0,
            "t_sep": 0,
            "yi_sep": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "xi_sep": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "yi_flashed_gas": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "xi_flashed_oil": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "zi": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "use_gor_recombination": true,
            "use_sep_gor": true,
            "psat_gor": 0,
            "gor_only": true,
            "MW_c7p_gas": 0,
            "MW_c7p_oil": 0,
            "xi": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0
            },
            "T_sep": 0
        }
        <<<

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/get_api_external_v1_wells__well_id__input_quick
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/input_quick",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully fetch input quick for well {well_id}")
        else:
            print(response.text)
        return response.json()

    def edit_input_quick(self, well_id: int, payload: dict) -> requests.Response:
        """
        Edit the input quick (PVT) property of a well.
        """
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/input_quick",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited input quick for well {well_id}")
        else:
            print(response.text)
        return response

    ### Getting mass fluid data

    def get_pvt_fluid_data(self, well_id: int) -> requests.Response:
        """
        Get the fluid properties after PVT initialization for {well_id}.

        Example payload:
        well_id = this_well_id (a number of type int)
        --> see swagger doc for additional params you can use such as well name or well id directly.

        Example usage:
        response = whitson_connection.get_pvt_fluid_data(well_id)

        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/mass_fluid_data",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"well_id": well_id},
        )

        if response.status_code == 200:
            print(f"Fluid data successfully retrieved for well_id {well_id} ")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_pvt_mass_fluid_data(self, project_id: int) -> requests.Response:
        """
        Get the fluid properties after PVT initialization for all the wells in {project_id}.

        Example payload:
        project_id = this_project_id (a number of type int)
        --> see swagger doc for additional params you can use such as well name or well id directly.

        Example usage:
        response = whitson_connection.get_pvt_mass_fluid_data(project_id)

        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/mass_fluid_data",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )

        if response.status_code == 200:
            print(
                f"Fluid data successfully retrieved for all wells in project {project_id} "
            )
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_pvt_mass_fluid_data_paginated(
        self, project_id: int, page_size: int = 1000
    ) -> requests.Response:
        """
        Get the fluid properties after PVT initialization for all the wells in {project_id} with pagination.

        Example payload:
        project_id = this_project_id (a number of type int)
        --> see swagger doc for additional params you can use such as well name or well id directly.

        Example usage:
        response = whitson_connection.get_pvt_mass_fluid_data_paginated(project_id)

        """
        all_wells = []
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/mass_fluid_data_paginated"

        page = 1  # Start with the first page

        while True:
            response = requests.get(
                base_url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                params={
                    "project_id": project_id,
                    "page": page,
                    "page_size": page_size,  # Lower this if Error 502
                },
            )
            res = response.json()

            retries = 0
            while response.status_code >= 500 and retries < 3:
                retries += 1
                print(f"Error occured Error {response.status_code} encountered.")
                print(response.text)
                print(f"Retrying {retries}")
                response = requests.get(
                    base_url,
                    headers={
                        "content-type": "application/json",
                        "Authorization": f"Bearer {self.access_token}",
                    },
                    params={
                        "project_id": project_id,
                        "page": page,
                        "page_size": page_size,  # Lower this if Error 502
                        "well_type": ["main"],
                    },
                )
                res = response.json()

            if response.status_code >= 400:
                print(f"Error occured Error {response.status_code} encountered.")
                print(response.text)
                break

            if (
                not res or res == []
            ):  # If the response is empty, there are no more wells for this project
                break

            all_wells.extend(
                res
            )  # Append the wells from this page to the list of all wells
            page += 1  # Move to the next page

        return all_wells

    def get_pvt_mass_fluid_data_from_projects(
        self, project_id_list: List[int]
    ) -> List[dict]:
        """
        Get the fluid properties after PVT initialization for all wells across multiple projects.

        Parameters:
        project_id_list (List[int]): A list of project IDs for which to retrieve fluid data.

        Returns:
        List[dict]: A list of responses containing fluid data for all projects.

        Example usage:
        response = whitson_connection.get_pvt_mass_fluid_data_from_projects([project_id1, project_id2])
        """
        all_fluid_data = []  # Store fluid data from all projects

        for project_id in project_id_list:
            response = self.get_pvt_mass_fluid_data(
                project_id
            )  # Call the existing function
            if response and not (
                isinstance(response, dict) and response.get("code") == 500
            ):  # Assuming response is a dictionary or list
                all_fluid_data.extend(
                    response
                )  # Add the fluid data from this project to the list

        return all_fluid_data

    def get_pvt_calcs(self, well_id_list: List[int]) -> requests.Response:
        response = requests.patch(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/pvt_calcs",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={"well_ids": well_id_list},
        )
        if response.status_code == 200:
            print(f"Fluid data successfully retrieved for all wells")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_pvt_status(self, payload: dict) -> requests.Response:
        """
        Get PVT Status

        Example payload:
        ----------
        >>>
            payload = {
                "well_ids": [
                    0
                ]
            }
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.get_pvt_status(payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/PVT%20Data/patch_api_external_v1_wells_pvt_calculation_status
        """
        base_url = f"http://{self.client_name}.whitson.com/api-external/v1/wells/pvt_calculation_status"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successful response")
        else:
            print(response.text)
        return response

    ### Getting bot table

    def get_well_ids_without_pvt(
        self, PROJECT_ID_LIST, whitson_wells, unique_item: str = "external_id"
    ):
        """ """
        external_id_dict = {item[unique_item]: item["id"] for item in whitson_wells}
        pvt_calcs = self.get_pvt_mass_fluid_data_from_projects(PROJECT_ID_LIST)
        well_ids_with_pvt = [
            entry["well_id"] for entry in pvt_calcs if "well_id" in entry
        ]
        all_well_ids = list(external_id_dict.values())
        return list(set(all_well_ids) - set(well_ids_with_pvt))

    def get_pvt_bot_table(self, well_id) -> requests.Response:
        """
        Get the black oil table generated for the {well_id} - each row of the BOT is returned as one element in the response.

        Example payload:
        well_id = this_well_id

        Example usage:
        response = whitson_connection.get_pvt_bot_table(well_id)

        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bot/bot_table",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"well_id": well_id},
        )

        if response.status_code == 200:
            print(f"BOT successfully retrieved for well {well_id}")
            return response.json()
        else:
            print("Something went wrong - ", response)
            return

    def run_composition_calc(self, well_id: int) -> requests.Response:
        """
        Run PVT (composition) calculation on well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_composition_calc",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"success on running composition calc on well {well_id}")
        else:
            print(response.text)
        return response

    def run_pvt_calc(self, well_id: int) -> requests.Response:
        """
        Run PVT (PVT) calculation on well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_pvt_calc",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"success on running PVT calc on well {well_id}")
        else:
            print(response.text)
        return response

    def run_pvt_calc_bulk_with_project_id(self, project_id: int) -> requests.Response:
        """
        Run PVT (PVT) calculation for bulk well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/run_pvt_calcs",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"success on running PVT calc on project {project_id}")
        else:
            print(response.text)
        return response

    def calc_median_gor(
        self,
        well_id: int,
        production_data: list[dict] = None,
        is_sep_rates: bool = False,
        num_of_timesteps: int = 30,
        default: float = 1000,
    ) -> float:
        """
        Calculate the median Gas-Oil Ratio (GOR) for a given well over a specified number of timesteps.

        This function retrieves production data for the specified well and calculates the GOR
        as the ratio of gas production (qg_sep) to oil production (qo_sep) for each timestep.
        If separator rates are used, the corresponding GOR is calculated using separator data.

        The function filters out None values and identifies the first valid non-zero GOR value.
        It then calculates the median GOR from the subsequent data points, up to the specified
        `number_of_timesteps`. If no valid GOR values are found, the function returns a default value.

        Parameters:
            well_id (int): The unique identifier for the well.
            production_data (dict, optional): use production data available instead of getting it from whitson+.
            is_sep_rates (bool, optional): Flag to determine if separator rates should be used. Defaults to False.
            num_of_timesteps (int, optional): The number of data points to consider for the median calculation. Defaults to 30.
            default (float, optional): The value to return if no valid GOR values are found. Defaults to 1000.

        Returns:
            float: The median GOR value for the specified well over the selected time steps,
                or the default value if no valid data is available.
        """
        production_data = (
            self.get_production(well_id) if production_data == None else production_data
        )
        production_data = sorted(
            production_data,
            key=lambda x: x["date"],
        )
        prefix = "_sep" if is_sep_rates else "_sc"
        qo_sep_series = [entry["qo" + prefix] for entry in production_data]
        qg_sep_series = [entry["qg" + prefix] for entry in production_data]
        gor = [
            (qg / qo * 1000) if qo not in [None, 0] and qg is not None else None
            for qo, qg in zip(qo_sep_series, qg_sep_series)
        ]

        filtered_gor = [value for value in gor if value is not None]
        first_non_zero_index = next(
            (i for i, value in enumerate(filtered_gor) if value != 0), None
        )

        if first_non_zero_index is not None:
            selected_gor = filtered_gor[
                first_non_zero_index : first_non_zero_index + num_of_timesteps
            ]

            return np.median(selected_gor) if selected_gor else None
        else:
            return default

    def calc_median_gor_no_zero_outlier_removal(
        self,
        well_id: int,
        production_data: list[dict] = None,
        is_sep_rates: bool = False,
        num_of_timesteps: int = 30,
        default: float = 1000,
        remove_outlier: bool = False,
        manual_lower_bound: float = None,
        manual_upper_bound: float = None,
    ) -> float:
        """
        Calculate the median Gas-Oil Ratio (GOR) for a given well over a specified number of timesteps.

        This function retrieves production data for the specified well and calculates the GOR
        as the ratio of gas production (qg_sep) to oil production (qo_sep) for each timestep.
        If separator rates are used, the corresponding GOR is calculated using separator data.
        The function filters out None and zero values.

        Then it calculates the median GOR from the first `num_of_timesteps` values.
        If remove_outlier is True, outliers are removed using 1.5 * IQR unless overridden by manual bounds.

        Parameters:
            well_id (int): The unique identifier for the well.
            production_data (list[dict], optional): Use production data available instead of getting it from whitson+.
            is_sep_rates (bool, optional): Flag to determine if separator rates should be used. Defaults to False.
            num_of_timesteps (int, optional): Number of data points to consider for the median calculation. Defaults to 30.
            default (float, optional): Value to return if no valid GOR values are found. Defaults to 1000.
            remove_outlier (bool, optional): Whether to remove outliers using IQR/manual bounds. Defaults to True.
            manual_lower_bound (float, optional): Optional user-defined lower GOR bound. Falls back to IQR if None.
            manual_upper_bound (float, optional): Optional user-defined upper GOR bound. Falls back to IQR if None.

        Returns:
            float: The median GOR over the selected time steps, or the default if no valid data exists.
        """

        production_data = (
            self.get_production(well_id) if production_data is None else production_data
        )

        prefix = "_sep" if is_sep_rates else "_sc"

        qo_sep_series = [entry.get("qo" + prefix) for entry in production_data]
        qg_sep_series = [entry.get("qg" + prefix) for entry in production_data]

        gor = [
            (qg / qo * 1000) if qo not in [None, 0] and qg is not None else None
            for qo, qg in zip(qo_sep_series, qg_sep_series)
        ]

        filtered_gor = list(filter(lambda x: x not in (0, None), gor))

        if filtered_gor:
            selected_gor = filtered_gor[:num_of_timesteps]

            if remove_outlier:
                # Compute IQR bounds
                q1 = np.percentile(selected_gor, 25)
                q3 = np.percentile(selected_gor, 75)
                iqr = q3 - q1
                iqr_lower_bound = q1 - 1.5 * iqr
                iqr_upper_bound = q3 + 1.5 * iqr

                # Use manual if provided, else fall back to IQR
                lower_bound = (
                    manual_lower_bound
                    if manual_lower_bound is not None
                    else iqr_lower_bound
                )
                upper_bound = (
                    manual_upper_bound
                    if manual_upper_bound is not None
                    else iqr_upper_bound
                )

                selected_gor = [
                    x for x in selected_gor if lower_bound <= x <= upper_bound
                ]

            return float(np.median(selected_gor)) if selected_gor else default
        else:
            return default

    # ---------------------------------------------------------------------------------------------------------
    # Sampling Data Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def get_sampling_data_for_well(self, well_id: int) -> requests.Response:
        """
        Returns all the sampling data uploaded for well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/sampling_data",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"success on returning all the sampling data uploaded for well {well_id}"
            )
        else:
            print(response.text)
        return response.json()

    def upload_sampling_data_to_well(
        self, well_id: int, payload: dict
    ) -> requests.Response:
        """
        Upload sampling data to a well.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/sampling_data"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully uploaded sampling data for for well {well_id}")
        else:
            print(response.text)
        return response

    # ---------------------------------------------------------------------------------------------------------
    # Common Process Conversion
    # ---------------------------------------------------------------------------------------------------------

    def get_common_process_rates_for_well(self, well_id: int) -> requests.Response:
        """
        Returns common process rates for well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/well_monitoring/{well_id}/common_process_conversion",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"success on returning common process rates for well {well_id}")
        else:
            print(response.text)
        return response.json()

    def get_separtor_oil_shrinkage_for_well(self, well_id: int) -> requests.Response:
        """
        Returns all separator oil shrinkage data for well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/well_monitoring/{well_id}/separator_oil_shrinkage",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"success on returning separator oil shrinkage data for well {well_id}"
            )
        else:
            print(response.text)
        return response.json()

    def run_separator_oil_shrinkage_calc(self, well_id: int) -> requests.Response:
        """
        Run common process conversion and separator oil shrinkage on the well specified by the provided well_id.

        More info here: https://internal.whitson.com/api-external/swagger/#/Well%20Monitoring/get_api_external_v1_wells__well_id__run_well_monitoring
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_well_monitoring"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code == 202:
            print(f"successfully run separator oil shrinkage calc on {well_id}")
        else:
            print(response.text)
        return response

    def run_separator_oil_shrinkage_calc_in_projects(
        self, project_ids: list
    ) -> requests.Response:
        """
        Bulk separator calculation on all the wells specified by the provided project_ids

        Example Payload:
        project_ids = [255, 94]
        Example function call:
        whitson_connection.run_separator_oil_shrinkage_calc_in_projects(project_ids)

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/run_well_monitoring"

        for project_id in project_ids:
            response = requests.get(
                base_url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                params={"project_id": project_id},
            )
            if response.status_code == 202:
                print(f"successfully ran well monitoring on Project: {project_id}")
            else:
                print(response.text)

    def get_compositional_tracking_data(self, well_id: int) -> requests.Response:
        """
        Returns compositional tracking data for well.
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/well_monitoring/{well_id}/composition_tracking",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"success on returning compositional tracking data for well {well_id}"
            )
        else:
            print(response.text)
        return response.json()

    # --------------------------------------------------------------------- #
    # Pressure-Normalised Rates (PNR)                                       #
    # --------------------------------------------------------------------- #
    def get_pressure_normalized_rates_for_wells(
        self,
        well_identifiers: list[int],
        from_date: str | None = None,
        end_date: str | None = None,
        last_updated: str | None = None,
        page_size: int = 1000,
    ) -> list[dict]:
        """
        Retrieve PNR rows well-by-well.

        Parameters
        ----------
        well_identifiers : list[int]
            List of well IDs to query.
        from_date, end_date, last_updated, page_size
            Same meaning as before.

        Returns
        -------
        list[dict]
            Concatenated pages for all requested wells.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/pnr"
        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        aggregated_rows: list[dict] = []

        for well_identifier in well_identifiers:
            page_number = 1
            while True:
                query_parameters = {
                    "well_id": well_identifier,
                    "page": page_number,
                    "page_size": page_size,
                }
                if from_date:
                    query_parameters["from_date"] = from_date
                if end_date:
                    query_parameters["end_date"] = end_date
                if last_updated:
                    query_parameters["updated"] = last_updated

                response = requests.get(
                    base_url, headers=headers, params=query_parameters, timeout=30
                )
                if response.status_code == 500:
                    print(
                        f"[PNR] 500-error on well {well_identifier}, page {page_number}"
                    )
                    break
                if not 200 <= response.status_code < 300:
                    print(
                        f"[PNR] {response.status_code} on well {well_identifier}:"
                        f" {response.text}"
                    )
                    break

                page_rows = response.json()
                if not page_rows:
                    break

                aggregated_rows.extend(page_rows)
                page_number += 1
                print(page_number)

        return aggregated_rows

    def get_pressure_normalized_rates_project_id(
        self,
        project_ids: list[int],
        from_date: str = None,
        page_size: int = 1000,
        end_date: str = None,
    ) -> requests.Response:
        """
        Retrieve PNR rows using project ID.
        """
        all_wells = []
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/pnr"
        for project_id in project_ids:
            page = 1  # Start with the first page
            while True:
                try:
                    response = requests.get(
                        base_url,
                        headers={
                            "content-type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                        },
                        params={
                            "project_id": project_id,
                            "page": page,
                            "from_date": from_date,
                            "page_size": page_size,  # Lower this if Error 502
                            "end_date": end_date,
                        },
                    )
                    res = response.json()
                    if (
                        not res
                    ):  # If the response is empty, there are no more wells for this project
                        break
                    all_wells.extend(
                        res
                    )  # Append the wells from this page to the list of all wells
                except:
                    print("Something went wrong")
                page += 1  # Move to the next page
                print(page)
        return all_wells

    # ---------------------------------------------------------------------------------------------------------
    # BHP Input Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def upload_well_data_to_well(
        self, well_id: int, payload: list[dict]
    ) -> requests.Response:
        """
        Upload a well data to a well.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input/well_data"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully updated well_data to well {well_id}")
        else:
            print(response.text)
        return response

    def upload_well_data_bulk(self, payload: list[dict]) -> requests.Response:
        """
        Upload a well data in bulk. It will replace the whole well data with the payload.
        """
        response = requests.post(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_input/well_data/bulk",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully updated well_data to wells")
        else:
            print(response.text)
        return response

    def edit_well_data_for_well_data_id(
        self, well_data_id: int, payload: list[dict]
    ) -> requests.Response:
        """
        Edit a well data to for well_data_id WELL_DATA_ID.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_input/well_data/{well_data_id}"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully updated well_data to well_data_id {well_data_id}")
        else:
            print(response.text)
        return response

    def edit_gas_lift_data(
        self, well_data_id: int, payload: list[dict]
    ) -> requests.Response:
        """
        Edit gas lift data for well_data_id WELL_DATA_ID.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_input/well_data/{well_data_id}/gas_lift_data"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully updated well_data to well_data_id {well_data_id}")
        else:
            print(response.text)
        return response

    def edit_well_deviation_data(
        self, well_id: int, payload: list[dict]
    ) -> requests.Response:
        """
        Edit well deviation data of a well in the database.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input/well_deviation_survey"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"changed well deivation survey on well_id {well_id}")
        else:
            print(response.text)
        return response

    def run_bhp_calc(self, well_id: int) -> requests.Response:
        """
        Run bhp calculation on the well specified by the provided well_id.

        More info here: https://internal.whitson.com/api-external/swagger/#/BHP%20Data/get_api_external_v1_wells_run_bhp_calculation
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_bhp_calculation"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code == 202:
            print(f"successfully ran bhp calc on Well: {well_id}")
        else:
            print(response.text)

    def run_bhp_calc_in_projects(self, project_ids: list) -> requests.Response:
        """
        Bulk Run bhp calculation on all the wells specified by the provided project_ids

        Example Payload:
        project_ids = [255, 94]
        Example function call:
        whitson_connection.run_bhp_calc_in_projects(project_ids)

        More info here: https://internal.whitson.com/api-external/swagger/#/BHP%20Data/get_api_external_v1_wells_run_bhp_calculation
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/run_bhp_calculation"

        for project_id in project_ids:
            response = requests.get(
                base_url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                params={"project_id": project_id},
            )
            if response.status_code == 202:
                print(f"successfully ran bhp calc on Project: {project_id}")
            else:
                print(response.text)

    def edit_rate_type_for_well(self, well_id: int, rate_type: str):
        """
        Updates the rate type for a specified well in the Whitson+ platform.

        Parameters:
            well_id (int): The unique identifier for the well in the Whitson+ system.
            rate_type (str): The rate type to set for the well. Valid options are:
                            - "measured" for stock tank rate
                            - "common" for separator rate

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/set_production_rate_type/{rate_type}"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"well_id": well_id, "rate_type": rate_type},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Changed the rate type on well with id {well_id}")
        else:
            print(response.text)
        return response

    def get_bhp_calc(self, well_id: int, from_date: str = None) -> requests.Response:
        """
        Get bhp calculation on the well specified by the provided well_id.
        If from_date is specified as "YYYY-MM-DD" the BHP calcs after this date is returned.
        If the from_date is not specified, all BHP records are returned.

        More info here: https://internal.whitson.com/api-external/swagger/#/BHP%20Data/get_api_external_v1_wells__well_id__bhp_calculation
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_calculation"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"from_date": from_date},
        )
        if response.status_code == 200:
            print(f"successfully retrieved bhp calc on {well_id}")
        else:
            print(response.text)
        return response.json()

    def get_bhp_from_projects(
        self,
        project_ids: list[int],
        from_date: str = None,
        page_size: int = 1000,
        last_updated: str = None,
        return_all: bool = True,
    ) -> requests.Response:
        """
        Get a list of well BHPs from projects with project_id given in list.
        Example: whitson_wells_bhp = whitson_connection.get_bhp_from_projects([1, 2, 3])
        If from_date is specified as "YYYY-MM-DD" the BHP calcs after this date is returned.
        If the from_date is not specified, all BHP records are returned.
        If last_updated is specified as "YYYY-MM-DD" the BHP calcs updated after this date is returned.
        Lower the page size if 502 Error
        """
        all_wells = []
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_calculation"
        for project_id in project_ids:
            page = 1  # Start with the first page
            while True:
                try:
                    response = requests.get(
                        base_url,
                        headers={
                            "content-type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                        },
                        params={
                            "project_id": project_id,
                            "page": page,
                            "from_date": from_date,
                            "page_size": page_size,  # Lower this if Error 502
                            "updated": last_updated,
                            "return_all": return_all,
                        },
                    )
                    res = response.json()
                    if (
                        not res
                    ):  # If the response is empty, there are no more wells for this project
                        break
                    all_wells.extend(
                        res
                    )  # Append the wells from this page to the list of all wells
                except:
                    print("Something went wrong")
                page += 1  # Move to the next page
                print(page)
        return all_wells

    def get_bhp_from_projects_with_pwf(
        self,
        project_ids: list[int],
        from_date: str = None,
        page_size: int = 1000,
        last_updated: str = None,
        return_all: bool = True,
        end_date: str = None,
    ) -> requests.Response:
        """
        Get a list of well BHPs (with Measured Pressure Gauge data) from projects with project_id given in list.
        Example: whitson_wells_bhp = whitson_connection.get_bhp_from_projects([1, 2, 3])
        If from_date is specified as "YYYY-MM-DD" the BHP calcs after this date is returned.
        If the from_date is not specified, all BHP records are returned.
        If last_updated is specified as "YYYY-MM-DD" the BHP calcs updated after this date is returned.
        Lower the page size if 502 Error
        """
        all_wells = []
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_calculation_with_gauge_data"
        for project_id in project_ids:
            page = 1  # Start with the first page
            while True:
                try:
                    response = requests.get(
                        base_url,
                        headers={
                            "content-type": "application/json",
                            "Authorization": f"Bearer {self.access_token}",
                        },
                        params={
                            "project_id": project_id,
                            "page": page,
                            "from_date": from_date,
                            "page_size": page_size,  # Lower this if Error 502
                            "updated": last_updated,
                            "return_all": return_all,
                            "end_date": end_date,
                        },
                    )
                    res = response.json()
                    if (
                        not res
                    ):  # If the response is empty, there are no more wells for this project
                        break
                    all_wells.extend(
                        res
                    )  # Append the wells from this page to the list of all wells
                except:
                    print("Something went wrong")
                page += 1  # Move to the next page
                print(page)
        return all_wells

    def get_well_data(self, well_id: int = 0):
        """
        Get the wellbore info.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input/well_data"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        return response.json()

    def get_well_data_bulk(self, payload: dict):
        """
        Get the wellbore info in bulk.

        Example payload:
        >>>
        {
            "use_from_date": "2026-03-20",
            "well_ids": [
                0
            ]
        }
        >>>
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_input/well_data"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code == 200:
            print(f"successfully retrieved well data")
        else:
            print(response.text)
        return response.json()

    def get_wellbore_data_from_well_id_list(
        self, well_id_list: list[int], updated: str = None
    ) -> dict:
        """
        Fetch BHP input data for a list of wells using their well IDs.

        Args:
            well_id_list (list[int]): A list of well IDs to retrieve BHP input data for.
            updated (str, optional): Date to filter the BHP input on and onwards in the format 'YYYY-MM-DD'.
                                    If not specified, all data is retrieved.

        Returns:
            dict: A dictionary containing the BHP input data for the specified wells in JSON format.

        Raises:
            requests.exceptions.RequestException: If there is an issue with the HTTP request.

        Example:
            >>> client.get_wellbore_data_from_well_id_list([101, 102], "2024-11-29")
            {
                "well_data": [...]
            }
        """
        url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_inputs"

        params = {"well_ids": well_id_list}
        if updated:
            params["updated"] = updated

        try:
            response = requests.patch(
                url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json=params,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch wellbore data: {e}")

    def get_wellbore_data_from_well_id_list_no_deviation(
        self, well_id_list: list[int], updated: str = None
    ) -> dict:
        """
        Fetch BHP input data for a list of wells without deviation surveys.

        Args:
            well_id_list (list[int]): A list of well IDs to retrieve BHP input data for.
            updated (str, optional): Date to filter the BHP input on and onwards in the format 'YYYY-MM-DD'.
                                    If not specified, all data is retrieved.

        Returns:
            dict: A dictionary containing the BHP input data for the specified wells in JSON format.

        Raises:
            requests.exceptions.RequestException: If there is an issue with the HTTP request.

        Example:
            >>> client.get_wellbore_data_no_deviation([101, 102], "2024-11-29")
            {
                "well_data": [...]
            }
        """
        url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_inputs_no_deviation_survey"

        payload = {"well_ids": well_id_list}
        if updated:
            payload["updated"] = updated

        try:
            response = requests.patch(
                url,
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch wellbore data (no deviation survey): {e}")

    def get_well_deviation_and_perf_interval(self, well_id: int = 0):
        """
        Get the bottomhole pressure input.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        return response.json()

    def edit_perf_interval(self, well_id: int, payload: list[dict]):
        """
        Edit perf interval
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Changed perforated interval on well_id {well_id}")
        else:
            print(response.text)
        return response

    def edit_perf_interval_bulk(self, payload: list[dict]):
        """
        Edit perf interval

        Example payload:
        [
            {
                "well_id":1,
                "top_perforation_md": 5000,
                "bottom_perforation_md": 10000
            }
        ]
        """
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_inputs"
        )
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(response.json())
        else:
            print(response.text)
        return response

    def is_wellbore_configuration_already_uploaded(
        self, new_wellbore_configuration, existing_wellbore_data
    ):
        """
        Checks if a wellbore configuration with the same 'use_from_date' as the new configuration
        already exists in the existing wellbore data.

        Args:
            new_wellbore_configuration (dict): The new wellbore configuration to check.
            existing_wellbore_data (list of dict): The list of existing wellbore configurations.

        Returns:
            bool: True if a configuration with the same 'use_from_date' already exists, False otherwise.
        """
        return any(
            wellbore["use_from_date"] == new_wellbore_configuration["use_from_date"]
            for wellbore in existing_wellbore_data
        )

    def is_default_well_configuration(self, wellbore_data) -> bool:
        """
        Checks if the provided wellbore data represents a default well configuration.

        Args:
            wellbore_data (list of dict): List containing dictionaries representing wellbore data.
                Each dictionary should have keys 'use_from_date', 'well_data_casing', and 'well_data_tubing'.

        Returns:
            bool: True if the well configuration matches default criteria:
                - 'bottom_md' of the first casing is 12000,
                - 'bottom_md' of the first tubing is 7000,
                - 'use_from_date' is None.
            False otherwise.
        """
        if not isinstance(wellbore_data, list) or len(wellbore_data) == 0:
            return False

        first_well_data = wellbore_data[0]

        # if first_well_data.get('well_data_tubing', [{}]) == []:
        #     return False

        return (
            first_well_data.get("use_from_date") is None
            and first_well_data.get("well_data_casing", [{}])[0].get("bottom_md")
            == 12000
            # and first_well_data.get('well_data_tubing', [{}])[0].get('bottom_md') == 7000
        )

    def is_default_deviation_survey(self, well_id: int) -> bool:
        """
        Checks if the provided deviation survey is is whitson+ default.
        """

        default_survey = [
            {"md": 0.0, "tvd": 0.0},
            {"md": 7000.0, "tvd": 7000.0},
            {"md": 12000.0, "tvd": 7000.0},
        ]
        return default_survey == self.get_well_deviation_data(well_id)

    def is_default_perforated_interval(self, well_id) -> bool:
        """
        Checks if the provided perforation interval is whitson+ default.
        """

        well_and_perf = self.get_well_deviation_and_perf_interval(well_id)

        return (
            well_and_perf.get("top_perforation_md") == 7100
            and well_and_perf.get("bottom_perforation_md") == 12000
        )

    def get_bhp_data_default_status(self, well_ids: list) -> dict:
        """
        Get default status for deviation survey, perforated interval, and wellbore config for a given list of well ids
        """
        payload = {
            "well_ids": well_ids,
            "well_data_checks": [
                "deviation_survey",
                "perforated_interval",
                "wellbore_config",
            ],
        }
        response = requests.post(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_well_data_default_status",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved bhp default status")
        else:
            print(response.text)
        return response.json()

    def edit_well_deviation_survey(
        self, well_id: int, payload: dict
    ) -> requests.Response:
        """

        Example payload: [{'md': 0, 'tvd': 0}, {'md': 95.1, 'tvd': 95.1}, {'md': 153.6, 'tvd': 153.6}]

        Endpoint: https://internal.whitson.com/api-external/swagger/#/BHP%20Data/put_api_external_v1_wells__well_id__bhp_input_well_deviation_survey

        """
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/bhp_input/well_deviation_survey",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited well deviation survey for {well_id}.")
        else:
            print(response.text)
        return response

    def edit_well_deviation_survey_bulk(self, payload: list) -> requests.Response:
        """

        Example payload:[
                            {
                                "well_id": 0,
                                "deviation_points": [
                                {
                                    "md": 0,
                                    "tvd": 0
                                }
                                ]
                            }
                        ]

        Endpoint: https://internal.whitson.com/api-external/swagger/#/BHP%20Data/post_api_external_v1_wells_bhp_input_well_deviation_survey
        """
        response = requests.post(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_input/well_deviation_survey",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"successfully edited well deviation survey for {len(payload)} wells."
            )
        else:
            print(response.text)
        return response

    def delete_wellbore_config_by_well_data_id(self, well_data_id: int):
        """
        Delete wellbore with wellbore id well_data_id.
        """
        response = requests.delete(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_input/well_data/{well_data_id}",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"Wellbore config {well_data_id} successfully deleted")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def edit_bhp_tuning_parameters(self, payloads: list):
        """
        Edit BHP tuning parameter for many wells

        Example function call:
        ----------
        response = whitson_connection.edit_bhp_tuning_parameters(payloads)

        Payload Example:
        ----------
        payloads = [
            {
                "well_id": 1,
                "hagedorn_and_brown": {
                "alpha": 1,
                "beta": 1
                },
                "beggs_and_brill": {
                "alpha": 1,
                "beta": 1
                },
                "woldesemayat_and_ghajar": {
                "alpha": 1,
                "beta": 1
                },
                "gray": {
                "alpha": 1,
                "beta": 1
                }
            }
        ]

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/BHP%20Data/put_api_external_v1_wells_bhp_tuning_parameters
        """
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_tuning_parameters",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payloads,
        )

        if response.status_code == 200:
            print(f"BHP tuning parameters successfully editted")
        else:
            print("Something went wrong - ", response)

        return response

    def get_bhp_status(self, project_id: int):
        """
        Get BHP calculation status with PROJECT ID
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/bhp_calculation_status",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )

        if response.status_code == 200:
            print(f"BHP calculation status successfully fetched")
        else:
            print("Something went wrong - ", response)

        return response

    # ---------------------------------------------------------------------------------------------------------
    # Numerical Model Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def run_numerical_model(
        self, well_id: int, include_forecast: bool = True, rate_control: str = "BHP"
    ):
        """
        Run numerical model
        """
        base_url = f"https://{self.client_name}.whitson.com//api-external/v1/wells/{well_id}/run_history_matching"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={
                "well_id": str(well_id),
                "grid_refinement": "Low",
                "rate_control": rate_control,
                "include_forecast": str(include_forecast).lower(),
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully ran numerical model for well with id {well_id}")
        else:
            print(response.text)
        return response

    def edit_numerical_model_for_many_wells(self, payload: dict):
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/history_matching_input",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited well(s)")
        else:
            print(response.text)
        return response

    def get_numerical_model_input_for_well(self, well_id):
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/history_matching_input",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"well_id": str(well_id)},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited well(s)")
        else:
            print(response.text)
        return response.json()

    def run_numerical_model_autofit(
        self, well_id: int, payload: dict, sleep_time: int = 0
    ):
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_autofit_history_matching",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully ran autofit on well: {well_id}")
        else:
            print(response.text)
        time.sleep(sleep_time)
        return response

    def edit_numerical_model_forecast(self, well_id: int, payload: dict):
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/forecast_input",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited forecast schedule for well: {well_id}")
        else:
            print(response.text)
        return response

    def edit_numerical_model_forecast_for_many_wells(self, payload: dict):
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/forecast_inputs",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully edited forecast for many wells")
        else:
            print(response.text)
        return response

    def get_numerical_model_rates_and_pressures(self, well_id: int):
        """
        Edit perf interval
        """
        base_url = f"https://{self.client_name}.whitson.com//api-external/v1/wells/{well_id}/history_matching"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"successfully retrieved numerical model rates and pressures for well_id {well_id}"
            )
        else:
            print(response.text)
        return response.json()

    def get_pwf_active(self, well_id: int, from_date: str = None) -> requests.Response:
        """s
        Get active pwf from the database for the given well_id, from the start date in from_date.

        Example params:
        this_well_id = integer
        this_from_date = "YYYY-MM-DD"

        Example function call:
        active_pwf_well = whitson_connection.get_pwf_active(this_well_id, this_from_date)
        """

        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/pwf_active"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"from_date": from_date},
        )
        if response.status_code == 200:
            print(f"successfully received active pwf on {well_id}")
        else:
            print(response.text)
        return response.json()

    def get_pwf_active_multiple(self, payload: dict) -> requests.Response:
        """
        Get active pwf from the database for all the well_ids from the start date in from_date

        Example payload:
        payload = {"from_date":"YYYY-MM-DD", "page": 0, "page_size": 10}

        Example function call:
        active_pwf_wells = whitson_connection.get_pwf_active_multiple(payload)

        """

        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/pwf_active"
        )
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params=payload,
        )
        if response.status_code == 200:
            print(f"successfully recieved active pwf")
        elif response.status_code == 404:
            print(f"No wells found matching the payload criteria")
        else:
            print(response.text)
        return response.json()

    def run_numerical_forecast(
        self, well_id, end_day, initial_bhp, decline_rate, abandonment_pressure
    ):
        """Run forecast based on"""
        forecast_payload = [
            {
                "well_id": well_id,
                "forecast_control": "bhp",
                "forecast_type": "parametric_decline",
                "initial_forecast_type": "simulated",
                "custom_schedule_data": {
                    "data": [{"day": 3650, "value": abandonment_pressure}]
                },
                "parametric_decline_segments": {
                    "data": [
                        {
                            "forecast_end_time": end_day,
                            "type": "Hyperbolic",
                            "initial_value": initial_bhp,
                            "decline": decline_rate,
                            "final_value": abandonment_pressure,
                        }
                    ]
                },
            }
        ]

        self.edit_numerical_model_forecast_for_many_wells(forecast_payload)

        self.run_numerical_model(well_id, rate_control="Gas")

    def get_hist_match_calculation_status(self, well_ids: list[int]):
        """
        Get history matching calculation status for list of wells {well_ids}.

        Example params:
        well_ids = [1, 2, 3]

        Example function call:
        response = whitson_connection.get_hist_match_calculation_status(well_ids)
        """
        url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/hist_match_calculation_status"
        response = requests.patch(
            url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={"well_ids": well_ids},
        )
        if response.status_code >= 200 and response.status_code < 300:
            return response
        else:
            print(response.text)

    def get_available_custom_attributes(self, well_id: int) -> requests.Response:
        """
        Get available (existing) custom attributes for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_available_custom_attributes(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/available_custom_attributes",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"Custom attribute(s) successfully retrieved for well {well_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_custom_attribute_value(self, well_id: int) -> requests.Response:
        """
        Get values for (existing) custom attributes for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_custom_attributes_value(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/custom_attributes",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"Custom attribute(s) data successfully retrieved for well {well_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_custom_attribute_value_bulk(self, well_ids: list[int]) -> requests.Response:
        """
        Get values for (existing) custom attributes for wells.

        Example params:
        well_ids = [1,2,3]

        Example function call:
        response = whitson_connection.get_custom_attribute_value_bulk(well_ids)
        """
        response = requests.patch(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/custom_attributes_bulk",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={"well_ids": well_ids},
        )

        if response.status_code == 200:
            print(
                f"Custom attribute(s) data successfully retrieved for {len(well_ids)} wells"
            )
        else:
            print("Something went wrong - ", response)

        return response.json()

    def delete_custom_attribute_value(
        self, well_id: int, attribute_name: str
    ) -> requests.Response:
        """
        Delete values for (existing) custom attributes for well {well_id}. Note that this does not delete the custom attribute from the project.

        Example params:
        this_well_id = integer
        this_custom_attribute = 'MyAttribute'

        Example function call:
        response = whitson_connection.delete_custom_attributes_value(this_well_id, this_custom_attribute)
        """
        response = requests.delete(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/custom_attributes/{attribute_name}",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"Custom attribute(s) successfully updated for well {well_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def edit_custom_attribute_value(
        self, well_id: int, payload: dict
    ) -> requests.Response:
        """
        Set values for (existing) custom attributes for well {well_id}.

        Example payload:
        payload = {"attribute_name": "MyNumericAttribute",
           "number_attribute": {"attribute_value": 1250}}

        Example function call:
        response = whitson_connection.set_custom_attributes_value(this_well_id, payload)
        """
        response = requests.post(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/custom_attributes",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )

        if response.status_code == 200:
            print(f"Custom attribute(s) successfully updated for well {well_id}")
        elif response.status_code == 403:
            print(f"Custom attribute does not exist")
        else:
            print("Something went wrong - ", response)

        return response

    def edit_custom_attribute_bulk(self, payload) -> requests.Response:
        """
        Set values for (existing) custom attributes in bulk.

        Example payload:
        payload = [
            {
            "well_id": 3701,
            "attribute_name": "DFIT Y|N",
            "value": "Y"
            },
            {
            "well_id": 3701,
            "attribute_name": "Well Alias Name",
            "value": "Me"
            }
        ]

        Example function call:
        response = whitson_connection.edit_custom_attribute_bulk(payload)
        """
        response = requests.post(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/custom_attributes_bulk",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )

        if response.status_code == 200:
            print(f"Custom attribute(s) successfully updated")
        else:
            print("Something went wrong - ", response)

        return response

    def delete_custom_attribute_bulk(self, payload) -> requests.Response:
        """
        Delete custom attribues in bulk

        Example payload:
        ----------
        >>>
            payload = [
                {
                    "well_id": 0,
                    "attribute_name": "string"
                }
            ]
        <<<

        Example function call:
        ----------
        whitson_wells = whitson_connection.delete_custom_attribute_bul(payload)

        More info about endpoint here:
        ----------
        https://internal.whitson.com/api-external/swagger/#/Base%20Data/delete_api_external_v1_wells_custom_attributes_bulk
        """

        response = requests.delete(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/custom_attributes_bulk",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )

        if response.status_code == 200:
            print(f"Custom attribute(s) successfully deleted")
        else:
            print("Something went wrong - ", response)

        return response

    # ---------------------------------------------------------------------------------------------------------
    # Analytical RTA Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def run_classical_rta(self, well_id: int) -> requests.Response:
        """
        Run classical RTA calculation for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.run_classical_rta(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/calculate_analytical_rta",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code >= 200 and response.status_code < 400:
            print("success")
        else:
            print(f"Error occured running well id {well_id}")

    def get_classical_rta_interpretation(self, well_id: int) -> requests.Response:
        """
        Get (existing) classical RTA results for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_classical_rta_interpretation(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/arta_interpretations",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

    def get_classical_rta_interpretation_for_project(
        self, project_id: int, batch_size=200
    ) -> list:
        """
        Get (existing) classical RTA results for wells in a project.

        Example params:
        project_id = integer

        Example function call:
        response = whitson_connection.get_classical_rta_interpretation(project_id)
        """
        well_ids = [well["id"] for well in self.get_wells_from_projects([project_id])]
        rta_to_return = []
        for i in range(0, len(well_ids), batch_size):
            response = requests.patch(
                f"https://{self.client_name}.whitson.com/api-external/v1/wells/arta_interpretations",
                headers={
                    "content-type": "application/json",
                    "Authorization": f"Bearer {self.access_token}",
                },
                json={"well_ids": well_ids[i : i + batch_size]},
            )

            if response.status_code == 200:
                rta_to_return.extend(response.json())
            else:
                print("Something went wrong - ", response)
                return

        print(f"Classical RTA data successfully retrieved for project {project_id}")
        return rta_to_return

    def get_analytical_rta_timeseries(self, well_id: int) -> requests.Response:
        """
        Get (existing) analytical RTA timeseries for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_analytical_rta_time_series(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/arta_time_series",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(
                f"Analytical RTA timeseries data successfully retrieved for well {well_id}"
            )
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_material_balance_timeseries(self, well_id: int) -> requests.Response:
        """
        Get (existing) material balance timeseries for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_material_balance_timeseries(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/material_balance_time",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(
                f"Material balance timeseries successfully retrieved for well {well_id}"
            )
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_fractional_rta_interpretations(self, well_id: int) -> requests.Response:
        """
        Get (existing) fractional RTA interpretations for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_fractional_rta_interpretations(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/fractional_rta_interpretations",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"Fractional RTA data successfully retrieved for well {well_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    # ---------------------------------------------------------------------------------------------------------
    # Data Status (on whitson) Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def get_new_data_status_by_project_id(self, project_id: int) -> requests.Response:
        """
        Get (existing) data status for well {well_id}.
        Bool flag set to True if there are changes in any of the fields returned since last BHP calc,
        False if there is no change in input to BHP calc since the previous run.

        Example params:
        this_project_id = integer

        Example function call:
        response = whitson_connection.get_data_status(this_project_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/new_well_input_status",
            params={
                "project_id": project_id,
            },
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"New Data Status successfully retrieved for project {project_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_well_data_status(self, well_id: int) -> requests.Response:
        """
        Get data status (data exists/not) for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_data_status(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/status",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code == 200:
            print(f"Data Status successfully retrieved for well {well_id}")
        else:
            print("Something went wrong - ", response)
        return response.json()

    # ---------------------------------------------------------------------------------------------------------
    # Numerical RTA Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def _get_nrta_report_dataframe(self):
        return pd.DataFrame(
            columns=[
                "Run",
                "swi",
                "fcd",
                "swc",
                "sorw",
                "sorg",
                "sgc",
                "nw",
                "now",
                "ng",
                "nog",
                "Error",
                "Probability_to_Accept",
                "Was_Accepted",
            ]
        )

    def _edit_nrta_weight_factors(self, payload: dict) -> requests.Response:
        """
        Edit numerical RTA weight factors.

        payload = [{
            "well_id": 1,
            "oil_cum": 0,
            "gas_cum": 0,
            "water_cum": 0,
            "gor_cum": 0,
            "oil": 0,
            "gas": 0,
            "water": 0,
            "gor": 0
        }]
        """
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/edit_nrta_weight_factors",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        return response

    def _edit_nrta_project_parameters(self, project_id: int, params):
        swi, fcd, swc, sorw, sorg, sgc, nw, now, ng, nog = params
        rel_perm_fcd = {
            "fcd": fcd,
            "swc": swc,
            "sorw": sorw,
            "sorg": sorg,
            "sgc": sgc,
            "nw": nw,
            "now": now,
            "ng": ng,
            "nog": nog,
            "krwro": 1,
            "krgro": 1,
            "krocw": 1,
            "fracture_swc": 0,
            "fracture_sorw": 0,
            "fracture_sorg": 0,
            "fracture_sgc": 0,
            "fracture_nw": 1,
            "fracture_now": 1,
            "fracture_ng": 1,
            "fracture_nog": 1,
            "fracture_krwro": 1,
            "fracture_krocw": 1,
            "fracture_krgro": 1,
        }

        swi_gamma_cr = {
            "Sw_i": swi,
            "gamma_m": 0.0000,
            "gamma_f": 0.0000,
            "cr": 0.000004,
        }

        wells = self.get_wells(project_id)

        rel_perm_fcd_payload = []
        swi_pressure_dep_payload = []

        for well in wells:
            well_id = well["id"]

            # For rel_perm_fcd_payload
            rel_perm_fcd_payload.append(
                {"well_id": well_id, **rel_perm_fcd}  # Unpack rel_perm_fcd dictionary
            )

            # For swi_pressure_dep_payload
            swi_pressure_dep_payload.append(
                {"id": well_id, **swi_gamma_cr}  # Unpack swi_gamma_cr dictionary
            )

        self.__edit_input_nrta_rel_perm_and_fcd_all_wells_project(rel_perm_fcd_payload)
        self.__edit_swi_pressure_dep_all_wells_in_project(swi_pressure_dep_payload)

    def __edit_swi_pressure_dep_all_wells_in_project(
        self, payload: dict
    ) -> requests.Response:
        """
        Edit the NRTA input for all wells in project.
        """
        response = requests.patch(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        # if response.status_code >= 200 and response.status_code < 300:
        #     print(f"successfully edited info for well {well_id}")
        # else:
        # print(response.text)
        return response

    def __edit_input_nrta_rel_perm_and_fcd_all_wells_project(
        self, payload: dict
    ) -> requests.Response:
        """
        Edit the NRTA input for all wells in project.
        """
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/rta_input",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
            # params={"project_id": self.project_id},
        )
        # if response.status_code >= 200 and response.status_code < 300:
        #     print(f"successfully edited input quick for well {well_id}")
        # else:
        #     print(response.text)
        return response

    def _run_nrta_on_all_wells_in_project(
        self,
        project_id: int,
        params: dict = {"grid_refinement": "Low", "num_type_curves": "5_normal"},
        sleep_time: dict = 0,
    ):
        """
        Executes numerical rate transient analysis (NRTA) on all wells in the current project.

        This method iterates through all wells in the project and performs NRTA on each well
        using the specified parameters.

        Parameters:
        ----------
        params : dict, optional
        A dictionary containing parameters for the NRTA. Defaults to:
        {
            "grid_refinement": "Low",
            "num_type_curves": "5_normal"
        }
        - "grid_refinement" (str): Specifies the level of grid refinement for the NRTA. Possible values are "Low", "Medium", and "High".
        - "num_type_curves" (str): Specifies the number and type of curves to be used in the analysis. The default value is "5_normal".

        sleep_time : int, optional
        The amount of time to wait (in seconds) between processing each well. Defaults to 0.
        """

        wells = self.get_wells(project_id)
        for well in wells:
            time.sleep(sleep_time)
            well_id = well["id"]
            self.run_numerical_rta_for_well(well_id, params)

    def run_numerical_rta_for_well(
        self,
        well_id: int,
        params: dict = {"grid_refinement": "Low", "num_type_curves": "5_normal"},
    ) -> requests.Response:
        """
        Runs numerical rate transient analysis (NRTA) for a specified well.

        This method sends a request to the Whitson API to initiate NRTA for the well identified by `well_id`
        using the given parameters.

        Parameters:
        ----------
        well_id : int
            The ID of the well for which to run the NRTA.
        params : dict, optional
            A dictionary containing parameters for the NRTA. Defaults to:
            {
                "grid_refinement": "Low",
                "num_type_curves": "5_normal"
            }
            - "grid_refinement" (str): Specifies the level of grid refinement for the NRTA. Possible values are "Low", "Medium", and "High".
            - "num_type_curves" (str): Specifies the number and type of curves to be used in the analysis. The default value is "5_normal".

        """

        response = requests.get(
            f"https://{self.client_name}.whitson.com//api-external/v1/wells/{well_id}/run_rta",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params=params,
        )
        # if response.status_code == 202:
        #     print(f"Numerical RTA for well {well_id}")
        # else:
        #     print(response.text)
        return response

    def run_numerical_rta_autofit(self, well_id: int) -> requests.Response:
        """
        Runs the numerical rate transient analysis (NRTA) autofit for a specified well.

        This method sends a request to the Whitson API to perform an autofit NRTA on the well
        identified by `well_id`.

        Parameters:
        ----------
        well_id : int
            The ID of the well for which to run the NRTA autofit.
        """

        response = requests.get(
            f"https://{self.client_name}.whitson.com//api-external/v1/wells/{well_id}/autofit_rta",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        # time.sleep(0.05)
        # if response.status_code == 200:
        #     print(f"Numerical RTA Autofit successful for well {well_id}")
        # else:
        #     print(response.text)
        return response

    def _run_nrta_autofit_on_all_wells_in_project(
        self, project_id: int, sleep_time: int = 0
    ):
        """
        Runs the numerical rate transient analysis (NRTA) autofit for all wells in a specified project.

        This method retrieves all wells associated with the given `project_id` and performs an NRTA autofit
        on each well.

        Parameters:
        ----------
        project_id : int
            The ID of the project for which to run the NRTA autofit on all wells.

        sleep_time : int, optional
        The amount of time to wait (in seconds) between processing each well. Defaults to 0.
        """

        wells = self.get_wells(project_id)
        for well in wells:
            time.sleep(sleep_time)
            well_id = well["id"]
            self.run_numerical_rta_autofit(well_id)

    def _update_nrta_input_parameters(self, min_values, max_values, params):
        """
        Update NRTA input parameters with small random variations.

        This method adjusts each parameter by adding a random value within a defined
        jump size range, ensuring the updated parameters remain within specified minimum
        and maximum values.

        Parameters
        ----------
        min_values : list or array-like
            The minimum values for each parameter.
        max_values : list or array-like
            The maximum values for each parameter.
        params : list or array-like
            The current parameters to be updated.

        Returns
        -------
        tuple
            A tuple of updated parameters, each adjusted by a small random amount
            and constrained within the provided minimum and maximum values.
        """
        # Define the jump size
        jump_size = [
            (max_val - min_val) / 5 for min_val, max_val in zip(min_values, max_values)
        ]

        # Update each parameter with a small random amount within its specified range
        updated_parameters = [
            param + random.uniform(-jump, jump)
            for param, jump in zip(params, jump_size)
        ]

        # Ensure the updated values stay within the specified range
        updated_parameters = [
            max(min_val, min(max_val, updated_param))
            for updated_param, min_val, max_val in zip(
                updated_parameters, min_values, max_values
            )
        ]

        return tuple(updated_parameters)

    def _get_total_project_error(self, project_id: int, weights: dict):
        """
        Calculate the total project error and individual well errors for a given project.

        This method computes the total error for a project by aggregating individual well
        errors. It also returns the LFP (Last Flowing Pressure) and OOIP (Original Oil in Place)
        values for each well.

        Parameters
        ----------
        project_id : int
            The unique identifier of the project for which the error is being calculated.
        weights : dict
            A dictionary containing weights for different error components used in the
            error calculation.

        Returns
        -------
        tuple
            A tuple containing:
            - error (float): The average total error for the project.
            - individual_errors (list of float): The average individual errors for each well.
            - lfps (list of float): The Last Flowing Pressure values for each well.
            - ooips (list of float): The Original Oil in Place values for each well.
        """
        wells = self.get_wells(project_id)

        error = 0
        cumulative_individual_errors = []
        lfps = []
        ooips = []

        all_nrta_data = self._get_nrta_outputs_for_project(project_id)
        all_nrta_data = sorted(all_nrta_data, key=lambda x: x["well_id"])

        lfps = [entry["lfp"] for entry in all_nrta_data]
        ooips = [entry["ooip"] for entry in all_nrta_data]

        get_all_errors_in_project = self._get_all_errors_in_project(project_id)

        for well in get_all_errors_in_project:
            this_error, individual_errors = self._get_nrta_error(well, weights)
            error += this_error

            if not cumulative_individual_errors:
                cumulative_individual_errors = individual_errors
            else:
                cumulative_individual_errors = [
                    sum(x) for x in zip(cumulative_individual_errors, individual_errors)
                ]

        error = error / len(wells)
        individual_errors = [
            error / len(wells) for error in cumulative_individual_errors
        ]

        return error, individual_errors, lfps, ooips

    def _get_nrta_outputs_for_project(self, project_id: int):
        """
        Retrieve NRTA outputs for a specific project.

        This method fetches LFP (Last Flowing Pressure), OOIP (Original Oil in Place),
        OGIP (Original Gas in Place), and OGIP_a for the given project using the API.

        Parameters
        ----------
        project_id : int
            The unique identifier of the project.

        Returns
        -------
        list
            A list of dictionaries containing NRTA output data for each well in the project.

        Raises
        ------
        Exception
            If no wells are found for the given project.
        """
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/rta_calc"
        )
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )
        res = response.json()
        if not res:
            raise Exception("no existing wells")
        return res

    def get_nrta_outputs_for_well(self, well_id: int):
        """
        Retrieve NRTA outputs for a specific well..

        This method fetches LFP (Last Flowing Pressure), OOIP (Original Oil in Place),
        OGIP (Original Gas in Place), and OGIP_a for the given well using the API.

        Parameters
        ----------
        well_id : int
            The unique identifier of the project.

        Returns
        -------
        list
            A list of dictionaries containing NRTA output data for each well in the project.

        Raises
        ------
        Exception
            If no wells are found for the given project.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/rta_calc"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        res = response.json()
        if not res:
            raise Exception("no existing wells")
        return res

    def get_nrta_outputs_for_wells_in_project(self, project_id: int):
        """
        Retrieve NRTA outputs for multiple wells in a project.

        This method fetches LFP (Last Flowing Pressure), OOIP (Original Oil in Place),
        OGIP (Original Gas in Place), and OGIP_a for the given well using the API.

        Parameters
        ----------
        project_id : int
            The unique identifier of the project

        Returns
        -------
        list
            A list of dictionaries containing NRTA output data for each well in the project.

        Raises
        ------
        Exception
            If no wells are found for the given project.
        """
        base_url = (
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/rta_calc"
        )
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )
        res = response.json()
        if not res:
            raise Exception("no existing wells")
        return res

    def _get_all_errors_in_project(self, project_id: int):
        """
        Retrieve all errors for a specific project.

        This method fetches error data for all wells in the specified project using the API.

        Parameters
        ----------
        project_id : int
            The unique identifier of the project.

        Returns
        -------
        list
            A list of dictionaries containing error data for each well in the project.

        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells//rta_autofit_rms"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )
        res = response.json()
        if not res:
            raise Exception("no existing wells")
        return res

    def _get_nrta_error(self, res, weights: dict):
        """
        Calculate NRTA error for a given well.

        This method computes the error for each run based on the provided results and weights.

        Parameters
        ----------
        res : dict
            A dictionary containing the results for a specific well.
        weights : dict
            A dictionary containing weights for different error components.

        Returns
        -------
        tuple
            A tuple containing:
            - tot_error (float): The total calculated error for the well.
            - individual_errors (list of float): The individual error values for each component.
        """

        individual_errors = [value for key, value in res.items()]

        res.pop("well_id", None)

        tot_error = math.log10(
            sum(((weights[key] * value)) for key, value in res.items())
        )

        return tot_error, individual_errors

    def _append_to_nrta_report(
        self,
        result_df,
        run,
        test_parameters,
        this_error,
        probability_to_accept,
        was_accpeted,
        individual_errors,
        lfps,
        ooips,
    ):
        """
        Append NRTA run results to the report DataFrame.

        This method adds the results of an NRTA run, including test parameters, error metrics,
        and other relevant data, to the provided result DataFrame.

        Parameters
        ----------
        result_df : pd.DataFrame
            The DataFrame to which the results will be appended.
        run : int
            The run identifier.
        test_parameters : list
            The list of test parameters used in the run.
        this_error : float
            The total error for the run.
        probability_to_accept : float
            The probability of accepting the run.
        was_accpeted : bool
            Whether the run was accepted.
        individual_errors : list of float
            The list of individual errors for each error component.
        lfps : list of float
            The Last Flowing Pressure values for the wells.
        ooips : list of float
            The Original Oil in Place values for the wells.

        Returns
        -------
        pd.DataFrame
            The updated result DataFrame with the new run results appended.
        """
        warnings.simplefilter(action="ignore", category=FutureWarning)
        row = {
            "Run": run,
            "swi": test_parameters[0],
            "fcd": test_parameters[1],
            "swc": test_parameters[2],
            "sorw": test_parameters[3],
            "sorg": test_parameters[4],
            "sgc": test_parameters[5],
            "nw": test_parameters[6],
            "now": test_parameters[7],
            "ng": test_parameters[8],
            "nog": test_parameters[9],
            "Error": this_error,
            "Probability_to_Accept": probability_to_accept,
            "Was_Accepted": was_accpeted,
            "cum_oil_error": individual_errors[4],
            "cum_gas_error": individual_errors[0],
            "cum_water_error": individual_errors[6],
            "cum_gor_error": individual_errors[2],
            "oil_error": individual_errors[5],
            "gas_error": individual_errors[1],
            "water_error": individual_errors[7],
            "gor_error": individual_errors[3],
        }

        row_string = (
            f"{run}, "
            f"{test_parameters[0]:.4f}, "
            f"{test_parameters[1]:.4f}, "
            f"{test_parameters[2]:.4f}, "
            f"{test_parameters[3]:.4f}, "
            f"{test_parameters[4]:.4f}, "
            f"{test_parameters[5]:.4f}, "
            f"{test_parameters[6]:.4f}, "
            f"{test_parameters[7]:.4f}, "
            f"{test_parameters[8]:.4f}, "
            f"{test_parameters[9]:.4f}, "
            f"{this_error:.4f}, "
            f"{probability_to_accept:.4f}, "
            f"{was_accpeted}, "
            f"{individual_errors[4]:.4f}, "
            f"{individual_errors[0]:.4f}, "
            f"{individual_errors[6]:.4f}, "
            f"{individual_errors[2]:.4f}, "
            f"{individual_errors[5]:.4f}, "
            f"{individual_errors[1]:.4f}, "
            f"{individual_errors[7]:.4f}, "
            f"{individual_errors[3]:.4f}"
        )

        print(row_string)

        # Concatenate all lfps and ooips into strings separated by commas
        lfps_str = ", ".join(map(str, lfps))
        ooips_str = ", ".join(map(str, ooips))

        # Add lfps and ooips to the row dictionary
        row["lfps"] = lfps_str
        row["ooips"] = ooips_str

        # Append the row to the result DataFrame
        row_df = pd.DataFrame([row])
        result_df = pd.concat([result_df, row_df], ignore_index=True)

        return result_df

    def get_nrta_calculation_status(self, well_ids: list[int]) -> requests.Response:
        """
        Get Numerical RTA calculation status for list of wells {well_ids}.

        Example params:
        well_ids = [1, 2, 3]

        Example function call:
        response = whitson_connection.get_nrta_calculation_status(well_ids)
        """
        url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/nrta_calculation_status"
        response = requests.patch(
            url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={"well_ids": well_ids},
        )
        if response.status_code >= 200 and response.status_code < 300:
            return response
        else:
            print(response.text)

    # ---------------------------------------------------------------------------------------------------------
    # DCA related Functions
    # ---------------------------------------------------------------------------------------------------------

    def get_dca_fits(self, well_id: int) -> requests.Response:
        """
        Get (existing) DCA fits for well {well_id}.

        Example params:
        this_well_id = integer

        Example function call:
        response = whitson_connection.get_dca_fits(this_well_id)
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/well_dca/dca_export",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"DCA data successfully retrieved for well {well_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_dca_saved_cases(self, well_id: int) -> requests.Response:
        """
        Fetch saved Decline Curve Analysis (DCA) cases for a given well ID.

        Args:
            well_id (int): The unique identifier of the well.

        Returns:
            requests.Response: The API response containing the DCA data if successful.

        Example:
            >>> response = whitson_connection.get_dca_saved_cases(12345)
            >>> if response.status_code == 200:
            >>>     print(response.json())
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/well_dca/saved_cases",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(f"DCA data successfully retrieved for well {well_id}")
        else:
            print("Something went wrong - ", response)

        return response.json()

    def get_saved_dca_fits_by_well_id_list(
        self, well_id_list: dict, last_updated: str = None
    ) -> list[dict]:
        """
        Retrieve all saved DCA case for one or more wells using their IDs.

        This function sends a PATCH request to update or retrieve Decline Curve Analysis (DCA) fits
        for wells specified in the payload. The well IDs are provided in a dictionary format.
        If last_updated is provided, it will fetch DCA that has been updated from this date onwards.

        Parameters:
        -------
        well_id_list : dict
            A dictionary containing the IDs of the wells. The payload structure is:
            {
                "well_ids": [
                    <well_id_1>, <well_id_2>, ...
                ]
            }
        last_updated : str
            Format date with YYYY-MM-DD

        Returns:
        -------
        requests.Response
            The response object from the API call, containing the status and any data returned by the server.

        Example:
        --------
        Example payload:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Example function call:
        response = whitson_connection.get_saved_dca_fits_by_well_id_list(well_id_list, last_updated = "2025-12-02")

        More information:
        -----------------
        For details about the endpoint, visit:
        https://internal.whitson.com/api-external/swagger/#/DCA/patch_api_external_v1_wells_dca_saved_cases
        """

        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/dca_saved_cases"
        payload = well_id_list.copy()

        if last_updated is not None:
            payload["updated"] = last_updated

        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,  # enter the body json
        )

        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully retrieved saved DCA well(s) from {last_updated}.")
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError:
                print("Warning: Response is not valid JSON.")
                print("Response text:", response.text)
                return []
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print("Response text:", response.text)
            return []

    def get_dca_fits_by_well_id_list(
        self, well_id_list: dict, last_updated: str = None
    ) -> requests.Response:
        """
        Retrieve DCA fits for one or more wells using their IDs.

        This function sends a PATCH request to update or retrieve Decline Curve Analysis (DCA) fits
        for wells specified in the payload. The well IDs are provided in a dictionary format.
        If last_updated is provided, it will fetch DCA that has been updated from this date onwards.

        Parameters:
        -------
        well_id_list : dict
            A dictionary containing the IDs of the wells. The payload structure is:
            {
                "well_ids": [
                    <well_id_1>, <well_id_2>, ...
                ]
            }
        last_updated : str
            Format date with YYYY-MM-DD

        Returns:
        -------
        requests.Response
            The response object from the API call, containing the status and any data returned by the server.

        Example:
        --------
        Example payload:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Example function call:
        response = whitson_connection.get_dca_fits_by_well_id_list(well_id_list, last_updated = "2025-12-02")

        More information:
        -----------------
        For details about the endpoint, visit:
        https://internal.whitson.com/api-external/swagger/#/DCA/patch_api_external_v1_wells_dca
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/dca"
        payload = well_id_list.copy()

        if last_updated is not None:
            payload["updated"] = last_updated

        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )

        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully retrieved saved DCA well(s) from {last_updated}.")
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError:
                print("Warning: Response is not valid JSON.")
                print("Response text:", response.text)
                return []
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print("Response text:", response.text)
            return []

    def get_dca_daily_rates_by_well_id_list(
        self, well_id_list: dict
    ) -> requests.Response:
        """
        Retrieve Decline Curve Analysis (DCA) daily rates for specified wells.

        This method sends a GET request to fetch or update DCA daily rates for wells
        identified by their IDs. The well IDs should be provided in the payload in a dictionary format.

        Parameters:
        ----------
        well_id_list : dict
            A dictionary containing the well IDs in the following structure:
            {
                "well_ids": [
                    <well_id_1>, <well_id_2>, ...
                ]
            }

        Returns:
        -------
        requests.Response
            The response object from the API call, which includes the HTTP status, any error messages,
            and the returned data from the server.

        Example:
        --------
        Payload structure:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Function call:
        response = whitson_connection.get_dca_daily_rates_by_well_id_list(well_id_list)

        Endpoint Documentation:
        ------------------------
        Refer to the API documentation for more details:
        https://internal.whitson.com/api-external/swagger/#/DCA/get_api_external_v1_wells_dca_daily_rates

        Notes:
        ------
        - Ensure the `self.access_token` is valid for authentication.
        - The method prints a success message for responses with status codes in the 200-299 range.
        - Non-successful responses log the server's response text for troubleshooting.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/dca/daily_rates"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"well_ids": well_id_list},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved DCA forecasted daily rates.")
        else:
            print(response.text)
        return response.json()

    def get_dca_saved_cases_daily_rates_by_well_id_list(
        self, well_id_list: dict
    ) -> requests.Response:
        """
        Retrieve saved cases Decline Curve Analysis (DCA) daily rates for specified wells.

        This method sends a GET request to fetch or update DCA daily rates for wells
        identified by their IDs. The well IDs should be provided in the payload in a dictionary format.

        Parameters:
        ----------
        well_id_list : dict
            A dictionary containing the well IDs in the following structure:
            {
                "well_ids": [
                    <well_id_1>, <well_id_2>, ...
                ]
            }

        Returns:
        -------
        requests.Response
            The response object from the API call, which includes the HTTP status, any error messages,
            and the returned data from the server.

        Example:
        --------
        Payload structure:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Function call:
        response = whitson_connection.get_dca_saved_cases_daily_rates_by_well_id_list(well_id_list)

        Endpoint Documentation:
        ------------------------
        Refer to the API documentation for more details:
        https://internal.whitson.com/api-external/swagger/#/DCA/get_api_external_v1_wells_dca_saved_cases_daily_rates

        Notes:
        ------
        - Ensure the `self.access_token` is valid for authentication.
        - The method prints a success message for responses with status codes in the 200-299 range.
        - Non-successful responses log the server's response text for troubleshooting.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/dca_saved_cases/daily_rates"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params=well_id_list,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved saved cases DCA forecasted daily rates.")
        else:
            print(response.text)
        return response.json()

    def get_dca_monthly_rates_by_well_id_list(
        self, well_id_list: dict
    ) -> requests.Response:
        """
        Retrieve Decline Curve Analysis (DCA) monthly rates for specified wells.

        This method sends a GET request to fetch or update DCA monthly rates for wells
        identified by their IDs. The well IDs should be provided in the payload in a dictionary format.

        Parameters:
        ----------
        well_id_list : dict
            A dictionary containing the well IDs in the following structure:
            {
                "well_ids": [
                    <well_id_1>, <well_id_2>, ...
                ]
            }

        Returns:
        -------
        requests.Response
            The response object from the API call, which includes the HTTP status, any error messages,
            and the returned data from the server.

        Example:
        --------
        Payload structure:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Function call:
        response = whitson_connection.get_dca_monthly_rates_by_well_id_list(well_id_list)

        Endpoint Documentation:
        ------------------------
        Refer to the API documentation for more details:
        https://internal.whitson.com/api-external/swagger/#/DCA/get_api_external_v1_wells_dca_monthly_rates

        Notes:
        ------
        - Ensure the `self.access_token` is valid for authentication.
        - The method prints a success message for responses with status codes in the 200-299 range.
        - Non-successful responses log the server's response text for troubleshooting.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/dca/monthly_rates"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params=well_id_list,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved DCA forecasted monthly rates.")
        else:
            print(response.text)
        return response.json()

    def get_dca_saved_cases_monthly_rates_by_well_id_list(
        self, well_id_list: dict
    ) -> requests.Response:
        """
        Retrieve saved cases Decline Curve Analysis (DCA) monthly rates for specified wells.

        This method sends a GET request to fetch or update DCA monthly rates for wells
        identified by their IDs. The well IDs should be provided in the payload in a dictionary format.

        Parameters:
        ----------
        well_id_list : dict
            A dictionary containing the well IDs in the following structure:
            {
                "well_ids": [
                    <well_id_1>, <well_id_2>, ...
                ]
            }

        Returns:
        -------
        requests.Response
            The response object from the API call, which includes the HTTP status, any error messages,
            and the returned data from the server.

        Example:
        --------
        Payload structure:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Function call:
        response = whitson_connection.get_dca_saved_cases_monthly_rates_by_well_id_list(well_id_list)

        Endpoint Documentation:
        ------------------------
        Refer to the API documentation for more details:
        https://internal.whitson.com/api-external/swagger/#/DCA/get_api_external_v1_wells_dca_saved_cases_monthly_rates

        Notes:
        ------
        - Ensure the `self.access_token` is valid for authentication.
        - The method prints a success message for responses with status codes in the 200-299 range.
        - Non-successful responses log the server's response text for troubleshooting.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/dca_saved_cases/monthly_rates"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params=well_id_list,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved saved cases DCA forecasted monthly rates.")
        else:
            print(response.text)
        return response.json()

    def get_mass_well_custom_results(self, well_id_list: list) -> requests.Response:
        """
        Retrieve summary results for specified wells.

        This method sends a PATCH request to fetch summary of results of wells
        identified by their IDs. The well IDs should be provided in the payload in a dictionary format.

        Parameters:
        ----------
        well_id_list : list
            A list containing the well IDs in the following structure:
           [
                <well_id_1>, <well_id_2>, ...
            ]

        Returns:
        -------
        requests.Response
            The response object from the API call, which includes the HTTP status, any error messages,
            and the returned data from the server.

        Example:
        --------
        Payload structure:
        well_id_list = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Function call:
        response = whitson_connection.get_mass_well_custom_results(well_id_list)

        Endpoint Documentation:
        ------------------------

        Notes:
        ------
        - Ensure the `self.access_token` is valid for authentication.
        - The method prints a success message for responses with status codes in the 200-299 range.
        - Non-successful responses log the server's response text for troubleshooting.
        """
        payload = {"well_ids": well_id_list}
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/mass_export_custom_results"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved wells summary results.")
        else:
            print(response.text)
        return response.json()

    def get_auto_forecast_setting(self, auto_forecast_id: int) -> requests.Response:
        """
        Get auto forecast setting for an auto-forecast id
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/auto_forecasts/{auto_forecast_id}"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved auto forecast setting.")
            return response.json()
        else:
            print(response.text)
            return None

    def run_auto_forecast(self, auto_forecast_id: int) -> requests.Response:
        """
        Run auto forecast autofit for autoforecast id provided
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/auto_forecasts/{auto_forecast_id}/autofit"
        response = requests.put(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully send auto forecast autofit request to app.")
        else:
            print(response.text)
        return response

    def get_auto_forecast_results(
        self, auto_forecast_id: int, well_ids=None, fit_start=True
    ) -> requests.Response:
        """
        Get auto forecast results for each fluid phase
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/auto_forecasts/{auto_forecast_id}/tables"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"fit_start": fit_start},
            json={"well_ids": well_ids},
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(f"successfully retrieved auto forecast results.")
            return response.json()
        else:
            print(response.text)
            return None

    def get_auto_forecast_monthly_rates(
        self, auto_forecast_id: int, well_ids=None
    ) -> requests.Response:
        """
        Get auto forecast results for each fluid phase
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/auto_forecasts/{auto_forecast_id}/monthly_rates"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={"well_ids": well_ids},
        )
        if response.status_code == 200:
            print(f"successfully retrieved auto forecast monthly rates.")
            return response.json()
        else:
            print(response.text)
            return None

    # ---------------------------------------------------------------------------------------------------------
    # FMB related Functions
    # ---------------------------------------------------------------------------------------------------------

    def get_fmb_interpretation_in_project(self, project_id: int):
        """
        Retrieve Multiphase FMB outputs for multiple wells in a project.

        Parameters
        ----------
        project_id : int
            The unique identifier of the project

        Returns
        -------
        list
            A list of dictionaries containing Multiphase FMB output data for each well in the project.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/multiphase_fmb_interpretations"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Something went wrong: {response}")
            return response

    def get_fmb_pavg(self, well_id: int):
        """
        Retrieve Multiphase FMBp average for a well given its well_id.

        Parameters
        ----------
        well_id : int
            The unique identifier of the well

        Returns
        -------
        list
            A list of dictionaries containing Multiphase FMB p average data for a well.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/multiphase_fmb_pavg"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Something went wrong: {response}")
            return response

    def get_fmb_pavg_bulk(
        self, well_ids: dict, from_date: str = None, end_date: str = None
    ):
        """
        Retrieve Multiphase FMBp average for well ids with date filter.

        Parameters
        ----------
        well_ids : dict
            The unique identifier of the well
        from_date : str
        end_date : str

        Example:
        --------
        Payload structure:
        well_ids = {
            "well_ids": [
                123, 456, 789
            ]
        }

        Function call:
        response = whitson_connection.get_mass_well_custom_results(well_ids, from_date, end_date)

        Returns
        -------
        list
            A list of dictionaries containing Multiphase FMB p average data for wells.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/multiphase_fmb_pavg"
        response = requests.patch(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=well_ids,
            params={"from_date": from_date, "end_date": end_date},
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Something went wrong: {response}")
            return response

    def get_fmb_pavg_in_project(self, project_id: int):
        """
        Retrieve Multiphase FMBp average for multiple wells in a project.

        Parameters
        ----------
        project_id : int
            The unique identifier of the project

        Returns
        -------
        list
            A list of dictionaries containing Multiphase FMB p average data for each well in the project.
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/multiphase_fmb_pavg"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params={"project_id": project_id},
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Something went wrong: {response}")
            return response

    def run_fmb_calc(self, well_id: int):
        """
        Runs the calculation for Multiphase FMB and/or bottom hole pressure

        Parameters
        ----------
        well_id : int
            The unique identifier of the well

        Returns
        -------
        API response
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_multiphase_fmb"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if response.status_code == 202:
            print(f"successfully ran bhp calc and/or fmb on well: {well_id}")
            return response
        else:
            print(response.text)
            return None

    def run_fmb_calc_bulk(self, well_ids: list[int]):
        """
        Runs the calculation for Multiphase FMB and/or bottom hole pressure

        Parameters
        ----------
        well_id : int
            The unique identifier of the well

        Returns
        -------
        API response
        """
        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/run_multiphase_fmb"
        response = requests.post(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={"well_ids": well_ids},
        )
        if response.status_code == 202:
            print(f"Sent {len(well_ids)} fmb calculations to the listener")
            return response
        else:
            print(response.text)
            return None

    # ---------------------------------------------------------------------------------------------------------
    # Nodal Analysis Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def run_ipr_with_date(
        self,
        well_id: int,
        from_date: str = None,
        end_date: str = None,
        reservoir_pressure: float = None,
    ):
        """
        Runs the IPR and VLP calculation for a well_id. If no date is provided, it will use the latest date.
        If only from_date or end_date is provided, it will use that date. If from_date and end_date is provided,
        it will use the average productions between the dates.

        Parameters
        ----------
        well_id : int
            The unique identifier of the well
        from_date: str
            Date in string format (2025-12-25). Determines the start period for averaging the production.
        end_date: str
            Date in string format (2025-12-25). Determines the end period for averaging the production.
        reservoir_pressure: float
            Optional custom reservoir pressure to be inputted.

        Returns
        -------
        IPR input for the nodal analysis
        """

        params = {}
        if from_date and end_date:
            params["from_date"] = from_date
            params["date"] = end_date
        elif from_date or end_date:
            params["date"] = from_date or end_date

        if reservoir_pressure:
            params["reservoir_pressure"] = reservoir_pressure

        base_url = f"https://{self.client_name}.whitson.com/api-external/v1/wells/{well_id}/run_ipr_with_date"
        response = requests.get(
            base_url,
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            params=params,
        )
        if response.status_code == 200:
            print(
                f"Successfully calculated IPR and send VLP calculation to listener for well id {well_id}"
            )
            return response.json()
        else:
            print(response.text)
            return None

    # ---------------------------------------------------------------------------------------------------------
    # Nodal Analysis Related Functions
    # ---------------------------------------------------------------------------------------------------------

    def get_mwgl_opt_project_id(self, project_id: int):
        """
        Retrieves all multi-well gas lift optimizers and associated areas within the specified project

        Parameters
        ----------
        project_id : int
            The unique identifier of the project

        Returns
        -------
        >>>
        [
            {
                "company_wide": true,
                "id": 0,
                "multi_well_gas_lifts": [
                {
                    "company_wide": true,
                    "id": 0,
                    "multi_well_gas_lift_area_id": 0,
                    "name": "string",
                    "note": "string",
                    "owners": [
                    "string"
                    ],
                    "well_ids": [
                    0
                    ]
                }
                ],
                "name": "string",
                "note": "string",
                "owners": [
                "string"
                ],
                "project_id": 0
            }
        ]
        >>>
        """
        response = requests.get(
            f"https://{self.client_name}.whitson.com/api-external/v1/projects/{project_id}/multi_well_gas_lift_opt/list",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        if response.status_code == 200:
            print(
                f"MWGL data successfully retrieved for MWGL project in project ID {project_id} "
            )
        else:
            print("Something went wrong - ", response)

        return response.json()

    def put_mwgl_config_settings(self, multi_well_gas_lift_id: int, payload: dict):
        """
        Update the configuration settings for the multi-well gas lift optimizer.

        Parameters
        ----------
        multi_well_gas_lift_id : int
            The unique identifier of the multi-well gas lift project

        Payload
        ----------
        >>>
        {
            "multi_well_gas_lift_id": 0,
            "injection_rates": 0,
            "injection_rate_days": 0,
            "injection_rate_type": "last_daily_aggregate",
            "minimum_injection_per_well": "none",
            "critical_rate_depth": "wellhead",
            "critical_rate_depth_value_user_default": 0,
            "reservoir_pressure_source": "automatic",
            "bhp_correlation": "beggs_and_brill",
            "rates_and_pressure": "last_production_data",
            "rates_and_pressure_last_production_days": 10,
            "maximum_injection_per_well": "none",
            "erosional_rate_depth": "wellhead",
            "oil_price": 0,
            "gas_price": 0,
            "gas_cost": 0,
            "active_economic_optimization": true,
            "erosional_rate_depth_value_user_default": 0,
            "well_ids": [
                0
            ]
        }
        >>>

        Returns
        ----------
        Response
        """
        response = requests.put(
            f"https://{self.client_name}.whitson.com/api-external/v1/multi_well_gas_lift_opt/{multi_well_gas_lift_id}/settings",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json=payload,
        )
        if response.status_code >= 200 and response.status_code < 300:
            print(
                f"Successfully edited multi-well gas lift in mwgl project ID {multi_well_gas_lift_id}"
            )
        else:
            print(response.text)
        return response

    # ---------------------------------------------------------------------------------------------------------
    # SNOWFLAKE Related Functions (https://www.snowflake.com/en/)
    # ---------------------------------------------------------------------------------------------------------

    def snowflake_connection(
        self,
        account: str,
        user: str,
        password,
        database: str,
        schema: str = "PUBLIC",
        warehouse: str = "XS",
        role: str = "ACCOUNTADMIN",
    ):
        """
        Creates a connection to Snowflake using the provided credentials and parameters.

        Parameters:
        - account (str): The Snowflake account identifier, typically in the format '<account_identifier>.<region>.<cloud_provider>'.
        - user (str): The Snowflake user name.
        - password (str): The password for the Snowflake user.
        - database (str): The name of the Snowflake database to connect to.
        - schema (str): The schema within the database. Default is 'PUBLIC'.
        - warehouse (str): The name of the Snowflake warehouse to use. Default is 'XS'.
        - role (str): The role to use for the connection. Default is 'ACCOUNTADMIN'.

        Returns:
        - connection: A connection object to the Snowflake database.
        """
        engine = create_engine(
            URL(
                account=account,
                user=user,
                password=password,
                database=database,
                schema=schema,
                warehouse=warehouse,
                role=role,
            )
        )

        return engine.connect()

    def snowflake_connection_key_pair(
        self,
        account: str,
        user: str,
        private_key_path: str,
        database: str,
        schema: str = "PUBLIC",
        warehouse: str = "XS",
        role: str = "ACCOUNTADMIN",
    ):
        """
        Creates a connection to Snowflake using the provided credentials and parameters.

        Parameters:
        - account (str): The Snowflake account identifier, typically in the format '<account_identifier>.<region>.<cloud_provider>'.
        - user (str): The Snowflake user name.
        - password (str): Path to the private key file (.p8).
        - database (str): The name of the Snowflake database to connect to.
        - schema (str): The schema within the database. Default is 'PUBLIC'.
        - warehouse (str): The name of the Snowflake warehouse to use. Default is 'XS'.
        - role (str): The role to use for the connection. Default is 'ACCOUNTADMIN'.

        Returns:
        - connection: A connection object to the Snowflake database.
        """

        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(), backend=default_backend(), password=None
            )

        pk_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        engine = create_engine(
            URL(
                account=account,
                user=user,
                database=database,
                schema=schema,
                warehouse=warehouse,
                role=role,
            ),
            connect_args={
                "private_key": pk_bytes,
            },
        )

        return engine.connect()

    def snowflake_table_to_dataframe(
        self, snowflake_connection, snowflake_query: str
    ) -> pd.DataFrame:
        """
        Executes a query on a Snowflake connection and returns the result as a pandas DataFrame.

        Parameters:
        - snowflake_connection: An active SQLAlchemy connection object to Snowflake.
        - snowflake_query (str): The SQL query to execute on the Snowflake database.

        Returns:
        - pd.DataFrame: A pandas DataFrame containing the results of the executed query.

        Example:
        >>> connection = create_snowflake_connection(account='your_account', user='your_user', password='your_password', database='your_database')
        >>> query = "SELECT * FROM your_database.your_schema.your_table"
        >>> df = snowflake_table_to_dataframe(connection, query)
        >>> print(df.head())

        Notes:
        - Ensure that the Snowflake connection is active and properly configured before calling this function.
        - The function fetches all rows from the result set, so be mindful of the query size to avoid memory issues.
        """
        # snowflake_connection.execute("USE WAREHOUSE XS")
        # Explicitly activate the warehouse
        snowflake_query = text(snowflake_query)
        result = snowflake_connection.execute(snowflake_query)
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns)

    def snowflake_table_to_dataframe_batched(
        self, snowflake_connection, snowflake_query: str
    ) -> pd.DataFrame:
        snowflake_query = text(snowflake_query)
        cursor = snowflake_connection.connection.cursor()
        try:
            cursor.execute(str(snowflake_query))
            batches = cursor.fetch_pandas_batches()
            df = pd.concat(batches, ignore_index=True)
        except Exception as e:
            print(f"An error occured: {e}")

        return df

    # ---------------------------------------------------------------------------------------------------------
    #  DATABRICKS Related Functions (https://www.databricks.com/)
    # ---------------------------------------------------------------------------------------------------------
    def databricks_connection(
        self, dsn: str, host: str, port: str, token: str, http_path: str
    ):
        """Create a connection string to connect to a Databricks cluster."""
        connection_string = (
            f"DSN={dsn};"
            f"HOST={host};"
            f"PORT={port};"
            f"OAuthMechanism=3;"  # OAuth mechanism for token-based authentication
            f"Auth_AccessToken={token};"  # Use Auth_AccessToken instead of Token
            f"HTTPPath={http_path};"
            f"SSL=1;"
        )

        return pyodbc.connect(connection_string, autocommit=True)

    def databricks_connection_oauth2(
        self,
        dsn: str,
        host: str,
        port: str,
        http_path: str,
        client_id: str,
        client_secret: str,
    ):
        """Create a connection string to connect to a Databricks cluster using OAuth2 authentication."""
        connection_string = (
            f"DSN={dsn};"
            f"HOST={host};"
            f"PORT={port};"
            f"AuthMech=11;"  # Authentication mechanism for OAuth2
            f"Auth_Flow=1;"  # Specifies OAuth2 flow
            f"Auth_Client_ID={client_id};"  # Updated to match the required format
            f"Auth_Client_Secret={client_secret};"  # Updated to match the required forma
            f"HTTPPath={http_path};"
            f"SSL=1;"
        )

        return pyodbc.connect(connection_string, autocommit=True)

    def connect_to_databricks_sql(self, host: str, token: str, http_path: str):
        from databricks import sql

        """Create a connection to a Databricks SQL warehouse using `databricks-sql-connector`."""
        try:
            conn = sql.connect(
                server_hostname=host, http_path=http_path, access_token=token
            )
            print("✅ Connected to Databricks successfully!")
            return conn
        except Exception as e:
            print(f"❌ Failed to connect to Databricks: {e}")
            return None

    # ---------------------------------------------------------------------------------------------------------
    # PRODMAN related Functions (https://prodman.ca/)
    # ---------------------------------------------------------------------------------------------------------

    def prodman_get_wells(self, domain: str, api_key: str) -> json:
        """
        Fetches well data from the Prodman API for a given domain.

        This function sends a GET request to the Prodman API, using the provided domain and API key,
        to retrieve a list of wells in JSON format. The function returns the parsed JSON response if
        the request is successful, otherwise, it prints an error message and returns an empty list.

        Args:
            domain (str): The domain name to be used in the API request URL (e.g., 'example' for 'https://example.prodman.ca').
            api_key (str): The API key required for authentication to access the Prodman API.

        Returns:
            json: A list of well data in JSON format if the request is successful.
            If the request fails, an empty list is returned.

        More info about PRODMAN api can be found at https://YOURCOMPANYDOMAIN.prodman.ca/api/help.
        """
        params = {"api_key": api_key}

        url = f"https://{domain}.prodman.ca/api/wells/?return-type=json"
        response = requests.get(url, params=params)

        if response.status_code == 200:
            content = response.content.decode("utf-8")
            return json.loads(content)
        else:
            print(f"Error: {response.status_code}")
            return []

    def prodman_get_production(self, domain: str, api_key: str) -> json:
        """
        Fetches production data from the Prodman API for all wells within a specified date range.

        This function sends a GET request to the Prodman API, using the provided domain and API key,
        to retrieve production data for all wells. The data includes fields such as `entity_id`, `date`,
        `oil`, `gas`, and `water`. The results are returned in JSON format. If the request fails, the
        function prints an error message and returns an empty list.

        Args:
            domain (str): The domain name to be used in the API request URL (e.g., 'example' for 'https://example.prodman.ca').
            api_key (str): The API key required for authentication to access the Prodman API.

        Returns:
            json: A list of production data in JSON format if the request is successful.
            If the request fails, an empty list is returned.

        More info about PRODMAN api can be found at https://YOURCOMPANYDOMAIN.prodman.ca/api/help.
        """

        params = {
            "api_key": api_key,
            "well_id": "all",
            "start": "2000-01-01",
            "end": datetime.today().strftime("%Y-%m-%d"),
            "fields": "entity_id, date, oil, gas, water, casing, tubing, intake, jtf",
            "units": "us",
            "return-type": "json",
        }

        url = f"https://{domain}.prodman.ca/api/production/"
        response = requests.get(url, params=params)

        if response.status_code == 200:
            content = response.content.decode("utf-8")
            return json.loads(content)
        else:
            print(f"Error: {response.status_code}")
            return []

    def propman_canada_uwi_cleanup(self, uwi: str) -> str:
        """
        Cleans up a UWI string for Propman Canada.

        This function performs the following steps:
        1. Removes all occurrences of '-' and '/' from the input UWI string.
        2. Ensures the cleaned UWI string starts with '1'. If it does not, '1' is added at the beginning.
        3. Pads the UWI string with '0's at the end to make it exactly 16 characters long if it has fewer than 16 characters.
        4. Prints the processed UWI if modifications are made.
        5. Prints a warning if the cleaned UWI is already 16 characters or more without needing modifications.

        Parameters:
        uwi (str): The input UWI string to be cleaned and formatted.

        Returns:
        str: The cleaned and formatted UWI string.
        """
        # Replace '/' and '-' with an empty string using str.replace
        cleaned_uwi = uwi.replace("/", "").replace("-", "")

        # Ensure the UWI starts with '1'
        if not cleaned_uwi.startswith("1"):
            cleaned_uwi = "1" + cleaned_uwi

        # If the length is less than 16, pad with '0' at the end
        if len(cleaned_uwi) == 15:
            cleaned_uwi = cleaned_uwi.ljust(16, "0")
            print(f"Processed UWI: {cleaned_uwi}")
            return cleaned_uwi
        elif len(cleaned_uwi) == 16:
            return cleaned_uwi
        else:
            # print("Warning: The processed UWI already meets the requirements or is longer than 16 characters.")
            return cleaned_uwi

    # ---------------------------------------------------------------------------------------------------------
    # General Functions
    # ---------------------------------------------------------------------------------------------------------

    def round_to_significant_digits(self, number: float, digits: int = 4) -> float:
        """
        Rounds a number to the specified number of significant digits.

        Parameters:
        -----------
        number : float
            The number to be rounded.
        digits : int
            The number of significant digits to round to.

        Returns:
        --------
        float
            The number rounded to the specified number of significant digits.
            Returns 0 if the input number is 0 to avoid log10 errors.

        Examples:
        ---------
        >>> round_to_significant_digits(12345.6789, 3)
        12300

        >>> round_to_significant_digits(0.012345, 4)
        0.01235

        Notes:
        ------
        This function uses logarithmic calculations to determine the
        appropriate rounding level for the desired number of significant
        digits. It handles zero input separately to avoid mathematical
        issues with logarithms of zero.
        """
        if number == 0:
            return 0  # Return 0 if the input is 0 to avoid log10 issues

        significant_digits = digits - int(math.floor(math.log10(abs(number)))) - 1
        return round(number, significant_digits)

    def run_function(self, func):
        try:
            # Print the name of the function being run
            print(f"Starting {func.__name__} process.")

            # Capture the start time
            start_time = time.time()

            # Run the function
            func()

            # Capture the end time
            end_time = time.time()

            # Calculate the duration in seconds
            elapsed_time = end_time - start_time

            # Convert the time to minutes and seconds
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)

            # Print completion message with time
            time_string = f"{func.__name__} completed successfully in {minutes} minutes and {seconds} seconds.\n"
            print(time_string)

            return time_string

        except Exception as e:
            # Print an error message if the function fails
            error_string = f"Something went wrong while running {func.__name__}: {e}\n"
            print(error_string)

            return error_string

    def _convert_dataframe_to_timestamp_json(
        self, dataframe, primary_key="well_id", timestamp_column_name="insert_date"
    ) -> json:
        """"""
        dataframe = dataframe[[primary_key, timestamp_column_name]].copy()
        dataframe[timestamp_column_name] = pd.to_datetime(
            dataframe[timestamp_column_name]
        )
        dataframe = dataframe.loc[
            dataframe.groupby(primary_key)[timestamp_column_name].idxmax()
        ]
        return dataframe.to_json(orient="records", date_format="iso", indent=4)

    def _save_dataframe_timestamp_to_json(
        self,
        dataframe,
        filepath,
        primary_key="well_id",
        timestamp_column_name="insert_date",
    ):
        """"""
        dataframe = dataframe[[primary_key, timestamp_column_name]].copy()
        dataframe[timestamp_column_name] = pd.to_datetime(
            dataframe[timestamp_column_name]
        )
        dataframe = dataframe.loc[
            dataframe.groupby(primary_key)[timestamp_column_name].idxmax()
        ]
        dataframe.to_json(filepath, orient="records", date_format="iso", indent=4)

    def _find_new_or_updated_well_ids(
        self, old_list, new_list, primary_key, timestamp_key
    ):
        """
        Find primary keys where the timestamp differs or where new primary keys are present.

        Parameters:
        old_list (list): The old list of dictionaries containing primary keys and timestamps.
        new_list (list): The new list of dictionaries containing primary keys and timestamps.
        primary_key (str): The key representing the primary key in each dictionary.
        timestamp_key (str): The key representing the timestamp in each dictionary.

        Returns:
        list: A list of primary keys where either:
            - The primary key is present in the new list but not in the old (new record).
            - The primary key exists in both lists, but the timestamps differ (updated record).
        """
        if old_list is None:
            return [item[primary_key] for item in new_list]

        differences = []

        # Convert the lists to dictionaries for easier comparison
        old_dict = {item[primary_key]: item[timestamp_key] for item in old_list}
        new_dict = {item[primary_key]: item[timestamp_key] for item in new_list}

        # Find primary keys that are either new or have a different timestamp
        for key in new_dict:
            if key not in old_dict or old_dict[key] != new_dict[key]:
                differences.append(key)

        return differences

    def is_data_sync_needed(
        self, dataframe: pd.DataFrame, json_filename: str, client_name: str = None
    ) -> bool:
        """
        Determines if the provided DataFrame needs to be synced with the JSON file.
        If the file does not exist or if the JSON content differs, it updates the file
        and returns True (indicating syncing is necessary). Otherwise, returns False.

        Parameters:
        ----------
        dataframe : pd.DataFrame
            The DataFrame to be compared or saved.
        json_filename : str
            The name of the JSON file to compare against or save to.
        client_name : str
            The name of the client, used to create the file path dynamically.

        Returns:
        -------
        bool
            True if syncing is necessary (file does not exist or JSON is different),
            False if the data matches the existing file.

        Notes:
        -----
        - If the JSON file exists, the function reads its contents and compares it with the
        JSON representation of the provided DataFrame.
        - If the JSON file does not exist, the function creates it by dumping the
        provided DataFrame into the JSON file.
        - The JSON file is stored in the path: `scheduler/company/{client_name}/associated_files/{json_filename}.json`
        """

        client_name_to_use = (
            self.client_name.lower() if client_name == None else client_name
        )

        filepath = (
            os.path.dirname(os.path.abspath(__file__)).replace("aries_python_code", "")
            + f"scheduler\\company\\{client_name_to_use}\\associated_files\\{json_filename}.json"
        )

        if os.path.exists(filepath):

            # Load JSON from a file
            with open(filepath, "r") as file:
                df_from_json = json.load(file)

            # Convert the dataframe to JSON and parse it as a dictionary
            df_from_dataframe = json.loads(dataframe.to_json(orient="records"))

            # Check if they are equal
            is_equal = df_from_json == df_from_dataframe

            if is_equal:
                return False  # No need to sync if data is the same
            else:
                dataframe.to_json(filepath, orient="records", indent=4)
                return True  # Sync is needed if data is different
        else:
            dataframe.to_json(filepath, orient="records", indent=4)
            print(f"File created and data frame saved to {filepath}")
            return True  # Sync is needed if the file does not exist

    def get_dataframe_to_update(
        self,
        dataframe: pd.DataFrame,
        timestamp_filename: str,
        primary_key: str,
        timestamp_column_name: str,
        client_name: str = None,
    ) -> List[str]:
        """
        Identify well IDs from the provided DataFrame that are new or have updated timestamps
        by comparing them with records in an existing JSON timestamp file. If discrepancies
        are found (i.e., new or modified well IDs), the function returns these IDs and updates
        the JSON timestamp file.

        Parameters:
        dataframe (pd.DataFrame): The current DataFrame containing well IDs and their corresponding timestamps.
        timestamp_filename (str): The name of the JSON file (excluding path) to store historical well ID timestamps.
        primary_key (str): The column name in the DataFrame that uniquely identifies each well (e.g., 'well_id').
        timestamp_column_name (str): The column name in the DataFrame representing the timestamp for each well ID.
        client_name (str, optional): The name of the client used to construct the JSON file path. Defaults to None,
                                    in which case the object's `client_name` attribute is used.

        Returns:
        List[str]: A list of well IDs that are either new or have an updated timestamp in the DataFrame compared
                to the records in the JSON file.
        """
        client_name_to_use = (
            self.client_name.lower() if client_name == None else client_name
        )
        base_dir = os.path.dirname(os.path.abspath(__file__)).replace(
            "aries_python_code", ""
        )
        folder_path = os.path.join(
            base_dir, "scheduler", "company", client_name_to_use, "associated_files"
        )
        os.makedirs(folder_path, exist_ok=True)
        filepath = os.path.join(folder_path, f"{timestamp_filename}.json")
        new_timestamp = json.loads(
            self._convert_dataframe_to_timestamp_json(
                dataframe, primary_key, timestamp_column_name
            )
        )
        old_timestamp = json.load(open(filepath)) if os.path.exists(filepath) else None
        well_ids_to_update = self._find_new_or_updated_well_ids(
            old_timestamp, new_timestamp, primary_key, timestamp_column_name
        )
        self._save_dataframe_timestamp_to_json(
            dataframe, filepath, primary_key, timestamp_column_name
        )
        return dataframe[dataframe[primary_key].isin(well_ids_to_update)]

    def send_email(
        self,
        from_email: str,
        password: str,
        to_email: str,
        subject: str,
        body: str,
        smtp_server: str = "smtp.office365.com",
        smtp_port: int = 587,
    ) -> None:
        """
        Sends an email using the specified SMTP server.

        Parameters:
            from_email (str): The sender's email address.
            password (str): The sender's email account password.
            to_email (str): The recipient's email address.
            subject (str): The subject of the email.
            body (str): The body of the email, which will be formatted as HTML.
            smtp_server (str, optional): The SMTP server address. Default is "smtp.office365.com".
            smtp_port (int, optional): The SMTP server port. Default is 587.

        Returns:
            None
        """

        message = f"Subject: {subject}\n\n{body}"

        formatted_body = body.replace("\n", "<br>")
        html_body = f"""
        <html>
        <body>
            <p style="font-size:10px; font-family:Arial;">{formatted_body}</p>
        </body>
        </html>
        """

        message = f"Subject: {subject}\n"
        message += "MIME-Version: 1.0\n"
        message += "Content-Type: text/html\n\n"
        message += html_body

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(from_email, password)
            server.sendmail(from_email, to_email, message)

    def clean_deviation_survey_payload(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Cleans a list of dictionaries by removing duplicates based on the 'md' value, ensuring the list is sorted in increasing order,
        and removing any entry where 'md' or 'tvd' includes a NaN value.

        Parameters:
        data (List[Dict[str, Any]]): A list of dictionaries, where each dictionary contains 'md' and 'tvd' keys.

        Returns:
        List[Dict[str, Any]]: A cleaned and sorted list of dictionaries based on 'md'.
        """
        # Remove entries with NaN values for 'md' or 'tvd'
        data = [
            entry
            for entry in data
            if not (pd.isna(entry["md"]) or pd.isna(entry["tvd"]))
        ]

        # Convert 'md' and 'tvd' to floats
        data = [
            {"md": float(entry["md"]), "tvd": float(entry["tvd"])} for entry in data
        ]

        unique_data = {}
        for entry in data:
            if entry["md"] not in unique_data:
                unique_data[entry["md"]] = entry

        cleaned_data = sorted(unique_data.values(), key=lambda x: x["md"])
        return cleaned_data

    def get_new_well_ids_2(
        self,
        unique_well_ids_df: pd.DataFrame,
        json_filename: str,
        client_name: str = None,
    ) -> List[dict]:
        """
        Returns a list of new well IDs not already present in the saved JSON file for the specified client.
        Overwrites the JSON file with the new set of well IDs.

        Parameters:
        - unique_well_ids_df (pd.DataFrame): DataFrame containing the queried well IDs.
        - client_name (str): The name of the client for directory construction.
        - json_filename (str): The base name of the JSON file where well IDs are stored.

        Returns:
        - list: A list of new well IDs. Returns an empty list if no new well IDs are detected.
        """
        client_name_to_use = (
            self.client_name.lower() if client_name == None else client_name
        )

        # Determine the full JSON filepath
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)).replace("aries_python_code", ""),
            f"scheduler/company/{client_name_to_use}/associated_files/{json_filename}.json",
        )

        # Load existing well IDs from JSON if the file exists, else initialize an empty list
        if os.path.exists(filepath):
            with open(filepath, "r") as file:
                existing_well_ids = json.load(file)
        else:
            existing_well_ids = []

        # Convert the new well IDs to a dictionary format for comparison
        new_well_ids = json.loads(unique_well_ids_df.to_json(orient="records"))

        # Identify only new well IDs
        new_wells_only = [
            well for well in new_well_ids if well not in existing_well_ids
        ]

        # Overwrite the JSON with the new well IDs (latest dataset)
        unique_well_ids_df.to_json(filepath, orient="records", indent=4)

        return new_wells_only  # Returns list of new well IDs or empty list if none

    def _get_auth0_logs_old(
        self, per_page: int = 50, page: int = 0
    ) -> requests.Response:
        """
        Retrieve log events from Auth0 Management API.

        :param per_page: Number of logs per page (default: 50, max: 100).
        :param page: Page number for pagination (default: 0).

        Example function call:
        response = whitson_connection.get_auth0_logs(per_page=100, page=1)
        """

        url = f"https://{self.client_name}/api/v2/logs"
        params = {"per_page": per_page, "page": page, "sort": "date:-1", "type": "ssa"}

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            print(f"Successfully retrieved {len(response.json())} log entries.")
            return response.json()
        else:
            print("Failed to retrieve logs - ", response.text)
            return response.json()

    from datetime import datetime

    def _get_auth0_logs(self, per_page: int = 50) -> list:
        """
        Retrieve log events from Auth0 Management API with automatic pagination,
        filtering only logs from today.

        :param per_page: Number of logs per page (default: 50, max: 100).

        This function retrieves logs from today by filtering with the "q" parameter.

        Example function call:
        logs = whitson_connection.get_auth0_logs(per_page=100)
        """

        # Get today's date in UTC
        today = datetime.datetime.utcnow().date()
        today_start = f"{today}T00:00:00.000Z"
        today_end = f"{today}T23:59:59.999Z"

        url = f"https://{self.client_name}/api/v2/logs"
        params = {
            "per_page": per_page,
            "sort": "date:-1",
            "type": "ssa",
            "q": f"date:[{today_start} TO {today_end}]",  # Filter logs for today
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        all_logs = []

        while url:
            response = requests.get(
                url, headers=headers, params=params if "?" in url else None
            )

            if response.status_code != 200:
                print("Failed to retrieve logs -", response.text)
                break

            logs = response.json()
            all_logs.extend(logs)
            print(f"Retrieved {len(logs)} log entries. Total: {len(all_logs)}")

            # Extract 'next' URL from Link header
            link_header = response.headers.get("Link", "")
            next_url = None
            if link_header:
                links = link_header.split(", ")
                for link in links:
                    if 'rel="next"' in link:
                        next_url = link.split(";")[0].strip("<>")
                        break

            url = next_url  # Set next URL or exit if None

        return all_logs

    def get_new_well_ids(
        self,
        unique_well_ids_df: pd.DataFrame,
        json_filename: str,
        client_name: str = None,
    ) -> List[str]:
        """
        Returns a list of new well IDs not already present in the saved JSON file for the specified client.
        Updates the JSON file with the new set of unique well IDs, storing one well ID per line.

        Parameters:
        - unique_well_ids_df (pd.DataFrame): DataFrame containing the queried well IDs (must include a 'well_id' column).
        - json_filename (str): The base name of the JSON file where well IDs are stored.
        - client_name (str, optional): The name of the client for directory construction. Defaults to `self.client_name`.

        Returns:
        - List[str]: A list of new well IDs. Returns an empty list if no new well IDs are detected.
        """
        if "well_id" not in unique_well_ids_df.columns:
            raise ValueError("The DataFrame must contain a 'well_id' column.")

        client_name_to_use = (
            self.client_name.lower() if client_name is None else client_name
        )

        # Determine the full JSON filepath
        filepath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)).replace("aries_python_code", ""),
            f"scheduler/company/{client_name_to_use}/associated_files/{json_filename}.json",
        )

        # Load existing well IDs from JSON if the file exists
        if os.path.exists(filepath):
            with open(filepath, "r") as file:
                existing_well_ids = set(json.load(file))
        else:
            existing_well_ids = set()

        # Extract unique well IDs from the DataFrame
        new_well_ids = set(
            unique_well_ids_df["well_id"].astype(str)
        )  # Ensure all well_ids are strings

        # Identify only new well IDs
        new_wells_only = list(new_well_ids - existing_well_ids)

        # Update the JSON file with the latest unique well IDs
        updated_well_ids = list(existing_well_ids.union(new_well_ids))
        with open(filepath, "w") as file:
            json.dump(updated_well_ids, file, indent=4)

        return new_wells_only

    def _find_decimals(self, data):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, decimal.Decimal):
                    print(f"Decimal found at key '{key}' with value {value}")
                elif isinstance(value, dict):
                    self._find_decimals(value)
                elif isinstance(value, list):
                    for item in value:
                        self._find_decimals(item)
        elif isinstance(data, list):
            for item in data:
                self._find_decimals(item)

    def _get_h_f(self, well):
        """Determine the appropriate h_f value based on the condition of h and h_f fields."""
        return (
            well["h"]
            if pd.notnull(well["h"]) and well["h"] < well["h_f"]
            else well["h_f"]
        )

    def _get_percentage(self, value, default=30):
        """Convert value to percentage if less than 1, otherwise use as-is. Return default if value is NaN."""
        return (
            float(value) * 100
            if pd.notna(value) and float(value) <= 1
            else (float(value) if pd.notna(value) else default)
        )

    def _normalize_porosity(self, value, default=0.05):
        """Convert porosity to decimal if greater than 1, otherwise use as-is. Return default if value is NaN."""
        return (
            float(value) / 100
            if pd.notna(value) and float(value) > 1
            else (float(value) if pd.notna(value) else default)
        )

    def _convert_temperature(self, value, threshold=400):
        """Convert temperature from Fahrenheit to Celsius if above threshold. Return as-is if NaN or below threshold."""
        return value - 459.67 if pd.notnull(value) and value > threshold else value

    def _is_date_in_list(self, date, list_of_dicts):
        """Checks if date exists in any dictionary in list_of_dicts."""
        return date in {d["date"] for d in list_of_dicts}

    def git_pull_and_restart_if_updated(self):
        # Go to the root of the repo
        repo_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../")
        )
        os.chdir(repo_root)
        try:
            # Save current commit hash
            old_commit = (
                subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
            )
            # Do a git pull
            subprocess.check_call(["git", "pull"])
            # Compare new commit hash
            new_commit = (
                subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
            )
            # If new commit is different, restart the originally-called script
            if old_commit != new_commit:
                print(
                    f"Repository updated from {old_commit[:7]} to {new_commit[:7]}. Restarting script..."
                )
                python_executable = sys.executable
                original_script = os.path.abspath(
                    sys.argv[0]
                )  # <-- The script that was run
                os.execv(python_executable, [python_executable, original_script])

        except subprocess.CalledProcessError as e:
            print(f"Git operation failed: {e}")


## Done!


## Done!
## Last updated - 27 Jan 2026