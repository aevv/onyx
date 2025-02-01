import fnmatch
import itertools
from collections import deque
from collections.abc import Iterable
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import Any
import time
import subprocess 
import os

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

from azure.devops.v7_1.work_item_tracking.models import WorkItem
from azure.devops.v7_1.work_item_tracking.models import Wiql

import pytz

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.interfaces import PollConnector
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.models import BasicExpertInfo
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import Section
from onyx.utils.logger import setup_logger


logger = setup_logger()


def _batch_azuredevops_objects(
    git_objs: Iterable[Any], batch_size: int
) -> Iterator[list[Any]]:
    it = iter(git_objs)
    while True:
        batch = list(itertools.islice(it, batch_size))
        if not batch:
            break
        yield batch


def get_author(author: Any) -> BasicExpertInfo:
    return BasicExpertInfo(
        display_name=author,
    )

def format_date(date: str) -> datetime:
    formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]  # Handles both cases
    for fmt in formats:
        try:
            return datetime.strptime(date, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data {date} does not match known formats")

def _convert_workitem_to_document(work_item: WorkItem, base_url) -> Document:
    work_item_url = f"{base_url}/_workItems/edit/{work_item.id}"
    changed_date = format_date(work_item.fields.get("System.ChangedDate"))
    currently_assigned = None
    assigned_to = work_item.fields.get("System.AssignedTo")
    if assigned_to is not None:
        currently_assigned = get_author(assigned_to["displayName"])

    doc = Document(
        id=work_item_url,
        sections=[
            Section(link=work_item_url, text=work_item.fields.get("System.Description") or ""),
            Section(link=work_item_url, text=work_item.fields.get("Microsoft.VSTS.TCM.SystemInfo") or ""),
            Section(link=work_item_url, text=work_item.fields.get("Microsoft.VSTS.Common.AcceptanceCriteria") or ""),
            ],
        source=DocumentSource.AZUREDEVOPSMANAGEMENT,
        semantic_identifier=work_item.id,
        doc_updated_at=changed_date.replace(tzinfo=timezone.utc),
        primary_owners=[get_author(work_item.fields.get("System.CreatedBy")["displayName"]), currently_assigned],
        metadata={
            "state": work_item.fields.get("System.State"), 
            "type": work_item.fields.get("System.WorkItemType"),
            "iteration": work_item.fields.get("System.IterationPath"),
            "area": work_item.fields.get("System.AreaPath"),
            "priority": work_item.fields.get("Microsoft.VSTS.Common.Priority"),
            "tags": work_item.fields.get("System.Tags"),
            "assigned_to": currently_assigned
            },
    )
    return doc

class AzureDevopsManagementConnector(LoadConnector, PollConnector):
    def __init__(
        self,
        project_name: str,
        number_days: str,
        state_filter: str,
        batch_size: int = INDEX_BATCH_SIZE
    ) -> None:
        self.batch_size = batch_size
        self.state_filter = state_filter
        self.project_name = project_name
        self.number_days = number_days
        self.azdo_client: Connection | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        azdo_credentials = BasicAuthentication("", credentials["azuredevops_access_token"])
        self.base_url = credentials["azuredevops_url"]
        self.azdo_client = Connection(base_url=credentials["azuredevops_url"], creds=azdo_credentials)
        self.pat = credentials["azuredevops_access_token"]       
        return None

    def _fetch_from_azuredevops(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> GenerateDocumentsOutput:
        if self.azdo_client is None:
            raise ConnectorMissingCredentialError("AzureDevops")
        
        if start is None and end is None:
            query_length = int(self.number_days) if self.number_days is not None else 0
        else:
            query_length = (end - start) * (24 * 60 * 60)

        if self.state_filter == "all" or self.state_filter is None:
            query_state = "<> ''"
        else:
            query_state = f"= '{self.state_filter}'"

        # Get workitems
        work_item_client = self.azdo_client.clients.get_work_item_tracking_client()
        
        query = f"""SELECT [System.Id]
          FROM WorkItems 
          WHERE [System.TeamProject] = '{self.project_name}' 
           AND [System.ChangedDate] > @today - {query_length}
           AND [System.State] {query_state} 
          ORDER BY [System.CreatedDate] Desc"""
        
        work_items = work_item_client.query_by_wiql(Wiql(query=query))
        work_item_ids = [item.id for item in work_items.work_items]

        for i in range(0, len(work_item_ids), self.batch_size):
            batch_ids = work_item_ids[i : i + self.batch_size]  # Get batch of IDs
            work_items_batch = work_item_client.get_work_items(batch_ids, expand="All")  # Fetch full details

            for workitem_batch in _batch_azuredevops_objects(work_items_batch, self.batch_size):
                workitem_doc_batch: list[Document] = []
                for work_item in workitem_batch:
                    workitem_doc_batch.append(_convert_workitem_to_document(work_item, self.base_url))
                yield workitem_doc_batch            


    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops(start=None, end=None)

    # TODO: Ongoing indexing
    def poll_source(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops(start=start, end=end)


if __name__ == "__main__":
    import os

    connector = AzureDevopsManagementConnector(
        batch_size=10,
        state_filter=os.environ["STATE_FILTER"].lower(),
        project_name=os.environ["PROJECT_NAME"],
        number_days=os.environ["NUMBER_DAYS"]
    )

    connector.load_credentials(
        {
            "azuredevops_access_token": os.environ["AZUREDEVOPS_ACCESS_TOKEN"],
            "azuredevops_url": os.environ["AZUREDEVOPS_URL"],
        }
    )
    document_batches = connector.load_from_state()
    print(next(document_batches))
