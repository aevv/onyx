import fnmatch
import itertools
from collections import deque
from collections.abc import Iterable
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import Any

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

from azure.devops.v7_1.work_item_tracking.models import WorkItem
from azure.devops.v7_1.work_item_tracking.models import Wiql
from azure.devops.v7_1.git.models import GitPullRequest
from azure.devops.v7_1.git.models import GitRepository
from azure.devops.v7_1.git.models import GitItem
from azure.devops.v7_1.git.models import GitPullRequestSearchCriteria

import pytz

from onyx.configs.app_configs import AZUREDEVOPS_CONNECTOR_INCLUDE_CODE_FILES
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

def _convert_repo_to_document(repo: GitRepository) -> Document:
    doc = Document(
        id=repo.id,
        sections=[Section(link=repo.url, text=repo.name)],
        source=DocumentSource.AZUREDEVOPS,
        semantic_identifier=repo.name,
        doc_updated_at=repo.creation_date.replace(tzinfo=timezone.utc),
        primary_owners=[],
        metadata={"type": "Repository"},
    )
    return doc

def _convert_pull_request_to_document(pr: GitPullRequest) -> Document:
    doc = Document(
        id=pr.url,
        sections=[Section(link=pr.url, text=pr.description or "")],
        source=DocumentSource.AZUREDEVOPS,
        semantic_identifier=pr.title,
        doc_updated_at=pr.creation_date.replace(tzinfo=timezone.utc),
        primary_owners=[get_author(pr.created_by.display_name)],
        metadata={"state": pr.status, "type": "PullRequest"},
    )
    return doc

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
    # convert string to datetime
    date = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    doc = Document(
        id=work_item_url,
        sections=[Section(link=work_item_url, text=work_item.fields.get("System.Description") or "")],
        source=DocumentSource.AZUREDEVOPS,
        semantic_identifier=work_item.fields.get("System.Title", "Unnamed"),
        doc_updated_at=date.replace(tzinfo=timezone.utc),
        primary_owners=[get_author(work_item.fields.get("System.CreatedBy")["displayName"])],
        metadata={"state": work_item.fields.get("System.State"), "type": work_item.fields.get("System.WorkItemType")},
    )
    return doc


def _convert_code_to_document(
    repo_id: str, file: GitItem, repo_url: str, project_name: str
) -> Document:
    
    file_url = f"{repo_url}/_git/{project_name}?path={file.path}&version=GBmaster"  # Construct the file URL

    try:
        file_content = file.content  # File content as a string
    except UnicodeDecodeError:
        file_content = file.content.decode("latin-1")  # Handle encoding issues

    doc = Document(
        id=f"{repo_id}:{file.path}",
        sections=[Section(link=file_url, text=file_content)],
        source=DocumentSource.AZURE_DEVOPS,
        semantic_identifier=file.path.split("/")[-1],  # Extract filename
        doc_updated_at=datetime.now().replace(tzinfo=timezone.utc),  # Use current time
        primary_owners=[],
        metadata={"type": "CodeFile"},
    )
    return doc


class AzureDevopsConnector(LoadConnector, PollConnector):
    def __init__(
        self,
        project_name: str,
        repo_name: str,
        batch_size: int = INDEX_BATCH_SIZE,
        state_filter: str = "all",
        include_prs: bool = True,
        include_workitems: bool = True,
        include_code_files: bool = AZUREDEVOPS_CONNECTOR_INCLUDE_CODE_FILES,
    ) -> None:
        self.project_name = project_name
        self.repo_name = repo_name
        self.batch_size = batch_size
        self.state_filter = state_filter
        self.include_prs = include_prs
        self.include_workitems = include_workitems
        self.include_code_files = include_code_files
        self.azdo_client: Connection | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        azdo_credentials = BasicAuthentication("", credentials["azuredevops_access_token"])
        self.base_url = credentials["azuredevops_url"]
        self.azdo_client = Connection(base_url=credentials["azuredevops_url"], creds=azdo_credentials)        
        return None

    def _fetch_from_azuredevops(self) -> GenerateDocumentsOutput:
        if self.azdo_client is None:
            raise ConnectorMissingCredentialError("AzureDevops")
        
        
        if True:
            # Get code
            git_client = self.azdo_client.clients.get_git_client()
            repo = git_client.get_repository(project=self.project_name, repository_id=self.repo_name)
            yield _convert_repo_to_document(repo)
            
            items = git_client.get_items(repository_id=repo.id, scope_path="/", recursion_level="full")
            for item_batch in _batch_azuredevops_objects(items, self.batch_size):
                code_doc_batch: list[Document] = []
                for item in item_batch:                    
                    code_file = git_client.get_item(repository_id=repo.id, path=item.path, include_content=True)
                    code_doc_batch.append(
                        _convert_code_to_document(
                            repo.id,
                            code_file,
                            self.azdo_client.url,
                            self.project_name,
                        )
                    )
                if code_doc_batch:
                    yield code_doc_batch
        
        if self.include_prs:
            # Get PRs
            git_client = self.azdo_client.clients.get_git_client()
            repo = git_client.get_repository(project=self.project_name, repository_id=self.repo_name)
            search_criteria = GitPullRequestSearchCriteria(repository_id=repo.id, status="all")
            prs = git_client.get_pull_requests_by_project(project=self.project_name, search_criteria=search_criteria)
            for pr_batch in _batch_azuredevops_objects(prs, self.batch_size):
                pr_doc_batch: list[Document] = []
                for pr in pr_batch:
                    pr_doc_batch.append(_convert_pull_request_to_document(pr))
                yield pr_doc_batch

        if self.include_workitems:
            # Get workitems
            work_item_client = self.azdo_client.clients.get_work_item_tracking_client()
            query = f"SELECT [System.Id], [System.Title] FROM WorkItems WHERE [System.TeamProject] = '{self.project_name}' AND [System.ChangedDate] > @today - 180 ORDER BY [System.CreatedDate] Desc"
            work_items = work_item_client.query_by_wiql(Wiql(query=query))
            work_item_ids = [item.id for item in work_items.work_items]
            
            batch_size = 200
            work_items = []

            for i in range(0, len(work_item_ids), batch_size):
                batch_ids = work_item_ids[i : i + batch_size]  # Get batch of IDs
                work_items_batch = work_item_client.get_work_items(batch_ids, expand="All")  # Fetch full details
                work_items.extend(work_items_batch)

            for workitem_batch in _batch_azuredevops_objects(work_items, self.batch_size):
                workitem_doc_batch: list[Document] = []
                for work_item in workitem_batch:
                    workitem_doc_batch.append(_convert_workitem_to_document(work_item, self.base_url))
                yield workitem_doc_batch   


    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops()

    def poll_source(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops()


if __name__ == "__main__":
    import os

    connector = AzureDevopsConnector(        
        project_name=os.environ["PROJECT_NAME"],
        batch_size=10,
        state_filter="all",
        include_prs=True,
        include_workitems=True,
        include_code_files=AZUREDEVOPS_CONNECTOR_INCLUDE_CODE_FILES,
    )

    connector.load_credentials(
        {
            "azuredevops_access_token": os.environ["AZUREDEVOPS_ACCESS_TOKEN"],
            "azuredevops_url": os.environ["AZUREDEVOPS_URL"],
        }
    )
    document_batches = connector.load_from_state()
    print(next(document_batches))
