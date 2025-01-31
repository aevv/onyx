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

def format_date(date: str) -> datetime:
    formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]  # Handles both cases
    for fmt in formats:
        try:
            return datetime.strptime(date, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data {date} does not match known formats")

def _convert_code_to_document(
    repo_id: str, content_string: str, repo_url: str, project_name: str
) -> Document:
    
    doc = Document(
        id=f"{repo_id}:{repo_url}",
        sections=[Section(link=repo_url, text=content_string)],
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
        self.include_code_files = include_code_files
        self.azdo_client: Connection | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        azdo_credentials = BasicAuthentication("", credentials["azuredevops_access_token"])
        self.base_url = credentials["azuredevops_url"]
        self.azdo_client = Connection(base_url=credentials["azuredevops_url"], creds=azdo_credentials)
        self.pat = credentials["azuredevops_access_token"]       
        return None

    def _fetch_from_azuredevops(self) -> GenerateDocumentsOutput:
        if self.azdo_client is None:
            raise ConnectorMissingCredentialError("AzureDevops")
        
        if self.repo_name is not None:
            # Get code
            git_client = self.azdo_client.clients.get_git_client()
            repo = git_client.get_repository(project=self.project_name, repository_id=self.repo_name)

            destination = "/mnt/datadisk/source"
            os.makedirs(destination, exist_ok=True)

            subprocess.run(["git", "config", "--global", "credential.helper", "store"], check=True)

            credential_data = f"""
                protocol=https
                host=dev.azure.com
                username=platform@codat.io
                password={self.pat}
                """

            subprocess.run(["git", "credential", "approve"], input=credential_data.encode(), check=True)
            subprocess.run(["git", "clone", f"https://{self.pat}@dev.azure.com/codat/Codat/_git/{self.repo_name}", destination], check=True)

            file_list = []
            allowed_extensions = {".cs"} 
            allowed_filenames = {"README", "README.md", "README.txt"} 

            file_list = []
            repo_path = f"{destination}/{self.repo_name}"
            for root, _, files in os.walk(repo_path):
                for file in files:
                    if file in allowed_filenames or os.path.splitext(file)[1] in allowed_extensions:
                        file_list.append(os.path.join(root, file))  

            for item_batch in _batch_azuredevops_objects(file_list, self.batch_size):
                code_doc_batch: list[Document] = []
                for item in item_batch:   
                    with open(item, "r", encoding="utf-8", errors="ignore") as f:
                        file_content = f.read()

                        code_doc_batch.append(
                            _convert_code_to_document(
                                repo.id,
                                file_content,
                                item.removeprefix(repo_path),
                                self.project_name,
                            )
                        )
                if code_doc_batch:
                    yield code_doc_batch


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
