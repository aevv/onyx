import fnmatch
import itertools
from collections import deque
from collections.abc import Iterable
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import List
import time
import subprocess 
import os

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

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
destination = "/git/azuredevops"


def _batch_azuredevops_objects(
    git_objs: Iterable[Any], batch_size: int
) -> Iterator[list[Any]]:
    it = iter(git_objs)
    while True:
        batch = list(itertools.islice(it, batch_size))
        if not batch:
            break
        yield batch

def _convert_code_to_document(
    repo_id: str, repo_url: str, repo_name: str, content_string: str, file_path: str
) -> Document:
    # https://dev.azure.com/{org}/{project}/_git/{repo}?path=/{path}
    file_url = f"{repo_url}?path={file_path}"
    doc = Document(
        id=f"{repo_id}:{repo_url}:{file_path}",
        sections=[Section(link=file_url, text=content_string)],
        source=DocumentSource.AZUREDEVOPSCODEBASE,
        semantic_identifier=f"{repo_name}/{file_path}",
        doc_updated_at=datetime.now().replace(tzinfo=timezone.utc),  # Use current time
        primary_owners=[],
        metadata={"type": "CodeFile"},
    )
    return doc


class AzureDevopsCodebaseConnector(LoadConnector, PollConnector):
    def __init__(
        self,
        repo_name: str,
        project_name: str,
        branch: str,
        extensions: List[str],
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.repo_name = repo_name
        self.project_name = project_name
        self.batch_size = batch_size
        self.branch = branch
        self.extensions = extensions     
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
        
        # Get code
        git_client = self.azdo_client.clients.get_git_client()
        repo = git_client.get_repository(project=self.project_name, repository_id=self.repo_name)

        os.makedirs(destination, exist_ok=True)

        # TODO can we remove credentials?
        subprocess.run(["git", "config", "--global", "credential.helper", "store"], check=True)

        credential_data = f"""
            protocol=https
            host=dev.azure.com
            username=onyx@azure.com
            password={self.pat}
            """
        organization = self.base_url.split("/")[-1]
        repo_path = f"{destination}/{self.repo_name}"
        repo_url = f"{self.base_url}/{self.project_name}/_git/{self.repo_name}"
        clone_url = f"https://{self.pat}@dev.azure.com/{organization}/{self.project_name}/_git/{self.repo_name}"
        subprocess.run(["git", "credential", "approve"], input=credential_data.encode(), check=True)
        subprocess.run(["git", "clone", "--branch", self.branch, clone_url, repo_path], check=True)

        file_list = self.get_repo_files_list(repo_path)
        yield from self.process_files(file_list, repo, repo_url, repo_path)


    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops()

    def poll_source(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops()

    def get_repo_files_list(self, repo_path: str) -> list[str]: 
        allowed_extensions = {".cs"} 
        allowed_filenames = {"README", "README.md", "README.txt"} 
        
        file_list = []
        for root, _, files in os.walk(repo_path):
            for file in files:
                if file in allowed_filenames or os.path.splitext(file)[1] in allowed_extensions:
                    file_list.append(os.path.join(root, file))  
        return file_list

    def process_files(self, file_list, repo, repo_url, repo_path):
        for item_batch in _batch_azuredevops_objects(file_list):
            code_doc_batch = []
            for item in item_batch:
                with open(item, "r", encoding="utf-8", errors="ignore") as f:
                    file_content = f.read()

                    code_doc_batch.append(
                        self._convert_code_to_document(
                            repo.id,
                            repo_url,
                            self.repo_name,
                            file_content,
                            item.removeprefix(repo_path),
                        )
                    )
            if code_doc_batch:
                yield code_doc_batch   


if __name__ == "__main__":
    import os

    connector = AzureDevopsCodebaseConnector(        
        repo_name=os.environ["REPO_NAME"],
        project_name=os.environ["PROJECT_NAME"],
        branch=os.environ["BRANCH"],
        extensions=os.environ["EXTENSIONS"],
        batch_size=10,
    )

    connector.load_credentials(
        {
            "azuredevops_access_token": os.environ["AZUREDEVOPS_ACCESS_TOKEN"],
            "azuredevops_url": os.environ["AZUREDEVOPS_URL"],
        }
    )
    document_batches = connector.load_from_state()
    print(next(document_batches))
