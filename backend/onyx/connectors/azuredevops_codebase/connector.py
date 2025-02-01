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

def _get_language_from_extension(extension: str) -> str:
    extension_to_language = {
        "py": "Python",
        "js": "JavaScript",
        "ts": "TypeScript",
        "cs": "C#",
        "html": "HTML",
        "css": "CSS",
        "json": "JSON",
        "xml": "XML",
        "yaml": "YAML",
        "yml": "YAML",
        "sql": "SQL",
        "sh": "Shell",
        "bash": "Shell",
        "ps1": "PowerShell",
        "bat": "Batch",
        "cmd": "Batch",
        "tf": "Terraform",
        "tfvars": "Terraform",
        "md": "Markdown",
    }

    return extension_to_language.get(extension, "Unknown")

def _convert_code_to_document(
    repo_id: str, repo_url: str, repo_name: str, content_string: str, file_path: str
) -> Document:
    extension = file_path.split(".")[-1].lower()
    language = _get_language_from_extension(extension)
    file_url = f"{repo_url}?path={file_path}"
    doc = Document(
        id=f"{repo_id}:{repo_url}:{file_path}",
        sections=[Section(link=file_url, text=content_string)],
        source=DocumentSource.AZUREDEVOPSCODEBASE,
        semantic_identifier=f"{repo_name}/{file_path}",
        doc_updated_at=datetime.now().replace(tzinfo=timezone.utc),  # Use current time
        primary_owners=[],
        metadata={"type": "CodeFile", "language": language, "repo": repo_name},
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

    def _fetch_from_azuredevops(self, start: SecondsSinceUnixEpoch | None, end: SecondsSinceUnixEpoch | None) -> GenerateDocumentsOutput:
        if self.azdo_client is None:
            raise ConnectorMissingCredentialError("AzureDevops")
        
        # Get code
        git_client = self.azdo_client.clients.get_git_client()
        repo = git_client.get_repository(project=self.project_name, repository_id=self.repo_name)

        subprocess.run(["git", "config", "--global", "credential.helper", "store"], check=True)
        credential_data = f"""
            protocol=https
            host=dev.azure.com
            username=onyx@azure.com
            password={self.pat}
            """
        subprocess.run(["git", "credential", "approve"], input=credential_data.encode(), check=True)
        
        organization = self.base_url.split("/")[-1]
        repo_path = f"{destination}/{self.repo_name}"
        # replace space with %20 in repo name
        repo_name_safe = self.repo_name.replace(" ", "%20")
        repo_url = f"{self.base_url}/{self.project_name}/_git/{repo_name_safe}"
        clone_url = f"https://{self.pat}@dev.azure.com/{organization}/{self.project_name}/_git/{repo_name_safe}"
        first_clone = False
        os.makedirs(destination, exist_ok=True)

        # check if repo exists
        if not os.path.exists(repo_path):        
            subprocess.run(["git", "clone", "--branch", self.branch, clone_url, repo_path], check=True)
            first_clone = True
        else:
            subprocess.run(["git", "-C", repo_path, "pull"], check=True)

        file_list = self.get_repo_files_list(repo_path)
        
        logger.info(f"codat: Processing {len(file_list)} files from {self.repo_name} repository")

        if not first_clone and start is not None and end is not None:
            result = subprocess.run(["git", "-C", repo_path, "log", f"--since={datetime.fromtimestamp(start)}",
                                      f"--until={datetime.fromtimestamp(end)}", "--name-only", 
                                      "--pretty=format:"], check=True, text=True, capture_output=True)
            changed_files = set(result.stdout.splitlines())            
            changed_files.discard("")
            logger.info(f"codat: Processing {len(changed_files)} files from {self.repo_name} repository")

            file_list = [f for f in file_list if f in changed_files]
            logger.info(f"codat: Processing {len(file_list)} files from {self.repo_name} repository")

        yield from self.process_files(file_list, repo, repo_url, repo_path)


    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops(start=None, end=None)

    def poll_source(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> GenerateDocumentsOutput:
        return self._fetch_from_azuredevops(start=start, end=end)

    def get_repo_files_list(self, repo_path: str) -> list[str]: 
        file_list = []
        for root, _, files in os.walk(repo_path):
            for file in files:
                if os.path.splitext(file)[1] in self.extensions:
                    file_list.append(os.path.join(root, file))  
        return file_list

    def process_files(self, file_list, repo, repo_url, repo_path):
        for item_batch in _batch_azuredevops_objects(file_list, self.batch_size):
            code_doc_batch = []
            for item in item_batch:
                with open(item, "r", encoding="utf-8", errors="ignore") as f:
                    file_content = f.read()

                    code_doc_batch.append(
                        _convert_code_to_document(
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
