from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

# Clients
from azure.devops.v7_1.work_item_tracking import WorkItemTrackingClient as devopsWorkItemClient
from azure.devops.v7_1.git import GitClient as devopsGitClient

# Work items
from azure.devops.v7_1.work_item_tracking.models import Wiql
from azure.devops.v7_1.work_item_tracking.models import WorkItem

# Git items
from azure.devops.v7_1.git.models import GitRepository
from azure.devops.v7_1.git.models import GitPullRequest
from azure.devops.v7_1.git.models import GitItem
from azure.devops.v7_1.git.models import GitPullRequestSearchCriteria


#AttributeLinks 


import time

# config
PAT = ""
base_url = "https://dev.azure.com/codat"
project = "codat"


def main():
    azdo_credentials = BasicAuthentication("", PAT)
    azdo_client = Connection(base_url=base_url, creds=azdo_credentials)

    # workitems(azdo_client)
    # repos(azdo_client)
    # pull_requests(azdo_client)
    # code(azdo_client)
    batch_code(azdo_client)


def batch_code(azdo_client):
    # Get code
    git_client = azdo_client.clients.get_git_client()
    repo = git_client.get_repository(project=project, repository_id="Infrastructure-As-Code")
        
    items = git_client.get_items(repository_id=repo.id, scope_path="/", recursion_level="full")
    for item in items: 
        time.sleep(1)  
        try:
            code_file = git_client.get_item(repository_id=repo.id, path=item.path, include_content=True)
            print(code_file.path)               
        except:       
            print("Error")     
            continue

def code(azdo_client):
    # Get code
    git_client: devopsGitClient = azdo_client.clients.get_git_client()
    git_repo: GitRepository = git_client.get_repository(repository_id="ad8599b2-773d-4218-b0dc-eca568a7242c")
    items = git_client.get_items(repository_id=git_repo.id, scope_path="/", recursion_level="Full")
    # for item in items:
        # print(f"Item: {item.path} - {item.is_folder}")

    # Get item content
    item: GitItem = git_client.get_item(repository_id=git_repo.id, path="README.md", include_content=False)
    print(item)

    # Get item content
    item: GitItem = git_client.get_item(repository_id=git_repo.id, path="README.md", download=True)
    # print(item.content)


def pull_requests(azdo_client):
    # Get pull requests
    git_client: devopsGitClient = azdo_client.clients.get_git_client()
    search_criteria = GitPullRequestSearchCriteria(repository_id="ad8599b2-773d-4218-b0dc-eca568a7242c", status="all")
    pull_requests = git_client.get_pull_requests_by_project(project=project, search_criteria=search_criteria)
    for pr in pull_requests:
        print(f"PR: {pr.pull_request_id} - {pr.title} - {pr.created_by.display_name}")

def repos(azdo_client):
    # Get repos
    git_client: devopsGitClient = azdo_client.clients.get_git_client()
    repos = git_client.get_repositories(project=project)
    for repo in repos:
        # print(f"Repo: {repo.id} - {repo.name} - {repo.created_by}")
        print(repo)

def workitems(azdo_client):
    # Get work items
    work_item_client: devopsWorkItemClient = azdo_client.clients.get_work_item_tracking_client()
    query = "SELECT [System.Id], [System.Title] FROM WorkItems WHERE [System.TeamProject] = 'Codat' AND [System.ChangedDate] > @today - 10 ORDER BY [System.CreatedDate] Desc"
    work_items = work_item_client.query_by_wiql(Wiql(query=query))
    work_item_ids = [item.id for item in work_items.work_items]
    
    batch_size = 200
    work_items = []

    for i in range(0, len(work_item_ids), batch_size):
        batch_ids = work_item_ids[i : i + batch_size]  # Get batch of IDs
        work_items_batch = work_item_client.get_work_items(batch_ids, expand="All")  # Fetch full details
        work_items.extend(work_items_batch)

    # for work_item in work_items:
        # print(f"Work item: {work_item.id} - {work_item.fields['System.Title']}")  

    # print desc of first
    # print(work_items[1].fields['System.Description'])
    print(work_items[1].fields['System.CreatedBy']["displayName"])
    item = work_items[1]
    # print(f"{item.id})


if __name__ == "__main__":
    main()



def x(azo_client):
    if True:
        # Get code
        git_client = azdo_client.clients.get_git_client()
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

    if True:
        # Get PRs
        git_client = azdo_client.clients.get_git_client()
        repo = git_client.get_repository(project=self.project_name, repository_id=self.repo_name)
        prs = git_client.get_pull_requests(project=self.project_name, repository_id=repo.id)
        for pr_batch in _batch_azuredevops_objects(prs, self.batch_size):
            pr_doc_batch: list[Document] = []
            for pr in pr_batch:
                pr_doc_batch.append(_convert_pull_request_to_document(pr))
            yield pr_doc_batch

    if True:
        # Get workitems
        work_item_client: devopsWorkItemClient = azdo_client.clients.get_work_item_tracking_client()
        query = f"SELECT [System.Id], [System.Title] FROM WorkItems WHERE [System.TeamProject] = '{self.project_name}' AND [System.ChangedDate] > @today - 180 ORDER BY [System.CreatedDate] Desc"
        work_items = work_item_client.query_by_wiql(Wiql(query=query))
        work_item_ids = [item.id for item in work_items.work_items]
        
        batch_size = 200
        work_items = []

        for i in range(0, len(work_item_ids), batch_size):
            batch_ids = work_item_ids[i : i + batch_size]  # Get batch of IDs
            work_items_batch = work_item_client.get_work_items(batch_ids, expand="All")  # Fetch full details
            work_items.extend(work_items_batch)

        for work_item in work_items:
            yield _convert_workitem_to_document(work_item)   