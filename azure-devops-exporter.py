import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from azure.devops.connection import Connection
from azure.devops.v7_1.core import CoreClient
from azure.devops.v7_1.git import GitClient, GitRepository, GitPullRequest, GitPullRequestSearchCriteria
from azure.devops.v7_1.work_item_tracking import WorkItemTrackingClient, WorkItemBatchGetRequest
from msrest.authentication import BasicAuthentication
from markdownify import markdownify as md

# Fill in with your personal access token and org URL
personal_access_token = 'xxx'
organization_url = 'https://dev.azure.com/dp-us-bus'
user_id = 'afdcec49-9996-6e2a-8306-7b34ee53005b'

# Create a connection to the org
credentials = BasicAuthentication('', personal_access_token)
connection = Connection(base_url=organization_url, creds=credentials)

# Get a client (the "core" client provides access to projects, teams, etc)
core_client: CoreClient = connection.clients_v7_1.get_core_client()
git_client: GitClient = connection.clients_v7_1.get_git_client()
work_item_tracking_client: WorkItemTrackingClient = connection.clients_v7_1.get_work_item_tracking_client()


repositories: list[GitRepository] = git_client.get_repositories('KDS-PCM')
repositories_json = [
    {
        'id': r.id,
        'name': r.name
    }
    for r in repositories
]

search_criteria = GitPullRequestSearchCriteria()
search_criteria.creator_id = user_id
search_criteria.status = 'Completed'
search_criteria.target_ref_name = 'refs/heads/develop'
pull_requests: list[GitPullRequest] = git_client.get_pull_requests_by_project('KDS-PCM', search_criteria, top=0)
filtered_pull_requests: list[GitPullRequest] = [ pr for pr in pull_requests if pr.closed_date >= datetime(2024,1,1, tzinfo=ZoneInfo('America/Edmonton'))] # TODO filter

print(f'Fetched {len(pull_requests)} PR(s) and {len(filtered_pull_requests)} matched the criteria')

work_item_ids = set()
pull_requests_json = []
for pr in filtered_pull_requests:
    detailed_pull_request: GitPullRequest = git_client.get_pull_request(pr.repository.id, pr.pull_request_id, 'KDS-PCM', include_work_item_refs=True)
    detailed_pull_request_json = {
        'id': detailed_pull_request.pull_request_id,
        'title': detailed_pull_request.title,
        'description': detailed_pull_request.description,
        'repository_id': detailed_pull_request.repository.id,
        'completed_date': detailed_pull_request.closed_date.strftime('%Y-%m-%d'),
        'work_item_ids': [ int(ref.id) for ref in (detailed_pull_request.work_item_refs or []) ],
        #'raw': detailed_pull_request.serialize()
    }
    pull_requests_json.append(detailed_pull_request_json)
    work_item_ids.update(detailed_pull_request_json['work_item_ids'])
    time.sleep(0.25)

# fields = work_item_tracking_client.get_fields('KDS-PCM')
# print(json.dumps([ { 'name': field.name, 'referenceName': field.reference_name } for field in fields]))

work_items = []
work_item_ids_batch_size = 200
work_item_ids_list = list(work_item_ids)
work_item_ids_batches = [work_item_ids_list[i:i + work_item_ids_batch_size] for i in range(0, len(work_item_ids_list), work_item_ids_batch_size)]
for work_item_ids_batch in work_item_ids_batches:
    get_work_items_request = WorkItemBatchGetRequest()
    get_work_items_request.ids = work_item_ids_batch
    get_work_items_request.fields = [
        'System.Title',
        'System.WorkItemType',
        'System.Description',
        'System.CreatedBy',
        'System.CreatedDate',
        'Microsoft.VSTS.Common.AcceptanceCriteria',
        'Microsoft.VSTS.TCM.ReproSteps'
    ]
    print(f'Fetching a batch of {len(work_item_ids_batch)} work items')
    work_items_batch = work_item_tracking_client.get_work_items_batch(get_work_items_request)
    work_items.extend(work_items_batch)
    print(f'Fetched a batch of {len(work_item_ids_batch)} work items')
work_items_json = []
for wi in work_items:
    wi_json = {
        'id': wi.id,
        'title': wi.fields['System.Title'],
        'type': wi.fields['System.WorkItemType']
    }

    if wi_json['type'] == 'User Story':
        if description := wi.fields.get('System.Description'):
            wi_json['description'] = md(description)
    elif wi_json['type'] == 'Bug':
        if repro_steps := wi.fields.get('Microsoft.VSTS.TCM.ReproSteps'):
            wi_json['description'] = md(repro_steps)

    commentsList = work_item_tracking_client.get_comments('KDS-PCM', wi.id, top=50)
    comments_json = []
    for comment in commentsList.comments:
        comment_json = {
            'author': comment.created_by.display_name,
            'created_date': comment.created_date.isoformat()
        }

        if comment.format == 'html':
            comment_json['text'] = md(comment.text)
        else:
            comment_json['text'] = comment.text

        comments_json.append(comment_json)
    wi_json['comments'] = comments_json

    work_items_json.append(wi_json)
    time.sleep(0.25)

full_json = {
    'repositories': repositories_json,
    'pull_requests': pull_requests_json,
    'work_items': work_items_json
}

with open('output.json', 'w') as f:
    json.dump(full_json, f)