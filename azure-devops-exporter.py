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
import pandas

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
repositories_dicts = [
    {
        'id': r.id,
        'name': r.name
    }
    for r in repositories
]
repositories_df = pandas.DataFrame.from_dict(repositories_dicts)

search_criteria = GitPullRequestSearchCriteria()
search_criteria.creator_id = user_id
search_criteria.status = 'Completed'
search_criteria.target_ref_name = 'refs/heads/develop'
pull_requests: list[GitPullRequest] = git_client.get_pull_requests_by_project('KDS-PCM', search_criteria, top=0)
filtered_pull_requests: list[GitPullRequest] = [ pr for pr in pull_requests if pr.closed_date >= datetime(2024,1,1, tzinfo=ZoneInfo('America/Edmonton'))] # TODO filter

print(f'Fetched {len(pull_requests)} PR(s) and {len(filtered_pull_requests)} matched the criteria')

pull_request_dicts = []
pull_request_work_item_dicts = []
for pr in filtered_pull_requests:
    detailed_pull_request: GitPullRequest = git_client.get_pull_request(pr.repository.id, pr.pull_request_id, 'KDS-PCM', include_work_item_refs=True)
    detailed_pull_request_json = {
        'id': detailed_pull_request.pull_request_id,
        'title': detailed_pull_request.title,
        'description': detailed_pull_request.description,
        'repository_id': detailed_pull_request.repository.id,
        'completed_date': detailed_pull_request.closed_date.strftime('%Y-%m-%d'),
        'created_by': detailed_pull_request.created_by.display_name
    }
    pull_request_work_item_dicts.extend([{
         "pull_request_id": detailed_pull_request.pull_request_id,
         "work_item_id": wi_ref.id
         } for wi_ref in detailed_pull_request.work_item_refs])
    pull_request_dicts.append(detailed_pull_request_json)
    time.sleep(0.25)

pr_df = pandas.DataFrame.from_dict(pull_request_dicts)
pr_wi_df = pandas.DataFrame.from_dict(pull_request_work_item_dicts)

# fields = work_item_tracking_client.get_fields('KDS-PCM')
# print(json.dumps([ { 'name': field.name, 'referenceName': field.reference_name } for field in fields]))

work_items = []
work_item_ids_batch_size = 200
work_item_ids_list = list(set([pr_wi['work_item_id'] for pr_wi in pull_request_work_item_dicts]))
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
work_item_dicts = []
work_item_comment_dicts = []
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
    for comment in commentsList.comments:
        comment_json = {
            'work_item_id': wi.id,
            'author': comment.created_by.display_name,
            'created_date': comment.created_date.isoformat()
        }

        if comment.format == 'html':
            comment_json['text'] = md(comment.text)
        else:
            comment_json['text'] = comment.text

        work_item_comment_dicts.append(comment_json)

    work_item_dicts.append(wi_json)
    time.sleep(0.25)

work_item_df = pandas.DataFrame.from_dict(work_item_dicts)
work_item_comment_df = pandas.DataFrame.from_dict(work_item_comment_dicts)

repositories_df.to_csv('repository.csv', index=False)
pr_df.to_csv('pull_request.csv', index=False)
pr_wi_df.to_csv('pull_request_work_item.csv', index=False)
work_item_df.to_csv('work_item.csv', index=False)
work_item_comment_df.to_csv('work_item_comment.csv', index=False)