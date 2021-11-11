#!/usr/bin/env python3

import datetime
import sys
import os
import re
import requests
import singer
from singer import Transformer, utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import pytz
import backoff
from strict_rfc3339 import rfc3339_to_timestamp
from dateutil.parser import isoparse

PER_PAGE_MAX = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v4",
    'private_token': None,
    'start_date': None,
    'groups': '',
    'ultimate_license': False,
    'fetch_bridges': False,
    'fetch_merge_request_commits': False,
    'fetch_pipelines_extended': False
}
STATE = {}
CATALOG = None

def parse_datetime(datetime_str):
    dt = isoparse(datetime_str)
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

RESOURCES = {
    'projects': {
        'url': '/projects/{id}?statistics=1',
        'schema': load_schema('projects'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_activity_at'],
    },
    'branches': {
        'url': '/projects/{id}/repository/branches',
        'schema': load_schema('branches'),
        'key_properties': ['project_id', 'name'],
        'replication_method': 'FULL_TABLE',
    },
    'commits': {
        'url': '/projects/{id}/repository/commits?since={start_date}&with_stats=true',
        'schema': load_schema('commits'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created_at'],
    },
    'issues': {
        'url': '/projects/{id}/issues?scope=all&updated_after={start_date}',
        'schema': load_schema('issues'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'jobs': {
        'url': '/projects/{id}/pipelines/{secondary_id}/jobs?include_retried=true',
        'schema': load_schema('jobs'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'merge_requests': {
        'url': '/projects/{id}/merge_requests?scope=all&updated_after={start_date}',
        'schema': load_schema('merge_requests'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'merge_request_commits': {
        'url': '/projects/{id}/merge_requests/{secondary_id}/commits',
        'schema': load_schema('merge_request_commits'),
        'key_properties': ['project_id', 'merge_request_iid', 'commit_id'],
        'replication_method': 'FULL_TABLE',
    },
    'project_milestones': {
        'url': '/projects/{id}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_milestones': {
        'url': '/groups/{id}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'users': {
        'url': '/projects/{id}/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'site_users': {
        'url': '/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'groups': {
        'url': '/groups/{id}',
        'schema': load_schema('groups'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'project_members': {
        'url': '/projects/{id}/members',
        'schema': load_schema('project_members'),
        'key_properties': ['project_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_members': {
        'url': '/groups/{id}/members',
        'schema': load_schema('group_members'),
        'key_properties': ['group_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'releases': {
        'url': '/projects/{id}/releases',
        'schema': load_schema('releases'),
        'key_properties': ['project_id', 'commit_id', 'tag_name'],
        'replication_method': 'FULL_TABLE',
    },
    'tags': {
        'url': '/projects/{id}/repository/tags',
        'schema': load_schema('tags'),
        'key_properties': ['project_id', 'commit_id', 'name'],
        'replication_method': 'FULL_TABLE',
    },
    'project_labels': {
        'url': '/projects/{id}/labels',
        'schema': load_schema('project_labels'),
        'key_properties': ['project_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_labels': {
        'url': '/groups/{id}/labels',
        'schema': load_schema('group_labels'),
        'key_properties': ['group_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'epics': {
        'url': '/groups/{id}/epics?updated_after={start_date}',
        'schema': load_schema('epics'),
        'key_properties': ['group_id', 'id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'epic_issues': {
        'url': '/groups/{id}/epics/{secondary_id}/issues',
        'schema': load_schema('epic_issues'),
        'key_properties': ['group_id', 'epic_iid', 'epic_issue_id'],
        'replication_method': 'FULL_TABLE',
    },
    'pipelines': {
        'url': '/projects/{id}/pipelines?updated_after={start_date}',
        'schema': load_schema('pipelines'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'pipelines_extended': {
        'url': '/projects/{id}/pipelines/{secondary_id}',
        'schema': load_schema('pipelines_extended'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'bridges': {
        'url': '/projects/{id}/pipelines/{secondary_id}/bridges',
        'schema': load_schema('bridges'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
}

ULTIMATE_RESOURCES = ("epics", "epic_issues")
STREAM_CONFIG_SWITCHES = ('merge_request_commits', 'pipelines_extended')

LOGGER = singer.get_logger()
SESSION = requests.Session()

TRUTHY = ("true", "1", "yes", "on")

class ResourceInaccessible(Exception):
    """
    Base exception for Rousources the current user can not access.
    e.g. Unauthorized, Forbidden, Not Found errors
    """

def truthy(val) -> bool:
    return str(val).lower() in TRUTHY

def get_url(entity, id, secondary_id=None, start_date=None):
    if not isinstance(id, int):
        id = id.replace("/", "%2F")

    if secondary_id and not isinstance(secondary_id, int):
        secondary_id = secondary_id.replace("/", "%2F")

    return CONFIG['api_url'] + RESOURCES[entity]['url'].format(
            id=id,
            secondary_id=secondary_id,
            start_date=start_date
        )


def get_start(entity):
    if entity not in STATE or parse_datetime(STATE[entity]) < parse_datetime(CONFIG['start_date']):
        STATE[entity] = CONFIG['start_date']
    return STATE[entity]


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(url, params=None):
    params = params or {}

    headers = { "Private-Token": CONFIG['private_token'] }
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    resp = SESSION.request('GET', url, params=params, headers=headers)
    LOGGER.info("GET {}".format(url))

    if resp.status_code in [401, 403]:
        LOGGER.info("Skipping request to {}".format(url))
        LOGGER.info("Reason: {} - {}".format(resp.status_code, resp.content))
        raise ResourceInaccessible
    elif resp.status_code >= 400:
        LOGGER.critical(
            "Error making request to GitLab API: GET {} [{} - {}]".format(
                url, resp.status_code, resp.content))
        sys.exit(1)

    return resp

def gen_request(url):
    if 'labels' in url:
        # The labels API is timing out for large per_page values
        #  https://gitlab.com/gitlab-org/gitlab-ce/issues/63103
        # Keeping it at 20 until the bug is fixed
        per_page = 20
    else:
        per_page = PER_PAGE_MAX

    params = {
        'page': 1,
        'per_page': per_page
    }

    # X-Total-Pages header is not always available since GitLab 11.8
    #  https://docs.gitlab.com/ee/api/#other-pagination-headers
    # X-Next-Page to check if there is another page available and iterate
    next_page = 1

    try:
        while next_page:
            params['page'] = int(next_page)
            resp = request(url, params)
            resp_json = resp.json()
            # handle endpoints that return a single JSON object
            if isinstance(resp_json, dict):
                yield resp_json
            # handle endpoints that return an array of JSON objects
            else:
                for row in resp_json:
                    yield row
            next_page = resp.headers.get('X-Next-Page', None)
    except ResourceInaccessible as exc:
        # Don't halt execution if a Resource is Inaccessible
        # Just skip it and continue with the rest of the extraction
        return []

def format_timestamp(data, typ, schema):
    result = data
    if data and typ == 'string' and schema.get('format') == 'date-time':
        rfc3339_ts = rfc3339_to_timestamp(data)
        utc_dt = datetime.datetime.utcfromtimestamp(rfc3339_ts).replace(tzinfo=pytz.UTC)
        result = utils.strftime(utc_dt)

    return result

def flatten_id(item, target):
    if target in item and item[target] is not None:
        item[target + '_id'] = item.pop(target, {}).pop('id', None)
    else:
        item[target + '_id'] = None

def sync_branches(project):
    entity = "branches"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="branches", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            flatten_id(row, "commit")
            transformed_row = transformer.transform(row, RESOURCES["branches"]["schema"], mdata)
            singer.write_record("branches", transformed_row, time_extracted=utils.now())

def sync_commits(project):
    entity = "commits"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    # Keep a state for the commits fetched per project
    state_key = "project_{}_commits".format(project["id"])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['created_at'])

    singer.write_state(STATE)

def sync_issues(project):
    entity = "issues"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    # Keep a state for the issues fetched per project
    state_key = "project_{}_issues".format(project["id"])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "assignee")
            flatten_id(row, "epic")
            flatten_id(row, "closed_by")
            flatten_id(row, "milestone")

            # Get the assignee ids
            assignee_ids = []
            for assignee in row.get("assignees"):
                assignee_ids.append(assignee["id"])
            row["assignees"] = assignee_ids

            # Get the time_stats
            time_stats = row.get("time_stats")
            if time_stats:
                row["time_estimate"] = time_stats.get("time_estimate")
                row["total_time_spent"] = time_stats.get("total_time_spent")
                row["human_time_estimate"] = time_stats.get("human_time_estimate")
                row["human_total_time_spent"] = time_stats.get("human_total_time_spent")
            else:
                row["time_estimate"] = None
                row["total_time_spent"] = None
                row["human_time_estimate"] = None
                row["human_total_time_spent"] = None

            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

    singer.write_state(STATE)

def sync_merge_requests(project):
    entity = "merge_requests"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    # Keep a state for the merge requests fetched per project
    state_key = "project_{}_merge_requests".format(project["id"])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "assignee")
            flatten_id(row, "milestone")
            flatten_id(row, "merged_by")
            flatten_id(row, "closed_by")

            # Get the assignee ids
            assignee_ids = []
            for assignee in row.get("assignees"):
                assignee_ids.append(assignee["id"])
            row["assignees"] = assignee_ids

            # Get the reviewer ids
            reviewer_ids = []
            for reviewer in row.get("reviewers"):
                reviewer_ids.append(reviewer["id"])
            row["reviewers"] = reviewer_ids

            # Get the time_stats
            time_stats = row.get("time_stats")
            if time_stats:
                row["time_estimate"] = time_stats.get("time_estimate")
                row["total_time_spent"] = time_stats.get("total_time_spent")
                row["human_time_estimate"] = time_stats.get("human_time_estimate")
                row["human_total_time_spent"] = time_stats.get("human_total_time_spent")
            else:
                row["time_estimate"] = None
                row["total_time_spent"] = None
                row["human_time_estimate"] = None
                row["human_total_time_spent"] = None

            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            # Write the MR record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # And then sync all the commits for this MR
            # (if it has changed, new commits may be there to fetch)
            sync_merge_request_commits(project, transformed_row)

    singer.write_state(STATE)

def sync_merge_request_commits(project, merge_request):
    entity = "merge_request_commits"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="merge_request_commits", id=project['id'], secondary_id=merge_request['iid'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            row['merge_request_iid'] = merge_request['iid']
            row['commit_id'] = row['id']
            row['commit_short_id'] = row['short_id']
            transformed_row = transformer.transform(row, RESOURCES["merge_request_commits"]["schema"], mdata)

            singer.write_record("merge_request_commits", transformed_row, time_extracted=utils.now())

def sync_releases(project):
    entity = "releases"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="releases", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "commit")
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES["releases"]["schema"], mdata)

            singer.write_record("releases", transformed_row, time_extracted=utils.now())


def sync_tags(project):
    entity = "tags"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="tags", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "commit")
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES["tags"]["schema"], mdata)

            singer.write_record("tags", transformed_row, time_extracted=utils.now())


def sync_milestones(entity, element="project"):
    stream_name = "{}_milestones".format(element)
    stream = CATALOG.get_stream(stream_name)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=element + "_milestones", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES[element + "_milestones"]["schema"], mdata)

            singer.write_record(element + "_milestones", transformed_row, time_extracted=utils.now())

def sync_users(project):
    entity = "users"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="users", id=project['id'])
    project["users"] = []
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["users"]["schema"], mdata)
            project["users"].append(row["id"])
            singer.write_record("users", transformed_row, time_extracted=utils.now())

def sync_site_users():
    entity = "site_users"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="site_users", id="all")
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["users"]["schema"], mdata)
            singer.write_record("site_users", transformed_row, time_extracted=utils.now())


def sync_members(entity, element="project"):
    stream_name = "{}_members".format(element)
    member_stream = CATALOG.get_stream(stream_name)
    if member_stream is None or not member_stream.is_selected():
        return
    user_stream = CATALOG.get_stream('users')
    member_mdata = metadata.to_map(member_stream.metadata)
    user_mdata = metadata.to_map(user_stream.metadata)

    url = get_url(entity=stream_name, id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            # First, write a record for the user
            if user_stream.is_selected():
                user_row = transformer.transform(row, RESOURCES["users"]["schema"], user_mdata)
                singer.write_record("users", user_row, time_extracted=utils.now())

            # And then a record for the member
            row[element + '_id'] = entity['id']
            row['user_id'] = row['id']
            member_row = transformer.transform(row, RESOURCES[element + "_members"]["schema"], member_mdata)
            singer.write_record(element + "_members", member_row, time_extracted=utils.now())


def sync_labels(entity, element="project"):
    stream_name = "{}_labels".format(element)
    stream = CATALOG.get_stream(stream_name)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=element + "_labels", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row[element + '_id'] = entity['id']
            transformed_row = transformer.transform(row, RESOURCES[element + "_labels"]["schema"], mdata)
            singer.write_record(element + "_labels", transformed_row, time_extracted=utils.now())

def sync_epic_issues(group, epic):
    entity = "epic_issues"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="epic_issues", id=group['id'], secondary_id=epic['iid'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['group_id'] = group['id']
            row['epic_iid'] = epic['iid']
            row['issue_id'] = row['id']
            row['issue_iid'] = row['iid']
            transformed_row = transformer.transform(row, RESOURCES["epic_issues"]["schema"], mdata)

            singer.write_record("epic_issues", transformed_row, time_extracted=utils.now())

def sync_epics(group):
    entity = "epics"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    # Keep a state for the epics fetched per group
    state_key = "group_{}_epics".format(group['id'])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=group['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            # Write the Epic record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # And then sync all the issues for that Epic
            # (if it has changed, new issues may be there to fetch)
            sync_epic_issues(group, transformed_row)

    singer.write_state(STATE)

def sync_group(gid, pids):
    stream = CATALOG.get_stream("groups")
    mdata = metadata.to_map(stream.metadata)
    url = get_url(entity="groups", id=gid)

    try:
        data = request(url).json()
    except ResourceInaccessible as exc:
        # Don't halt execution if a Group is Inaccessible
        # Just skip it and continue with the rest of the extraction
        return

    time_extracted = utils.now()

    if not pids:
        #  Get all the projects of the group if none are provided
        for project in data['projects']:
            if project['id']:
                sync_project(project['id'])
    else:
        # Sync only specific projects of the group, if explicit projects are provided
        for pid in pids:
            if pid.startswith(data['full_path'] + '/') or pid in [str(p['id']) for p in data['projects']]:
                sync_project(pid)

    sync_milestones(data, "group")

    sync_members(data, "group")

    sync_labels(data, "group")

    if CONFIG['ultimate_license']:
        sync_epics(data)

    if not stream.is_selected():
        return

    with Transformer(pre_hook=format_timestamp) as transformer:
        group = transformer.transform(data, RESOURCES["groups"]["schema"], mdata)
        singer.write_record("groups", group, time_extracted=time_extracted)

def sync_pipelines(project):
    entity = "pipelines"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return

    mdata = metadata.to_map(stream.metadata)
    # Keep a state for the pipelines fetched per project
    state_key = "project_{}_pipelines".format(project['id'])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):

            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            # Write the Pipeline record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # Sync additional details of a pipeline using get-a-single-pipeline endpoint
            # https://docs.gitlab.com/ee/api/pipelines.html#get-a-single-pipeline
            sync_pipelines_extended(project, transformed_row)

            # Sync all jobs attached to the pipeline.
            # Although jobs cannot be queried by updated_at, if a job changes
            # it's pipeline's updated_at is changed.
            sync_jobs(project, transformed_row)

            sync_bridges(project, transformed_row)

    singer.write_state(STATE)

def sync_bridges(project, pipeline):
    entity = "bridges"
    stream = CATALOG.get_stream(entity)
    if not stream.is_selected():
        return

    mdata = metadata.to_map(stream.metadata)
    url = get_url(entity=entity, id=project['id'], secondary_id=pipeline['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())

            if transformed_row["downstream_pipeline"]:
                # Sync additional details of a pipeline using get-a-single-pipeline endpoint
                # https://docs.gitlab.com/ee/api/pipelines.html#get-a-single-pipeline
                sync_pipelines_extended(project, transformed_row["downstream_pipeline"])

                # Sync all jobs attached to the pipeline.
                # Although jobs cannot be queried by updated_at, if a job changes
                # it's pipeline's updated_at is changed.
                sync_jobs(project, transformed_row["downstream_pipeline"])

def sync_pipelines_extended(project, pipeline):
    entity = "pipelines_extended"
    stream = CATALOG.get_stream(entity)
    if not stream.is_selected():
        return

    mdata = metadata.to_map(stream.metadata)
    url = get_url(entity=entity, id=project['id'], secondary_id=pipeline['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())

def sync_jobs(project, pipeline):
    entity = "jobs"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=entity, id=project['id'], secondary_id=pipeline['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            flatten_id(row, 'user')
            flatten_id(row, 'commit')
            flatten_id(row, 'pipeline')
            flatten_id(row, 'runner')

            transformed_row = transformer.transform(row, RESOURCES[entity]['schema'], mdata)
            singer.write_record(entity, transformed_row, time_extracted=utils.now())

def sync_project(pid):
    url = get_url(entity="projects", id=pid)

    try:
        data = request(url).json()
    except ResourceInaccessible as exc:
        # Don't halt execution if a Project is Inaccessible
        # Just skip it and continue with the rest of the extraction
        return

    time_extracted = utils.now()
    stream = CATALOG.get_stream("projects")
    mdata = metadata.to_map(stream.metadata)

    state_key = "project_{}".format(data["id"])

    #pylint: disable=maybe-no-member
    last_activity_at = data.get('last_activity_at', data.get('created_at'))
    if not last_activity_at:
        raise Exception(
            #pylint: disable=line-too-long
            "There is no last_activity_at or created_at field on project {}. This usually means I don't have access to the project."
            .format(data['id']))


    if data['last_activity_at'] >= get_start(state_key):

        sync_members(data)
        sync_users(data)
        sync_issues(data)
        sync_merge_requests(data)
        sync_commits(data)
        sync_branches(data)
        sync_milestones(data)
        sync_labels(data)
        sync_releases(data)
        sync_tags(data)
        sync_pipelines(data)

        if not stream.is_selected():
            return

        with Transformer(pre_hook=format_timestamp) as transformer:
            flatten_id(data, "owner")
            project = transformer.transform(data, RESOURCES["projects"]["schema"], mdata)
            singer.write_record("projects", project, time_extracted=time_extracted)

        utils.update_state(STATE, state_key, last_activity_at)
        singer.write_state(STATE)

def do_discover(select_all=False):
    streams = []
    api_url_regex = re.compile(r'^gitlab.com')

    for resource, config in RESOURCES.items():
        mdata = metadata.get_standard_metadata(
            schema=config["schema"],
            key_properties=config["key_properties"],
            valid_replication_keys=config.get("replication_keys"),
            replication_method=config["replication_method"],
        )

        if (
            resource in ULTIMATE_RESOURCES and not CONFIG["ultimate_license"]
        ) or (
            resource == "site_users" and api_url_regex.match(CONFIG['api_url']) is not None
        ) or (
            resource in STREAM_CONFIG_SWITCHES and not CONFIG["fetch_{}".format(resource)]
        ):
            mdata = metadata.to_list(metadata.write(metadata.to_map(mdata), (), 'inclusion', 'unsupported'))
        elif select_all:
            # If a catalog was unsupplied, we want to select all streams by default. This diverges
            # slightly from Singer recommended behavior but is necessary for backwards compatibility
            mdata = metadata.to_list(metadata.write(metadata.to_map(mdata), (), 'selected', True))

        streams.append(
            CatalogEntry(
                tap_stream_id=resource,
                stream=resource,
                schema=Schema.from_dict(config["schema"]),
                key_properties=config["key_properties"],
                metadata=mdata,
                replication_key=config.get("replication_keys"),
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=config["replication_method"],
            )
        )
    return Catalog(streams)

def do_sync():
    LOGGER.info("Starting sync")

    gids = list(filter(None, CONFIG['groups'].split(' ')))
    pids = list(filter(None, CONFIG['projects'].split(' ')))

    for stream in CATALOG.get_selected_streams(STATE):
        singer.write_schema(stream.tap_stream_id, stream.schema.to_dict(), stream.key_properties)

    sync_site_users()

    for gid in gids:
        sync_group(gid, pids)

    if not gids:
        # When not syncing groups
        for pid in pids:
            sync_project(pid)

    # Write the final STATE
    # This fixes syncing using groups, which don't emit a STATE message
    #  so the last message is not a STATE message
    #  which, in turn, breaks the behavior of some targets that expect a STATE
    #  as the last message
    # It is also a safeguard for future updates
    singer.write_state(STATE)

    LOGGER.info("Sync complete")


def main_impl():
    # TODO: Address properties that are required or not
    args = utils.parse_args(["private_token", "projects", "start_date"])
    args.config["private_token"] = args.config["private_token"].strip()

    CONFIG.update(args.config)
    CONFIG['ultimate_license'] = truthy(CONFIG['ultimate_license'])
    CONFIG['fetch_merge_request_commits'] = truthy(CONFIG['fetch_merge_request_commits'])
    CONFIG['fetch_pipelines_extended'] = truthy(CONFIG['fetch_pipelines_extended'])

    if '/api/' not in CONFIG['api_url']:
        CONFIG['api_url'] += '/api/v4'

    if args.state:
        STATE.update(args.state)

    # If discover flag was passed, log an info message and exit
    global CATALOG
    if args.discover:
        CATALOG = do_discover()
        CATALOG.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            CATALOG = args.catalog
        else:
            CATALOG = do_discover(select_all=True)
        do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
