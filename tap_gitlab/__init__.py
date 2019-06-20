#!/usr/bin/env python3

import datetime
import sys
import os
import requests
import singer
from singer import Transformer, utils

import pytz
import backoff
from strict_rfc3339 import rfc3339_to_timestamp

PER_PAGE_MAX = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v3",
    'private_token': None,
    'start_date': None,
    'groups': '',
    'ultimate_license': False
}
STATE = {}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

RESOURCES = {
    'projects': {
        'url': '/projects/{id}?statistics=1',
        'schema': load_schema('projects'),
        'key_properties': ['id'],
    },
    'branches': {
        'url': '/projects/{id}/repository/branches',
        'schema': load_schema('branches'),
        'key_properties': ['project_id', 'name'],
    },
    'commits': {
        'url': '/projects/{id}/repository/commits?since={start_date}',
        'schema': load_schema('commits'),
        'key_properties': ['id'],
    },
    'issues': {
        'url': '/projects/{id}/issues?scope=all&updated_after={start_date}',
        'schema': load_schema('issues'),
        'key_properties': ['id'],
    },
    'merge_requests': {
        'url': '/projects/{id}/merge_requests?scope=all&updated_after={start_date}',
        'schema': load_schema('merge_requests'),
        'key_properties': ['id'],
    },
    'project_milestones': {
        'url': '/projects/{id}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
    },
    'group_milestones': {
        'url': '/groups/{id}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
    },
    'users': {
        'url': '/projects/{id}/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
    },
    'groups': {
        'url': '/groups/{id}',
        'schema': load_schema('groups'),
        'key_properties': ['id'],
    },
    'project_members': {
        'url': '/projects/{id}/members',
        'schema': load_schema('project_members'),
        'key_properties': ['project_id', 'id'],
    },
    'group_members': {
        'url': '/groups/{id}/members',
        'schema': load_schema('group_members'),
        'key_properties': ['group_id', 'id'],
    },
    'releases': {
        'url': '/projects/{id}/releases',
        'schema': load_schema('releases'),
        'key_properties': ['project_id', 'commit_id', 'tag_name'],
    },
    'tags': {
        'url': '/projects/{id}/repository/tags',
        'schema': load_schema('tags'),
        'key_properties': ['project_id', 'commit_id', 'name'],
    },
    'project_labels': {
        'url': '/projects/{id}/labels',
        'schema': load_schema('project_labels'),
        'key_properties': ['project_id', 'id'],
    },
    'group_labels': {
        'url': '/groups/{id}/labels',
        'schema': load_schema('group_labels'),
        'key_properties': ['group_id', 'id'],
    },
    'epics': {
        'url': '/groups/{id}/epics?updated_after={start_date}',
        'schema': load_schema('epics'),
        'key_properties': ['group_id', 'id'],
    },
    'epic_issues': {
        'url': '/groups/{id}/epics/{secondary_id}/issues',
        'schema': load_schema('epic_issues'),
        'key_properties': ['group_id', 'epic_iid', 'epic_issue_id'],
    },
}

ULTIMATE_RESOURCES = ("epics", "epic_issues")

LOGGER = singer.get_logger()
SESSION = requests.Session()

TRUTHY = ("true", "1", "yes", "on")

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
    if entity not in STATE:
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

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        LOGGER.critical(
            "Error making request to GitLab API: GET {} [{} - {}]".format(
                req.url, resp.status_code, resp.content))
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

    while next_page:
        params['page'] = int(next_page)
        resp = request(url, params)
        for row in resp.json():
            yield row
        next_page = resp.headers.get('X-Next-Page', None)

def format_timestamp(data, typ, schema):
    result = data
    if typ == 'string' and schema.get('format') == 'date-time':
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
    url = get_url(entity="branches", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            flatten_id(row, "commit")
            transformed_row = transformer.transform(row, RESOURCES["branches"]["schema"])
            singer.write_record("branches", transformed_row, time_extracted=utils.now())

def sync_commits(project):
    entity = "commits"
    # Keep a state for the commits fetched per project
    state_key = "project_{}_commits".format(project["id"])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"])

            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['created_at'])

    singer.write_state(STATE)

def sync_issues(project):
    entity = "issues"
    # Keep a state for the issues fetched per project
    state_key = "project_{}_issues".format(project["id"])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "assignee")
            flatten_id(row, "milestone")
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"])

            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

    singer.write_state(STATE)


def sync_merge_requests(project):
    entity = "merge_requests"
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
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"])

            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

    singer.write_state(STATE)

def sync_releases(project):
    url = get_url(entity="releases", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "commit")
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES["releases"]["schema"])

            singer.write_record("releases", transformed_row, time_extracted=utils.now())


def sync_tags(project):
    url = get_url(entity="tags", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "commit")
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES["tags"]["schema"])

            singer.write_record("tags", transformed_row, time_extracted=utils.now())


def sync_milestones(entity, element="project"):
    url = get_url(entity=element + "_milestones", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES[element + "_milestones"]["schema"])

            if row["updated_at"] >= get_start(element + "_{}".format(entity["id"])):
                singer.write_record(element + "_milestones", transformed_row, time_extracted=utils.now())

def sync_users(project):
    url = get_url(entity="users", id=project['id'])
    project["users"] = []
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["users"]["schema"])
            project["users"].append(row["id"])
            singer.write_record("users", transformed_row, time_extracted=utils.now())


def sync_members(entity, element="project"):
    url = get_url(entity=element + "_members", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            # First, write a record for the user
            user_row = transformer.transform(row, RESOURCES["users"]["schema"])
            singer.write_record("users", user_row, time_extracted=utils.now())

            # And then a record for the member
            row[element + '_id'] = entity['id']
            row['user_id'] = row['id']
            member_row = transformer.transform(row, RESOURCES[element + "_members"]["schema"])
            singer.write_record(element + "_members", member_row, time_extracted=utils.now())


def sync_labels(entity, element="project"):
    url = get_url(entity=element + "_labels", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row[element + '_id'] = entity['id']
            transformed_row = transformer.transform(row, RESOURCES[element + "_labels"]["schema"])
            singer.write_record(element + "_labels", transformed_row, time_extracted=utils.now())

def sync_epic_issues(group, epic):
    url = get_url(entity="epic_issues", id=group['id'], secondary_id=epic['iid'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['group_id'] = group['id']
            row['epic_iid'] = epic['iid']
            row['issue_id'] = row['id']
            row['issue_iid'] = row['iid']
            transformed_row = transformer.transform(row, RESOURCES["epic_issues"]["schema"])

            singer.write_record("epic_issues", transformed_row, time_extracted=utils.now())

def sync_epics(group):
    entity = "epics"
    # Keep a state for the epics fetched per group
    state_key = "group_{}_epics".format(group['id'])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=group['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"])

            # Write the Epic record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # And then sync all the issues for that Epic
            # (if it has changed, new issues may be there to fetch)
            sync_epic_issues(group, transformed_row)

    singer.write_state(STATE)

def sync_group(gid, pids):
    url = get_url(entity="groups", id=gid)

    data = request(url).json()
    time_extracted = utils.now()

    with Transformer(pre_hook=format_timestamp) as transformer:
        group = transformer.transform(data, RESOURCES["groups"]["schema"])

    if not pids:
        #  Get all the projects of the group if none are provided
        for project in group['projects']:
            if project['id']:
                pids.append(project['id'])

    for pid in pids:
        sync_project(pid)

    sync_milestones(group, "group")

    sync_members(group, "group")

    sync_labels(group, "group")

    if CONFIG['ultimate_license']:
        sync_epics(group)

    singer.write_record("groups", group, time_extracted=time_extracted)


def sync_project(pid):
    url = get_url(entity="projects", id=pid)
    data = request(url).json()
    time_extracted = utils.now()

    with Transformer(pre_hook=format_timestamp) as transformer:
        flatten_id(data, "owner")
        project = transformer.transform(data, RESOURCES["projects"]["schema"])

    state_key = "project_{}".format(project["id"])

    #pylint: disable=maybe-no-member
    last_activity_at = project.get('last_activity_at', project.get('created_at'))
    if not last_activity_at:
        raise Exception(
            #pylint: disable=line-too-long
            "There is no last_activity_at or created_at field on project {}. This usually means I don't have access to the project."
            .format(project['id']))


    if project['last_activity_at'] >= get_start(state_key):

        sync_members(project)
        sync_users(project)
        sync_issues(project)
        sync_merge_requests(project)
        sync_commits(project)
        sync_branches(project)
        sync_milestones(project)
        sync_labels(project)
        sync_releases(project)
        sync_tags(project)

        singer.write_record("projects", project, time_extracted=time_extracted)
        utils.update_state(STATE, state_key, last_activity_at)
        singer.write_state(STATE)


def do_sync():
    LOGGER.info("Starting sync")

    gids = list(filter(None, CONFIG['groups'].split(' ')))
    pids = list(filter(None, CONFIG['projects'].split(' ')))

    for resource, config in RESOURCES.items():
        if (resource not in ULTIMATE_RESOURCES) or CONFIG['ultimate_license']:
            singer.write_schema(resource, config['schema'], config['key_properties'])

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

    CONFIG.update(args.config)
    CONFIG['ultimate_license'] = truthy(CONFIG['ultimate_license'])

    if args.state:
        STATE.update(args.state)

    # If discover flag was passed, log an info message and exit
    if args.discover:
        LOGGER.info('Schema discovery is not supported by tap-gitlab')
        sys.exit(1)
    # Otherwise run in sync mode
    else:
        do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
