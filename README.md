# GitLab Tap

## Install

```bash
git clone git@gitlab.com:meltano/tap-gitlab.git
cd tap-gitlab
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Setup

1. Update config.json with a valid api_key from gitlab personal settings.

## Run

```bash
cd tap-gitlab
. ./venv/bin/activate
tap-gitlab --config config.json
```

Feel free to pipe to a target.