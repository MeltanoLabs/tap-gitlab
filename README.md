# My First Tap

## Install

```bash
git clone git@gitlab.com:meltano/tap-first.git
cd tap-first
python -m venv venv
pip install -e .
```

## Setup

1. Update config.json with a valid api_key from gitlab personal settings.

## Run

```bash
cd first_tap
. ./venv/bin/activate
tap-first --config config.json
```

Feel free to pipe to a target.