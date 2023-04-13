#!/bin/bash

set -euo pipefail

DAGSTER_HOME=$(realpath dagster-home) dagster dev
