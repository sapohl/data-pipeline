#!/bin/bash

VERSION=0.8

tar czvf executive-report-v4-${VERSION}.tar.gz exec.toml run.sh schema_template.exec.json backfill.sh
