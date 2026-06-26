#!/bin/sh
# Restic file backup (template convention).
# $HOST, $URI, $SNAPSHOT are provided by the edwh restic plugin at runtime.
restic $HOST -r $URI backup ./backups --tag files 2>/dev/null || true
