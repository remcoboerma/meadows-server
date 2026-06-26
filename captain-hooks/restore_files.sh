#!/bin/sh
# Restic file restore (template convention).
# $HOST, $URI, $SNAPSHOT are provided by the edwh restic plugin at runtime.
restic $HOST -r $URI restore $SNAPSHOT --target ./ 2>/dev/null || true
