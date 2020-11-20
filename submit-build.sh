#!/usr/bin/env sh

gcloud builds submit . --substitutions SHORT_SHA=$(git rev-parse --short HEAD)