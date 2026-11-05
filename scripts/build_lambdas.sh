#!/usr/bin/env bash
# Stage Lambda code into build/ so Terraform can zip it.
#   build/processor : handler + anomaly detector + aws-xray-sdk
#   build/canary    : handler + the llm_observability SDK
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"

rm -rf "$BUILD/processor" "$BUILD/canary"
mkdir -p "$BUILD/processor" "$BUILD/canary"

cp "$ROOT/lambdas/processor/"*.py "$BUILD/processor/"
cp "$ROOT/lambdas/canary/"*.py "$BUILD/canary/"
cp -r "$ROOT/sdk/llm_observability" "$BUILD/canary/"

# X-Ray SDK gives per-call subsegments; skip with NO_XRAY_SDK=1 (tracing still works at function level)
if [[ "${NO_XRAY_SDK:-0}" != "1" ]]; then
  # Linux arm64 / Python 3.12 wheels for Lambda; --no-deps so we never shadow the runtime boto3/botocore
  PIP_ARGS=(--quiet --platform manylinux2014_aarch64 --implementation cp --python-version 3.12 --only-binary=:all:)
  python3 -m pip install "${PIP_ARGS[@]}" --no-deps --target "$BUILD/processor" aws-xray-sdk==2.14.0 wrapt
  python3 -m pip install "${PIP_ARGS[@]}" --no-deps --target "$BUILD/canary" aws-xray-sdk==2.14.0 wrapt
fi

find "$BUILD" -name "__pycache__" -type d -prune -exec rm -rf {} +
echo "Lambda code staged in $BUILD"
