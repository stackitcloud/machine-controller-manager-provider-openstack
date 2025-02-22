#!/usr/bin/env bash
#
# Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

ENVTEST_K8S_VERSION=${ENVTEST_K8S_VERSION:-"1.22"}

echo "> Installing envtest tools@${ENVTEST_K8S_VERSION} with setup-envtest if necessary"
if ! command -v setup-envtest &> /dev/null ; then
  >&2 echo "setup-envtest not available, please install it first by running 'make install-requirements'"
  exit 1
fi

# --use-env allows overwriting the envtest tools path via the KUBEBUILDER_ASSETS env var just like it was before
export KUBEBUILDER_ASSETS="$(setup-envtest use --use-env -p path ${ENVTEST_K8S_VERSION})"
echo "using envtest tools installed at '$KUBEBUILDER_ASSETS'"

echo "> Integration Tests"

export KUBEBUILDER_CONTROLPLANE_START_TIMEOUT=2m
GO111MODULE=on go test -timeout=5m -mod=vendor $@ | grep -v 'no test files'
