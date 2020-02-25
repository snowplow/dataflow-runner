# Dataflow Runner

[![Build Status][travis-image]][travis] [![Release][release-image]][releases] [![License][license-image]][license] [![Coverage][coveralls-image]][coveralls] [![Go Report Card][go-report-image]][go-report]

## Overview

Run templatable playbooks of Hadoop/Spark/et al jobs on Amazon EMR.

|  **[Devops Guide][devops-guide]**     | **[Analysts Guide][analysts-guide]**     | **[Developers Guide][developers-guide]**     |
|:--------------------------------------:|:-----------------------------------------:|:---------------------------------------------:|
|  [![i1][devops-image]][devops-guide] | [![i2][analysts-image]][analysts-guide] | [![i3][developers-image]][developers-guide] |

## Quickstart

Assuming you are running on 64bit Linux:

```bash
host> wget http://dl.bintray.com/snowplow/snowplow-generic/dataflow_runner_0.4.2_linux_amd64.zip
host> unzip dataflow_runner_0.4.2_linux_amd64.zip
host> ./dataflow-runner --help
```

## Copyright and license

Dataflow Runner is copyright 2016-2020 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[travis]: https://travis-ci.org/snowplow/dataflow-runner
[travis-image]: https://travis-ci.org/snowplow/dataflow-runner.png?branch=master

[release-image]: http://img.shields.io/badge/release-0.4.2-6ad7e5.svg?style=flat
[releases]: https://github.com/snowplow/dataflow-runner/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[coveralls-image]: https://coveralls.io/repos/github/snowplow/dataflow-runner/badge.svg
[coveralls]: https://coveralls.io/github/snowplow/dataflow-runner

[go-report-image]: https://goreportcard.com/badge/github.com/snowplow/dataflow-runner
[go-report]: https://goreportcard.com/report/github.com/snowplow/dataflow-runner

[analysts-guide]: https://github.com/snowplow/dataflow-runner/wiki/Guide-for-analysts
[developers-guide]: https://github.com/snowplow/dataflow-runner/wiki/Guide-for-developers
[devops-guide]: https://github.com/snowplow/dataflow-runner/wiki/Guide-for-devops

[devops-image]:  http://sauna-github-static.s3-website-us-east-1.amazonaws.com/devops.svg
[analysts-image]: http://sauna-github-static.s3-website-us-east-1.amazonaws.com/analyst.svg
[developers-image]:  http://sauna-github-static.s3-website-us-east-1.amazonaws.com/developer.svg
