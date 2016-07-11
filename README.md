# Dataflow Runner

[ ![Build Status] [travis-image] ] [travis] [ ![Release] [release-image] ] [releases] [ ![License] [license-image] ] [license]

## Overview

UNFINISHED. Run templatable playbooks of Hadoop/Spark/et al jobs on Amazon EMR

The first version of Dataflow Runner should be a command-line app which is called `dataflow-runner` and when invoked says "Hello from Dataflow Runner".

## Developer Quickstart

### Building

Assuming git, **[Vagrant] [vagrant-install]** and **[VirtualBox] [virtualbox-install]** installed:

```bash
 host> git clone https://github.com/snowplow/dataflow-runner
 host> cd sql-runner
 host> vagrant up && vagrant ssh
guest> cd /opt/gopath/src/github.com/snowplow/dataflow-runner
guest> godep go build
```

## SCRATCH BOOK

Example:

```
$ dataflow-runner
Hello from Dataflow Runner!
```

And the source code of this must have had `gofmt` run on it.

The following should be supported:

```
$ dataflow-runner version
dataflow-runner: 0.1.0
$ dataflow-runner -help
dataflow-runner: 0.1.0
Run templatable playbooks of Hadoop/Spark/et al jobs on Amazon EMR
Usage:
  -help
        Shows this message
  -version
        Shows the program version
```

We should also support the following command:

```
$ dataflow-runner up emr_cluster.json --region xxx
```

where `emr_cluster.json` contains the following contents:

```yaml
{
  "schema": "iglu:com.snowplowanalytics.dataflow-runner.emr/Cluster/avro/1-0-0",
  "data": {
      "name": "xxx",
      "roles": {
        "jobflow": "EMR_EC2_DefaultRole"
        "service": "EMR_DefaultRole"
      },
      "ec2": {
        "amiVersion": "4.5.0",
        "keyName": "snowplow-yyy-key",
        "location": {
          "classic": {
            "availabilityZone": "us-east-1a"
          } // OR 
          "vpc": {
            "subnetId": "xxx"
          }
        },
        "instances": {
          "master": {
            "type": "m1.medium"
          },
          "core": {
            "type": "c3.4xlarge",
            "count": 3,
          },
          "task": {
            "type": "m1.medium",
            "count": 0,
            "bid": 0.0.15
          }
        }
      }
    }
  }
}
```

The response should be as follows:

```
$ dataflow-runner up emr_cluster.json --region xxx
Jobflow ID: j-1WJAF3S7TRUXM
```

i.e. the configuration is used to start a new EMR cluster and return the jobflow ID for this cluster.

Under the hood, you should be using this library: https://github.com/aws/aws-sdk-go Specifically this is the documentation for EMR: http://docs.aws.amazon.com/sdk-for-go/api/service/emr/ This is a helpful example: http://docs.aws.amazon.com/sdk-for-go/api/service/emr/#example_EMR_RunJobFlow

### Creating the avro file from json 
```
$ java -jar avro-tools-1.8.0.jar fromjson --schema-file steps/schema.avsc steps.json > playbook.avro
```

### Running a playbook of jobflow steps

We should support the following command:

```
$ dataflow-runner run playbook.avro --emr-cluster j-1WJAF3S7TRUXM --region xxx
```

Which will run the `playbook.avro` on the specified cluster.

The `playbook.avro` should be a self-describing Avro contain the following contents:

```yaml
{
  "schema": "iglu:com.snowplowanalytics.dataflow-runner/Playbook/avro/1-0-0",
  "data": {
    "steps": [
      {
        "type": "CUSTOM_JAR",
        "name": "Combine Months",
        "actionOnFailure": "TERMINATE_CLUSTER",
        "jar": "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
        "arguments": [
          "--src",
          "s3n://{{my-output-bucket/enriched/bad}}/",
          "--dest",
          "hdfs:///local/monthly/"
        ]
      },
      {
        "type": "CUSTOM_JAR",
        "name": "Combine Months",
        "actionOnFailure": "CONTINUE",
        "jar": "s3://snowplow-hosted-assets/3-enrich/hadoop-event-recovery/snowplow-hadoop-event-recovery-0.2.0.jar",
        "arguments": [
          "com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob",
          "--hdfs",
          "--input",
          "hdfs:///local/monthly/*",
          "--output",
          "hdfs:///local/recovery/"
        ]
      }
    ]
  }
}
```

This will then add the specified job steps to the existing EMR cluster, and they will run.

With a --wait, the Dataflow Runner will block waiting for the job to complete before returning.

A common failure when running on EMR is that the cluster fails to Bootstrap; oftentimes simply trying again will successfully boot the cluster.

Dataflow Runner will transparently attempt to restart the cluster in the case of a bootstrap failure, up to 3 times.

For the logic used to restart the cluster in the case of a bootstrap failure see:

* https://github.com/snowplow/snowplow/blob/master/3-enrich/emr-etl-runner/lib/snowplow-emr-etl-runner/emr_job.rb#L707
* https://github.com/snowplow/snowplow/blob/master/3-enrich/emr-etl-runner/lib/snowplow-emr-etl-runner/runner.rb#L63-L82

### Schema Changes

When the schema for creating a cluster or for running job steps change we have to generate new .go files using gogen-avro so as to parse the avro files.

```
$ gogen-avro . schema.avsc
```
#### Note

* The gogen-avro creates parse files with default package avro we will have to edit to suit our needs accordingly.
* Since we have two schemas, we might have to remove any redundant code that gets generated.

## Copyright and license

Dataflow Runner is copyright 2016 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0] [license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[travis]: https://travis-ci.org/snowplow/dataflow-runner
[travis-image]: https://travis-ci.org/snowplow/dataflow-runner.png?branch=master

[release-image]: http://img.shields.io/badge/release-0.1.0-6ad7e5.svg?style=flat
[releases]: https://github.com/snowplow/sql-runner/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads
