//
// Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Apache License Version 2.0,
// and you may not use this file except in compliance with the Apache License Version 2.0.
// You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the Apache License Version 2.0 is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
//

package main

var ClusterRecord1 = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "classic": {
          "availabilityZone": "us-east-1a"
        },
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium"
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3
        },
        "task": {
          "type": "m1.medium",
          "count": 1,
          "bid": "0.015"
        }
      }
    }
  }
}`

var ClusterRecord2 = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "iam",
      "secretAccessKey": "iam"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium"
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3
        },
        "task": {
          "type": "m1.medium",
          "count": 0,
          "bid": "0.015"
        }
      }
    }
  }
}`

var ClusterRecordWithEBS = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "classic": {
          "availabilityZone": "us-east-1a"
        },
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium",
          "ebsConfiguration": {
            "ebsOptimized": true,
            "ebsBlockDeviceConfigs": [
              {
                "volumesPerInstance": 12,
                "volumeSpecification": {
                  "iops": 8,
                  "sizeInGB": 10,
                  "volumeType": "gp2"
                }
              }
            ]
          }
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3,
          "ebsConfiguration": {
            "ebsOptimized": false,
            "ebsBlockDeviceConfigs": [
              {
                "volumesPerInstance": 8,
                "volumeSpecification": {
                  "iops": 20,
                  "sizeInGB": 4,
                  "volumeType": "io1"
                }
              }
            ]
          }
        },
        "task": {
          "type": "m1.medium",
          "count": 1,
          "bid": "0.015",
          "ebsConfiguration": {
            "ebsOptimized": false,
            "ebsBlockDeviceConfigs": [
              {
                "volumesPerInstance": 4,
                "volumeSpecification": {
                  "iops": 100,
                  "sizeInGB": 6,
                  "volumeType": "standard"
                }
              }
            ]
          }
        }
      }
    }
  }
}`

var ClusterRecordWithApps = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "classic": {
          "availabilityZone": "us-east-1a"
        },
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium"
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3
        },
        "task": {
          "type": "m1.medium",
          "count": 0,
          "bid": "0.015"
        }
      }
    },
    "applications": [ "Hadoop", "Spark" ]
  }
}`

var ClusterRecordWithTags = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "classic": {
          "availabilityZone": "us-east-1a"
        },
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium"
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3
        },
        "task": {
          "type": "m1.medium",
          "count": 0,
          "bid": "0.015"
        }
      }
    },
    "tags": [
      {
        "key": "hello",
        "value": "world"
      }
    ]
  }
}`

var ClusterRecordWithActions = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "classic": {
          "availabilityZone": "us-east-1a"
        },
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium"
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3
        },
        "task": {
          "type": "m1.medium",
          "count": 0,
          "bid": "0.015"
        }
      }
    },
    "bootstrapActionConfigs": [
      {
        "name": "Bootstrap Action",
        "scriptBootstrapAction": {
          "path": "s3://snowplow/script.sh",
          "args": [ "1.5" ]
        }
      }
    ]
  }
}`

var ClusterRecordWithConfigs = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-0-0",
  "data": {
    "name": "xxx",
    "logUri": "s3://logging/",
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "roles": {
      "jobflow": "EMR_EC2_DefaultRole",
      "service": "EMR_DefaultRole"
    },
    "ec2": {
      "amiVersion": "4.5.0",
      "keyName": "snowplow-yyy-key",
      "location": {
        "classic": {
          "availabilityZone": "us-east-1a"
        },
        "vpc": {
          "subnetId": "subnet-123456"
        }
      },
      "instances": {
        "master": {
          "type": "m1.medium"
        },
        "core": {
          "type": "c3.4xlarge",
          "count": 3
        },
        "task": {
          "type": "m1.medium",
          "count": 0,
          "bid": "0.015"
        }
      }
    },
    "configurations": [
      {
        "classification": "c",
        "properties": {
          "key": "value"
        }
      }
    ]
  }
}`

var PlaybookRecord1 = `{
  "schema": "iglu:com.snowplowanalytics.dataflowrunner/PlaybookConfig/avro/1-0-0",
  "data": {
    "region": "us-east-1",
    "credentials": {
      "accessKeyId": "env",
      "secretAccessKey": "env"
    },
    "steps": [
      {
        "type": "CUSTOM_JAR",
        "name": "Combine Months",
        "actionOnFailure": "CANCEL_AND_WAIT",
        "jar": "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
        "arguments": [
          "--src",
          "s3n://my-output-bucket/enriched/bad/",
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
}`
