package cluster

type Ec2Record struct {
	AmiVersion string
	KeyName    string
	Location   *LocationRecord
	Instances  *InstancesRecord
}
