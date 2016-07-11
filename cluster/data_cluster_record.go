package cluster

type DataClusterRecord struct {
	Name        string
	Credentials *CredentialsRecord
	Roles       *RolesRecord
	Ec2         *Ec2Record
}
