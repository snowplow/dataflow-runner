package cluster

type InstancesRecord struct {
	Master *MasterRecord
	Core   *CoreRecord
	Task   *TaskRecord
}
