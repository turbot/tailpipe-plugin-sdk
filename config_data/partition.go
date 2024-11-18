package config_data

import "github.com/hashicorp/hcl/v2"

type PartitionConfigData struct {
	*ConfigDataImpl
	Table     string
	Partition string
}

func NewPartitionConfigData(hcl []byte, decRange hcl.Range, table string, partition string) *PartitionConfigData {
	return &PartitionConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:   hcl,
			Range: decRange,
			Id:    table,
		},
		Table:     table,
		Partition: partition,
	}
}
