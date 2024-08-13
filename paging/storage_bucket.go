package paging

type StorageBuckets struct{}

func NewStorageBucket() *StorageBuckets {
	return &StorageBuckets{}
}

// implement marker interface
func (*StorageBuckets) pagingData() {}
