package paging

type StorageBuckets struct {
	PagingBase
}

func NewStorageBucket() *StorageBuckets {
	return &StorageBuckets{}
}
