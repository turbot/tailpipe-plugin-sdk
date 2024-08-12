package paging

type StorageBuckets struct{}

func NewStorageBucket() *StorageBuckets {
	return &StorageBuckets{}
}

// Update implements the Data interface
func (s *StorageBuckets) Update(Data) error {
	// TODO: #paging implement paging for storage buckets
	return nil
}
