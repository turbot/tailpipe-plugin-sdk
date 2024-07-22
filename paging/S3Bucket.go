package paging

type S3Buckets struct {
	// TODO figure out paging for s3
}

func NewS3Bucket() *S3Buckets {
	return &S3Buckets{}
}

// Update implements the Data interface
func (s *S3Buckets) Update(Data) error {
	return nil
}
