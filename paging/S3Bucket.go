package paging

import (
	"fmt"
	"maps"
	"time"
)

type S3Bucket struct {
	Bucket  string                      `json:"bucket"`
	Prefix  string                      `json:"prefix"`
	Region  string                      `json:"region"`
	Objects map[string]S3BucketMetadata `json:"objects"`
}

type S3BucketMetadata struct {
	LastModified time.Time `json:"last_modified"`
	Size         int64     `json:"size"`
}

func NewS3Bucket(name string, prefix string, region string) *S3Bucket {
	if region == "" {
		region = "us-east-1"
	}
	return &S3Bucket{
		Bucket:  name,
		Prefix:  prefix,
		Region:  region,
		Objects: make(map[string]S3BucketMetadata),
	}
}

// Update implements the Data interface
func (s *S3Bucket) Update(data Data) error {
	other, ok := data.(*S3Bucket)
	if !ok {
		return fmt.Errorf("cannot update S3Bucket paging data with %T", data)
	}

	// merge the objects, preferring the latest
	maps.Copy(s.Objects, other.Objects)
	return nil
}

func (s *S3Bucket) Add(name string, lastModified time.Time, size int64) {
	if lastModified.IsZero() {
		return
	}
	s.Objects[name] = S3BucketMetadata{
		LastModified: lastModified,
		Size:         size,
	}
}
