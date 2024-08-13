package paging

import (
	"sync"
	"time"
)

type S3Bucket struct {
	Bucket  string                       `json:"bucket"`
	Prefix  string                       `json:"prefix"`
	Region  string                       `json:"region"`
	Objects map[string]*S3BucketMetadata `json:"objects"`

	objectLock sync.RWMutex
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
		Objects: make(map[string]*S3BucketMetadata),
	}
}

// Upsert adds new/updates an existing object with its current metadata
func (s *S3Bucket) Upsert(name string, lastModified time.Time, size int64) {
	s.objectLock.Lock()
	defer s.objectLock.Unlock()

	s.Objects[name] = &S3BucketMetadata{
		LastModified: lastModified,
		Size:         size,
	}
}

// Get returns the metadata for the given path (if it is currently stored) or null if not found
func (s *S3Bucket) Get(path string) *S3BucketMetadata {
	s.objectLock.RLock()
	defer s.objectLock.RUnlock()

	metadata, _ := s.Objects[path]
	// return metadata (or null if it does not exist)
	return metadata
}

// implement marker interface
func (*S3Bucket) pagingData() {}
