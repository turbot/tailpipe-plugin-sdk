package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"time"
)

// InitSourceRequest is an sdk type which is mapped from the proto.InitSourceRequest
type InitSourceRequest struct {
	// the source config to use (with raw config)
	SourceData *SourceConfigData
	// the source format to use (with raw config)
	SourceFormat *FormatConfigData
	// the raw hcl of the connection
	ConnectionData *ConnectionConfigData
	// this is json encoded data that represents the state of the collection, i.e. what data has been collected
	// this is used to resume a collection
	CollectionStatePath string
	// the time to start collecting data from
	FromTime time.Time
}

func InitSourceRequestFromProto(pr *proto.InitSourceRequest) (*InitSourceRequest, error) {
	sourceData, err := DataFromProto[*SourceConfigData](pr.SourceData)
	if err != nil {
		return nil, err
	}

	req := &InitSourceRequest{
		SourceData:          sourceData,
		CollectionStatePath: pr.CollectionStatePath,
		FromTime:            pr.FromTime.AsTime(),
	}

	if pr.SourceFormat != nil {
		sourceFormat, err := DataFromProto[*FormatConfigData](pr.SourceFormat)
		if err != nil {
			return nil, err
		}
		req.SourceFormat = sourceFormat
	}

	if pr.ConnectionData != nil {
		connectionData, err := DataFromProto[*ConnectionConfigData](pr.ConnectionData)
		if err != nil {
			return nil, err
		}
		req.ConnectionData = connectionData
	}

	return req, nil
}
