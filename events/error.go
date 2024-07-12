package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Error struct {
	Base
	Request       *proto.CollectRequest
	RowCount      int
	ChunksWritten int
	Err           error
}

func NewErrorEvent(request *proto.CollectRequest, rowCount int, chunksWritten int, err error) *Error {
	return &Error{
		Request:       request,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Err:           err,
	}
}

func (c *Error) ToProto() *proto.Event {
	return proto.NewCompleteEvent(c.Request.ExecutionId, c.RowCount, c.ChunksWritten, c.Err)
}
