package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Event interface {
	ToProto() *proto.Event
}

type Row struct {
	Request    *proto.CollectRequest
	Connection string
	Row        any
}

func NewRowEvent(request *proto.CollectRequest, connection string, row any) *Row {
	return &Row{
		Request:    request,
		Connection: connection,
		Row:        row,
	}
}

// ToProto converts the event to a proto.Event
func (r *Row) ToProto() *proto.Event {
	// there is no proto for a row event
	// we should never call toProto for a Row event
	panic("Row event should not be converted to proto")
}

type Chunk struct {
	Request     *proto.CollectRequest
	ChunkNumber int
}

func NewChunkEvent(request *proto.CollectRequest, chunkNumber int) *Chunk {
	return &Chunk{
		Request:     request,
		ChunkNumber: chunkNumber,
	}
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return proto.NewChunkWrittenEvent(r.Request.ExecutionId, r.ChunkNumber)
}

type Started struct {
	Request *proto.CollectRequest
}

func NewStartedEvent(request *proto.CollectRequest) *Started {
	return &Started{
		Request: request,
	}
}

func (s *Started) ToProto() *proto.Event {
	return proto.NewStartedEvent(s.Request.ExecutionId)
}

type Completed struct {
	Request       *proto.CollectRequest
	RowCount      int
	ChunksWritten int
	Err           error
}

func NewCompletedEvent(request *proto.CollectRequest, rowCount int, chunksWritten int, err error) *Completed {
	return &Completed{
		Request:       request,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Err:           err,
	}
}

func (c *Completed) ToProto() *proto.Event {
	return proto.NewCompleteEvent(c.Request.ExecutionId, c.RowCount, c.ChunksWritten, c.Err)
}
