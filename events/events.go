package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Event interface {
	ToProto() *proto.Event
}

type Row struct {
	ExecutionId string
	Row         any
}

func NewRow(executionId string, row any) *Row {
	return &Row{
		ExecutionId: executionId,
		Row:         row,
	}
}

// ToProto converts the event to a proto.Event
func (r *Row) ToProto() *proto.Event {
	// there is no proto for a row event
	// we should never call toProto for a Row event
	panic("Row event should not be converted to proto")
}

type Chunk struct {
	ExecutionId string
	ChunkNumber int
}

func NewChunk(executionId string, chunkNumber int) *Chunk {
	return &Chunk{
		ExecutionId: executionId,
		ChunkNumber: chunkNumber,
	}
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return proto.NewChunkWrittenEvent(r.ExecutionId, r.ChunkNumber)
}

type Started struct {
	ExecutionId string
}

func NewStarted(executionId string) *Started {
	return &Started{
		ExecutionId: executionId,
	}
}

func (s *Started) ToProto() *proto.Event {
	return proto.NewStartedEvent(s.ExecutionId)
}

type Completed struct {
	ExecutionId   string
	RowCount      int
	ChunksWritten int
	Err           error
}

func NewCompleted(executionId string, rowCount int, chunksWritten int, err error) *Completed {
	return &Completed{
		ExecutionId:   executionId,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Err:           err,
	}
}

func (c *Completed) ToProto() *proto.Event {
	return proto.NewCompleteEvent(c.ExecutionId, c.RowCount, c.ChunksWritten, c.Err)
}
