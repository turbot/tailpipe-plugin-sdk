package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

func EventFromProto(e *proto.Event) Event {
	panic("unimplemented")
	switch e.Event.(type) {

	// TODO need to define proto events for all events a source can send
	}
	return nil

}
