package source

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type ArtifactInfo struct {
	Name string
}

func (i ArtifactInfo) ToProto() *proto.ArtifactInfo {
	return &proto.ArtifactInfo{
		Name: i.Name,
	}
}

type Artifact struct {
	ArtifactInfo
	Data []byte
}

func (a *Artifact) FromInfo(info *ArtifactInfo) *Artifact {
	a.ArtifactInfo = *info
	return a
}

func (a *Artifact) ToProto() *proto.Artifact {
	return &proto.Artifact{
		ArtifactInfo: a.ArtifactInfo.ToProto(),
		Data:         a.Data,
	}
}
