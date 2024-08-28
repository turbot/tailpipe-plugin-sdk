package proto

import "github.com/hashicorp/hcl/v2"

func RangeToProto(r hcl.Range) *Range {
	return &Range{
		Start:    PosToProto(r.Start),
		End:      PosToProto(r.End),
		Filename: r.Filename,
	}
}

func RangeFromProto(r *Range) hcl.Range {
	return hcl.Range{
		Start:    PosFromProto(r.Start),
		End:      PosFromProto(r.End),
		Filename: r.Filename,
	}
}

func PosFromProto(pos *Pos) hcl.Pos {
	return hcl.Pos{
		Line:   int(pos.Line),
		Column: int(pos.Column),
		Byte:   int(pos.Byte),
	}
}

func PosToProto(pos hcl.Pos) *Pos {
	return &Pos{
		Line:   int64(pos.Line),
		Column: int64(pos.Column),
		Byte:   int64(pos.Byte),
	}
}
