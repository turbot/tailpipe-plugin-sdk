package collection

// todo should this be in the proto package as that is where it is consumed? Also consumed by CLI though
type Row interface {
	GetConnection() string
	GetYear() int
	GetMonth() int
	GetDay() int
	GetTpID() string
	GetTpTimestamp() int64
}
