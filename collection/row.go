package collection

type Row interface {
	GetConnection() string
	GetYear() int
	GetMonth() int
	GetDay() int
	GetTpID() string
	GetTpTimestamp() int64
}
