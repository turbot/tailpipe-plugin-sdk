package schema

// GetDescription is an interface that can be implemented by any struct that has a description
// it is used by tables to specify the description of the table
type GetDescription interface {
	GetDescription() string
}

// GetColumnDescriptions is an interface that can be implemented by a row struct to provide descriptions for each column
type GetColumnDescriptions interface {
	GetColumnDescriptions() map[string]string
}
