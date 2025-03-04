package schema

// DescriptionProvider is an interface that can be implemented by any struct that has a description
// it is used by tables to specify the description of the table
type DescriptionProvider interface {
	GetDescription() string
}

// ColumnDescriptionProvider is an interface that can be implemented by a row struct to provide descriptions for each column
type ColumnDescriptionProvider interface {
	GetColumnDescriptions() map[string]string
}
