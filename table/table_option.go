package table

// TableOption is an option function passed to the table factory
type TableOption func(*tableConfig)

type tableConfig struct {
	name string
}

func WithName(name string) TableOption {
	return func(tc *tableConfig) {
		tc.name = name
	}
}
