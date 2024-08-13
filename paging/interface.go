package paging

type Data interface {
	// marker interface
	pagingData()
}
