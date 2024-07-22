package paging

type Data interface {
	// update this data with another paging data
	Update(Data) error
}
