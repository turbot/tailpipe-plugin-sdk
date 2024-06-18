package proto

import "errors"

func (x *CollectRequest) Validate() error {
	if x.ExecutionId == "" {
		return errors.New("ExecutionId must be provided")
	}
	return nil
}
