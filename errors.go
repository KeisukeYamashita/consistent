package consistent

import "errors"

var (
	//ErrInsufficientBins represents an error which means there are not enough bins to complete the task.
	ErrInsufficientBins = errors.New("insufficient bins")

	// ErrBallNotFound represents an error which means requested ball could not be found in consistent hash ring.
	ErrBallNotFound = errors.New("ball not found")

	// ErrBinNotFound represents an error which means requested bin could not be found in consistent hash ring.
	ErrBinNotFound = errors.New("bin not found")

	// ErrBinAlreadyExist represents an error which means requested bin already exists in the ring
	ErrBinAlreadyExist = errors.New("bin already exist")

	// ErrInsufficientPartitionCapacity represents an error which user needs to decrease partition count, increase bin count or increase load factor.
	ErrInsufficientPartitionCapacity = errors.New("not enough room to distribute partitions")
)
