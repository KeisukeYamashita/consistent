package consistent

// Bin represents an entity that serves the balls.
// Usually it's a server but it can be anything.
type Bin struct {
	Name         string
	PartitionIDs []PartitionID
}

// NewBin generates a bin from the passed name.
// In most cases, it's IP address of the server, hash of the metadata, etc.
// It can be anything, it's up to you.
func NewBin(name string) Bin {
	return Bin{
		Name: name,
	}
}

// String returns the bin's name.
func (b Bin) String() string {
	return b.Name
}
