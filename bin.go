package consistent

type Bin struct {
	Name         string
	PartitionIDs []PartitionID
}

func NewBin(name string) Bin {
	return Bin{
		Name: name,
	}
}

func (b Bin) String() string {
	return b.Name
}
