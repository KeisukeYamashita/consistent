package consistent

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
)

// PartitionID represents the ID of the partition.
type PartitionID int

// Config represents a configuration of the consistent hashing.
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Partition represents the number of partitions created on a ring.
	// Partitions are used to divide the ring and assign bin and ball.
	// Balls are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big number if you have too many keys.
	Partition int

	// Bins are replicated on consistent hash ring.
	// It's known as virtual nodes to uniform the distribution.
	ReplicationFactor int

	// LoadBalancingParameter is used to calculate average load.
	// According to the Google paper, one or more bins will be adjusted so that they do not exceed a specific load.
	// The maximum number of partitions are calculated by LoadBalancingParameter * (number of balls/number of bins).
	LoadBalancingParameter float64
}

// Consistent represents the consistent hashing ring.
type Consistent struct {
	mu sync.RWMutex

	hasher                 Hasher
	partition              uint64
	replicationFactor      int
	loadBalancingParameter float64

	// load is a mapping of a bin and it's load (partitions).
	loads map[string][]PartitionID

	// bins is a mapping of raw bin string and a bin.
	bins map[string]*Bin

	// balls maps the partition and the ball
	balls map[PartitionID]Ball

	// partitions is a mapping partition ID to a bin.
	partitions map[PartitionID]*Bin

	// ring is a mapping hash to a bin.
	ring map[uint64]*Bin

	// sortedSet holds the sorted bins in the ring
	sortedSet []uint64
}

// New generates a new Consistent by passed config.
func New(cfg *Config, bins []Bin) (*Consistent, error) {
	c := &Consistent{
		hasher:                 cfg.Hasher,
		balls:                  map[PartitionID]Ball{},
		bins:                   make(map[string]*Bin),
		loadBalancingParameter: cfg.LoadBalancingParameter,
		partition:              uint64(cfg.Partition),
		replicationFactor:      cfg.ReplicationFactor,
		ring:                   make(map[uint64]*Bin),
	}
	for _, bin := range bins {
		c.add(bin)
	}
	if bins != nil {
		if err := c.distributePartitions(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Add adds a new bin to the consistent hash ring.
// After adding the bin, it will recalculate the partitions.
func (c *Consistent) Add(bin Bin) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.bins[bin.String()]; ok {
		return ErrBinAlreadyExist
	}

	c.add(bin)
	if err := c.distributePartitions(); err != nil {
		return err
	}
	c.relocate()
	return nil
}

// add replicates the bin by replication factor and stores to the ring.
func (c *Consistent) add(bin Bin) {
	for i := 0; i < c.replicationFactor; i++ {
		key := []byte(fmt.Sprintf("%d%s", i, bin.String()))
		h := c.hasher.Sum64(key)
		c.ring[h] = &bin
		c.sortedSet = append(c.sortedSet, h)
	}
	// sort hashes ascending
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
	// storing bin at this map is useful to find backup bins of a partition.
	c.bins[bin.String()] = &bin
}

func (c *Consistent) delSlice(val uint64) {
	for i := 0; i < len(c.sortedSet); i++ {
		if c.sortedSet[i] == val {
			c.sortedSet = append(c.sortedSet[:i], c.sortedSet[i+1:]...)
			break
		}
	}
}

// Delete removes a ball from the ring.
func (c *Consistent) Delete(ball Ball) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var exist bool
	newBalls := map[PartitionID]Ball{}
	for partID, b := range c.balls {
		if b.String() == ball.String() {
			exist = true
			continue
		}

		newBalls[partID] = b
	}

	if !exist {
		return ErrBallNotFound
	}

	c.balls = newBalls
	return nil
}

// distributePartitions calculates the partitions and each loads of the bin.
func (c *Consistent) distributePartitions() error {
	loads := make(map[string][]PartitionID)
	for _, bin := range c.bins {
		loads[bin.String()] = []PartitionID{}
	}
	partitions := make(map[PartitionID]*Bin)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partition; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.hasher.Sum64(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		if err := c.distributeWithLoad(PartitionID(partID), idx, partitions, loads); err != nil {
			return err
		}
	}

	c.partitions = partitions
	c.loads = loads
	return nil
}

// distributeWithLoad calculates the average load and assign the partition to a bin.
func (c *Consistent) distributeWithLoad(partID PartitionID, idx int, partitions map[PartitionID]*Bin, loads map[string][]PartitionID) error {
	maxLoad := c.MaximumLoad()
	var count int
	for {
		count++
		if count >= len(c.sortedSet) {
			return ErrInsufficientPartitionCapacity
		}
		i := c.sortedSet[idx]
		bin := *c.ring[i]
		load := float64(len(loads[bin.String()]))
		if load+1 <= maxLoad {
			partitions[partID] = &bin
			loads[bin.String()] = append(loads[bin.String()], partID)
			return nil
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

// FindPartitionID returns partition id for given key.
func (c *Consistent) FindPartitionID(key []byte) PartitionID {
	hkey := c.hasher.Sum64(key)
	return PartitionID(hkey % c.partition)
}

// GetBin returns a thread-safe copy of bins.
func (c *Consistent) GetBin(name string) (*Bin, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	bin, exist := c.bins[name]
	if exist {
		// create a thread-safe copy of bin list.
		bin2 := *bin
		return &bin2, nil
	}

	return nil, ErrBinNotFound
}

// GetBalls returns the balls associated with the Bin
func (c *Consistent) GetBalls(bin Bin) ([]Ball, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	partitionIDs, exist := c.loads[bin.String()]
	if !exist {
		return nil, ErrBinNotFound
	}

	res := []Ball{}
	for _, id := range partitionIDs {
		balls, exist := c.balls[id]
		if !exist {
			continue
		}

		res = append(res, balls)
	}

	return res, nil
}

// GetBins returns a thread-safe copy of bins.
func (c *Consistent) GetBins() []Bin {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy of bin list.
	bins := make([]Bin, 0, len(c.bins))
	for _, bin := range c.bins {
		bins = append(bins, *bin)
	}
	return bins
}

// GetPartitionOwner returns the owner of the given partition.
func (c *Consistent) GetPartitionOwner(partID PartitionID) *Bin {
	c.mu.RLock()
	defer c.mu.RUnlock()

	bin, ok := c.partitions[partID]
	if !ok {
		return nil
	}
	// Create a thread-safe copy of bin and return it.

	bin2 := *bin
	return &bin2
}

// LoadDistribution exposes load distribution of bins.
func (c *Consistent) LoadDistribution() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy
	res := make(map[string]float64)
	for bin, partitions := range c.loads {
		res[bin] = float64(len(partitions))
	}
	return res
}

// Locate finds a home for given ball
func (c *Consistent) Locate(ball Ball) *Bin {
	c.mu.Lock()
	partID := c.FindPartitionID(ball)
	c.balls[partID] = ball
	c.mu.Unlock()
	return c.GetPartitionOwner(partID)
}

// MaximumLoad exposes the current average load.
func (c *Consistent) MaximumLoad() float64 {
	load := float64(float64(c.partition)/float64(len(c.bins))) * c.loadBalancingParameter
	return math.Ceil(load)
}

// relocate redistributes the balls to the current existing bins
func (c *Consistent) relocate() {
	newBalls := map[PartitionID][]Ball{}
	for _, ball := range c.balls {
		partID := c.FindPartitionID(ball)
		if len(newBalls[partID]) == 0 {
			newBalls[partID] = []Ball{ball}
			continue
		}

		newBalls[partID] = append(newBalls[partID], ball)
	}
}

// Remove removes a bin from the consistent hash ring.
func (c *Consistent) Remove(bin Bin) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.bins[bin.String()]; !ok {
		// skip if the bin does not exist
		return nil
	}

	for i := 0; i < c.replicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", bin.String(), i))
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
	delete(c.bins, bin.String())
	if len(c.bins) == 0 {
		// consistent hash ring is empty now. Reset the partition table.
		c.partitions = make(map[PartitionID]*Bin)
		return nil
	}
	return c.distributePartitions()
}
