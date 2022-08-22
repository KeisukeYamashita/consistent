package consistent

import (
	"errors"
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	binPrefix  = "node"
	ballPrefix = "data"
)

func newConfig() *Config {
	return &Config{
		Partition:              23,
		ReplicationFactor:      21,
		LoadBalancingParameter: 1.1,
		Hasher:                 hasher{},
	}
}

func new(t *testing.T, cfg *Config) *Consistent {
	t.Helper()

	c, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create consistent: %v", err)
	}

	return c
}

type hasher struct{}

func (hs hasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func initialBalls(cnt int) []Ball {
	balls := make([]Ball, cnt)
	for i := 0; i < cnt; i++ {
		balls[i] = Ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i)))
	}
	return balls
}

func initialBins(cnt int) []Bin {
	bins := make([]Bin, cnt)
	for i := 0; i < cnt; i++ {
		bins[i] = NewBin(fmt.Sprintf("%s%d", binPrefix, i))
	}
	return bins
}

func TestConsistent_Add(t *testing.T) {
	type testcase struct {
		bins     []Bin
		expected int
	}

	tcs := map[string]testcase{
		"bin should be added to the ring": {
			bins:     initialBins(4),
			expected: 4,
		},
		"duplicated bin addition should be ignore": {
			bins:     append(initialBins(4), NewBin(fmt.Sprintf("%s0", binPrefix))),
			expected: 4,
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range tc.bins {
				c.Add(bin)
			}

			bins := c.GetBins()
			if len(bins) != tc.expected {
				t.Fatalf("number of bins mismatch, got:%d, want:%d", len(bins), tc.expected)
			}
		})
	}
}

func TestConsistent_Delete(t *testing.T) {
	type testcase struct {
		ball     Ball
		expected int
		relocate bool
		want     error
	}

	balls := initialBalls(4)

	tcs := map[string]testcase{
		"ball should be deleted": {
			ball:     balls[0],
			expected: 3,
		},
		"fail on non existing ball": {
			ball: Ball([]byte("not exist")),
			want: ErrBallNotFound,
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range initialBins(4) {
				if err := c.Add(bin); err != nil {
					t.Fatalf("failed to add bin: %v", err)
				}
			}

			for _, ball := range balls {
				c.Locate(ball)
			}

			oldBalls := map[PartitionID]Ball{}
			for partID, ball := range c.balls {
				oldBalls[partID] = ball
			}

			if err := c.Delete(tc.ball); err != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("error unexpected: got:%v want:%v", err, tc.want)
				}

				return
			}

			if tc.want != nil {
				t.Fatalf("should fail got:%v", tc.want)
			}

			if len(c.balls) != tc.expected {
				t.Fatalf("unexpected result: got:%d want:%d", len(c.balls), tc.expected)
			}
		})
	}
}

func TestConsistent_GetBalls(t *testing.T) {
	type testcase struct {
		bins []Bin
		bin  Bin
		want error
	}

	bins := initialBins(6)

	tcs := map[string]testcase{
		"return bin's balls": {
			bins: bins,
			bin:  bins[0],
		},
		"return error if bin not exist": {
			bins: bins,
			bin:  NewBin("not exist"),
			want: ErrBinNotFound,
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range tc.bins {
				if err := c.Add(bin); err != nil {
					t.Fatalf("error bin (name: %s) add: %v", bin.String(), err)
				}
			}

			_, err := c.GetBalls(tc.bin)
			if err != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("error not expected, got:%v want:%v", err, tc.want)
				}

				return
			}
		})
	}
}

func TestConsistent_GetBin(t *testing.T) {
	type testcase struct {
		bins []Bin
		bin  Bin
		want error
	}

	bins := initialBins(4)

	tcs := map[string]testcase{
		"bin exist in the bins": {
			bins: bins,
			bin:  bins[0],
		},
		"bin not exist": {
			bins: bins,
			bin:  NewBin("not exist"),
			want: ErrBinNotFound,
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range tc.bins {
				if err := c.Add(bin); err != nil {
					t.Fatalf("error bin add: %v", err)
				}
			}

			got, err := c.GetBin(tc.bin.String())
			if err != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("error not expected, got:%v want:%v", err, tc.want)
				}

				return
			}

			if got.String() != tc.bin.String() {
				t.Fatalf("mismatch, got:%v, want:%v", got.String(), tc.bin.String())
			}
		})
	}
}

func TestConsistent_GetBins(t *testing.T) {
	type testcase struct {
		bins []Bin
	}

	tcs := map[string]testcase{
		"bins should be added": {
			bins: initialBins(4),
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range tc.bins {
				if err := c.Add(bin); err != nil {
					t.Fatalf("error bin add: %v", err)
				}
			}

			got := c.GetBins()
			opts := []cmp.Option{
				cmpopts.SortSlices(func(i, j Bin) bool {
					return i.String() > j.String()
				}),
			}
			if diff := cmp.Diff(got, tc.bins, opts...); diff != "" {
				t.Fatalf("mismatch request(-got,+want):%s\n", diff)
			}
		})
	}
}

func TestConsistent_MaximumLoad(t *testing.T) {
	type testcase struct {
		bins       []Bin
		partitions int
		expected   float64
	}

	tcs := map[string]testcase{
		"2 bins with 4 partitions": {
			bins:       initialBins(2),
			partitions: 4,
			expected:   2,
		},
		"4 bins with 2 partitions": {
			bins:       initialBins(4),
			partitions: 2,
			expected:   1,
		},
		"4 bins with 4 partitions": {
			bins:       initialBins(12),
			partitions: 4,
			expected:   1,
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			t.Parallel()

			c := new(t, &Config{
				Hasher:                 hasher{},
				Partition:              tc.partitions,
				ReplicationFactor:      10,
				LoadBalancingParameter: 1,
			})
			for _, bin := range tc.bins {
				if err := c.Add(bin); err != nil {
					t.Fatalf("error bin add: %v", err)
				}
			}

			load := c.MaximumLoad()
			if load != tc.expected {
				t.Fatalf("mismatch, got:%f, want:%f", load, tc.expected)
			}
		})
	}
}

func TestConsistent_Relocate(t *testing.T) {
	type testcase struct {
		balls   []Ball
		bins    []Bin
		f       func(*Consistent) error
		hasDiff bool
	}

	tcs := map[string]testcase{
		"simple relocate": {
			balls: initialBalls(100),
			bins:  initialBins(4),
			f: func(c *Consistent) error {
				c.relocate()
				return nil
			},
			hasDiff: false,
		},
		"add ball": {
			balls: initialBalls(100),
			bins:  initialBins(4),
			f: func(c *Consistent) error {
				balls := initialBalls(3)
				for _, ball := range balls {
					c.Locate(ball)
				}
				return nil
			},
			hasDiff: true,
		},
		"delete ball": {
			balls: initialBalls(100),
			bins:  initialBins(4),
			f: func(c *Consistent) error {
				balls := initialBalls(3)
				for _, ball := range balls {
					c.Locate(ball)
				}
				return nil
			},
			hasDiff: true,
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range tc.bins {
				if err := c.Add(bin); err != nil {
					t.Fatalf("failed to add: %v", err)
				}
			}

			for _, ball := range tc.balls {
				c.Locate(ball)
			}

			oldBalls := map[PartitionID]Ball{}
			for partID, ball := range c.balls {
				oldBalls[partID] = ball
			}

			if err := tc.f(c); err != nil {
				t.Fatalf("failed to run setup: %v", err)
			}

			newBalls := c.balls
			if diff := cmp.Diff(oldBalls, newBalls); diff != "" {
				if !tc.hasDiff {
					t.Fatalf("should not have diff, got(-got,+want): %s", diff)
				}

				return
			}

			if tc.hasDiff {
				t.Fatalf("should have diff")
			}
		})
	}
}

func TestConsistent_Remove(t *testing.T) {
	type testcase struct {
		bins         []Bin
		expected     int
		removingBins []Bin
	}

	tcs := map[string]testcase{
		"bins should be removed": {
			expected:     2,
			bins:         initialBins(4),
			removingBins: initialBins(2),
		},
		"not existing bin removal should not be affected": {
			expected: 4,
			bins:     initialBins(4),
			removingBins: []Bin{
				NewBin("fake"),
			},
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range tc.bins {
				c.Add(bin)
			}

			for _, bin := range tc.removingBins {
				c.Remove(bin)
			}

			bins := c.GetBins()
			if len(bins) != tc.expected {
				t.Fatalf("number of bins mismatch, got:%d, want:%d", len(bins), tc.expected)
			}
		})
	}
}
