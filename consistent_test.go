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

type ball []byte

func (b ball) String() string {
	return string(b)
}

func initialBalls(cnt int) []Ball {
	balls := make([]Ball, cnt)
	for i := 0; i < cnt; i++ {
		balls[i] = ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i)))
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

func TestNew(t *testing.T) {
	type testcase struct {
		cfg  *Config
		pass bool
	}

	tcs := map[string]testcase{
		"ok": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              100,
				ReplicationFactor:      10,
				LoadBalancingParameter: 1.20,
			},
			pass: true,
		},
		"0 partition": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              0,
				ReplicationFactor:      10,
				LoadBalancingParameter: 1.20,
			},
			pass: false,
		},
		"no partition": {
			cfg: &Config{
				Hasher:                 hasher{},
				ReplicationFactor:      10,
				LoadBalancingParameter: 1.20,
			},
			pass: false,
		},
		"negative replication factor": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              20,
				ReplicationFactor:      -1,
				LoadBalancingParameter: 1.20,
			},
			pass: false,
		},
		"zero replication factor": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              20,
				ReplicationFactor:      -1,
				LoadBalancingParameter: 1.20,
			},
			pass: false,
		},
		"no replication factor": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              20,
				LoadBalancingParameter: 1.20,
			},
			pass: false,
		},
		"negative load balancing parameter": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              20,
				ReplicationFactor:      10,
				LoadBalancingParameter: -1,
			},
			pass: false,
		},
		"0 load balancing parameter": {
			cfg: &Config{
				Hasher:                 hasher{},
				Partition:              20,
				ReplicationFactor:      10,
				LoadBalancingParameter: 0,
			},
			pass: false,
		},
		"no load balancing parameter": {
			cfg: &Config{
				Hasher:            hasher{},
				Partition:         20,
				ReplicationFactor: 10,
			},
			pass: false,
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			_, err := New(tc.cfg, nil)
			if err != nil {
				if tc.pass {
					t.Fatalf("should fail: %v", err)
				}

				return
			}

			if !tc.pass {
				t.Fatal("should not pass")
			}
		})
	}

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
		want     error
	}

	balls := initialBalls(4)

	tcs := map[string]testcase{
		"ball should be deleted": {
			ball:     balls[0],
			expected: 3,
		},
		"fail on non existing ball": {
			ball: ball([]byte("not exist")),
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

			oldBalls := map[PartitionID][]Ball{}
			for partID, balls := range c.balls {
				oldBalls[partID] = append(oldBalls[partID], balls...)
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

			if len(c.GetBalls()) != tc.expected {
				t.Fatalf("unexpected result: got:%d want:%d", len(c.GetBalls()), tc.expected)
			}
		})
	}
}

func TestConsistent_GetBalls(t *testing.T) {
	type testcase struct {
		balls []Ball
	}

	tcs := map[string]testcase{
		"add balls": {
			balls: initialBalls(4),
		},
	}

	cfg := newConfig()
	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			tc := tc
			t.Parallel()

			c := new(t, cfg)
			for _, bin := range initialBins(6) {
				if err := c.Add(bin); err != nil {
					t.Fatalf("error bin (name: %s) add: %v", bin.String(), err)
				}
			}

			for _, ball := range tc.balls {
				_ = c.Locate(ball)
			}

			if cnt := len(c.GetBalls()); cnt != len(tc.balls) {
				t.Fatalf("ball count mismatch, got:%d want:%d", cnt, len(tc.balls))
			}
		})
	}
}

func TestConsistent_GetBallsByBin(t *testing.T) {
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

			_, err := c.GetBallsByBin(tc.bin)
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
		partitions uint64
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

			oldBalls := map[PartitionID][]Ball{}
			for partID, balls := range c.balls {
				oldBalls[partID] = append(oldBalls[partID], balls...)
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

func BenchmarkConsistent_FindPartitionID(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FindPartitionID(ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i))))
	}
}

func BenchmarkConsistent_LoadDistribution(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	for i := 0; i < 100; i++ {
		c.Locate(ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.LoadDistribution()
	}
}

func BenchmarkConsistent_Locate(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Locate(ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i))))
	}
}

func BenchmarkConsistent_GetBalls(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	for i := 0; i < 100; i++ {
		c.Locate(ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetBalls()
	}
}

func BenchmarkConsistent_GetBallsByBin(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	bins := initialBins(100)
	for _, bin := range bins {
		c.Add(bin)
	}

	for i := 0; i < 100; i++ {
		c.Locate(ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.GetBallsByBin(bins[0])
	}
}

func BenchmarkConsistent_GetBin(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	bins := initialBins(100)
	for _, bin := range bins {
		c.Add(bin)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.GetBin(bins[0].String())
	}
}

func BenchmarkConsistent_GetBins(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetBins()
	}
}

func BenchmarkConsistent_GetPartitionOwner(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	for i := 0; i < 100; i++ {
		c.Locate(ball([]byte(ballPrefix)))
	}

	b.ResetTimer()
	partID := c.FindPartitionID(ball([]byte(ballPrefix)))
	for i := 0; i < b.N; i++ {
		c.GetPartitionOwner(partID)
	}
}

func BenchmarkConsistent_MaximumLoad(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.MaximumLoad()
	}
}

func BenchmarkConsistent_Relocate(b *testing.B) {
	cfg := newConfig()
	c, err := New(cfg, nil)
	if err != nil {
		b.Errorf("failed: %v", err)
	}

	for _, bin := range initialBins(100) {
		c.Add(bin)
	}

	for i := 0; i < 100; i++ {
		c.Locate(ball([]byte(fmt.Sprintf("%s%d", ballPrefix, i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.relocate()
	}
}
