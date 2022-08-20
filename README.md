# consistent

> Go written consistent hashing package inspired by Google's "Consistent Hashing with Bounded Loads"

If you don't know the consistent hashing, please refer to [Introducing Consistent Hashing, Teiva Harsanyi](https://itnext.io/introducing-consistent-hashing-9a289769052e) to learn about it!

## References

- [Consistent Hashing with Bounded Loads on Google Research Blog](https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)
- [Improving load balancing with a new consistent-hashing algorithm on Vimeo Engineering Blog](https://medium.com/vimeo-engineering-blog/improving-load-balancing-with-a-new-consistent-hashing-algorithm-9f1bd75709ed)
- [Consistent Hashing with Bounded Loads paper on arXiv](https://arxiv.org/abs/1608.01350)

## Install

Please run to install the package:

```console
$ go get -u github.com/KeisukeYamashita/consistent
```

## Configuration

```go
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
```

## Usage

<!-- TODO -->

## Contributions

All contributions are welcome, please file a issue or a pull request 🚀

## License

[Apache License 2.0](./LICENSE).

## Acknowledgements

This package was inspired heavily by these packages:

- [buraksezer/consistent](https://github.com/buraksezer/consistent)

Thank you very much for open sourcing these hard work.
