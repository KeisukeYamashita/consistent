package consistent

// Ball represents data to be placed on the ring.
// In most cases, it is a subsequent server for load balancing, data for sharding, etc but
// it can be anything unless it implements this interface.
type Ball interface {
	// String returns the name of the ball.
	String() string
}
