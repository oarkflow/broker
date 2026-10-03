package broker

import "os"

// BROKER_TEST_COMPACT=1 runs the whole suite with compact backlog storage as the
// default, so every existing test doubles as a compact-storage regression test.
func init() {
	if os.Getenv("BROKER_TEST_COMPACT") == "1" {
		compactBacklogDefault = true
	}
}
