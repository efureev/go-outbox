package app

import "errors"

// errNotStarted is what a health check reports before its module has opened
// whatever it probes.
var errNotStarted = errors.New("module has not started")
