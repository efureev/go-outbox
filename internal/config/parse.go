package config

import (
	"strconv"
	"time"
)

// Small parsers shared by the driver builders. They exist because driver
// settings are looked up by name at run time and so cannot go through the
// struct binding that handles the rest of the configuration.

func intOrDefault(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}

	return strconv.Atoi(s)
}

func boolOrDefault(s string, def bool) (bool, error) {
	if s == "" {
		return def, nil
	}

	return strconv.ParseBool(s)
}

func durationOrDefault(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}

	return time.ParseDuration(s)
}
