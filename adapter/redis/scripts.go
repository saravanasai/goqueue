package redis

import _ "embed"

// Embedded Lua scripts for Redis operations.
//
//go:embed scripts/move_retry.lua
var moveRetryJobScript string

//go:embed scripts/cleanup_processing.lua
var cleanupProcessingJobScript string
