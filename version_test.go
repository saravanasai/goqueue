package goqueue

import (
	"testing"
)

func TestVersion(t *testing.T) {
	version := GetVersion()
	if version == "" {
		t.Error("Version should not be empty")
	}

	if version != Version {
		t.Errorf("GetVersion() = %s, want %s", version, Version)
	}

	// Version should follow semantic versioning pattern
	if len(version) < 5 { // At least "0.0.1"
		t.Errorf("Version %s appears to be too short", version)
	}
}

func TestVersionConstant(t *testing.T) {
	if Version == "" {
		t.Error("Version constant should not be empty")
	}

	// Should start with 0.0.1 for initial release
	if Version != "0.0.2" {
		t.Logf("Version is %s (this test will need updating for new versions)", Version)
	}
}
