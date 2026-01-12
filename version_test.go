package goqueue

import (
	"regexp"
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

	// Version should follow semantic versioning pattern like "0.2.4.1"
	// This regex checks for three dot-separated numbers.
	
	re := regexp.MustCompile(`^\d+\.\d+\.\d+(?:\.\d+)?$`)
	if !re.MatchString(Version) {
		t.Errorf("Version constant '%s' does not match semantic versioning pattern 'X.Y.Z'", Version)
	}
}
