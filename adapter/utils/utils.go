package utils

import (
	"reflect"

	"github.com/danish-a1/goqueue/job"
	"github.com/google/uuid"
)

// genearte JOB Id for jobs pushed
func GenerateID() string {
	return uuid.New().String()
}

// getJobName returns the concrete type name of a job
func GetJobName(jb job.Job) string {
	t := reflect.TypeOf(jb)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}
