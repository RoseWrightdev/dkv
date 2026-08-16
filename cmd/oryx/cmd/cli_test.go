package cmd

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExitError_Formatting(t *testing.T) {
	err1 := &exitError{code: 1}
	assert.Equal(t, "exit code 1", err1.Error())
	assert.Nil(t, err1.Unwrap())

	err2 := &exitError{code: 2, err: errors.New("connection failed")}
	assert.Equal(t, "connection failed", err2.Error())
	assert.Equal(t, errors.New("connection failed"), err2.Unwrap())
}
