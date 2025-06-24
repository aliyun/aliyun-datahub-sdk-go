package datahub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateHashCode(t *testing.T) {
	res, err := calculateHashCode("aaa")
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(876991330))

	res, err = calculateHashCode("test")
	assert.Nil(t, err)
	assert.Equal(t, res, uint32(2949673445))
}

func TestCalculateMD5(t *testing.T) {
	res, err := calculateMD5("aaa")
	assert.Nil(t, err)
	assert.Equal(t, res, "47bce5c74f589f4867dbd57e9ca9f808")

	res, err = calculateMD5("test")
	assert.Nil(t, err)
	assert.Equal(t, res, "098f6bcd4621d373cade4e832627b4f6")
}
