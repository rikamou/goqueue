package goqueue

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestTruncateErrMsgLeavesShortStringsAlone(t *testing.T) {
	assert.Equal(t, "hi", truncateErrMsg("hi"))
}

func TestTruncateErrMsgTruncatesAtRuneBoundary(t *testing.T) {
	// Force a multi-byte rune at the boundary.
	// "€" is 3 bytes; ensure the slice would otherwise cut mid-rune.
	prefix := strings.Repeat("a", maxErrMsgLen-1)
	in := prefix + "€" + strings.Repeat("b", 10)

	out := truncateErrMsg(in)
	assert.True(t, utf8.ValidString(out))
	assert.Equal(t, prefix+"...(truncated)", out)
}
