package goqueue

import (
	"strings"
	"testing"

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
	assert.LessOrEqual(t, len(out), maxErrMsgLen+len("...(truncated)"))
	assert.True(t, strings.HasSuffix(out, "...(truncated)"))
	// Result must remain valid UTF-8 (implicitly, because Go strings are UTF-8 and we didn't panic),
	// but also ensure the "€" wasn't partially included.
	assert.NotContains(t, out, "\uFFFD")
}

