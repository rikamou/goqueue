package goqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeValScanAcceptsTimeTime(t *testing.T) {
	var tt time.Time
	v := timeVal{t: &tt}

	in := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	require.NoError(t, v.Scan(in))
	assert.Equal(t, in, tt)
}

func TestTimeValScanParsesBytesWithAndWithoutMicros(t *testing.T) {
	cases := []struct {
		in   []byte
		want time.Time
	}{
		{[]byte("2020-01-02 03:04:05"), time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)},
		{[]byte("2020-01-02 03:04:05.000000"), time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)},
	}

	for _, c := range cases {
		var tt time.Time
		v := timeVal{t: &tt}
		require.NoError(t, v.Scan(c.in))
		assert.Equal(t, c.want, tt)
	}
}

func TestTimeValScanRejectsNil(t *testing.T) {
	var tt time.Time
	v := timeVal{t: &tt}

	err := v.Scan(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected NULL")
}

func TestTimeValScanRejectsInvalidBytes(t *testing.T) {
	var tt time.Time
	v := timeVal{t: &tt}

	err := v.Scan([]byte("not-a-time"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot parse")
}

func TestNullTimeValScanAcceptsNil(t *testing.T) {
	var tt *time.Time
	v := nullTimeVal{t: &tt}

	require.NoError(t, v.Scan(nil))
	assert.Nil(t, tt)
}

func TestNullTimeValScanParsesNonNil(t *testing.T) {
	var out *time.Time
	v := nullTimeVal{t: &out}

	require.NoError(t, v.Scan("2020-01-02 03:04:05"))
	require.NotNil(t, out)
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), *out)
}
