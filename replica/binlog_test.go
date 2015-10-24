package replica

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

type fileStat struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	sys     interface{}
}

func (fs *fileStat) Name() string       { return fs.name }
func (fs *fileStat) Size() int64        { return fs.size }
func (fs *fileStat) Mode() os.FileMode  { return fs.mode }
func (fs *fileStat) ModTime() time.Time { return fs.modTime }
func (fs *fileStat) IsDir() bool        { return false }
func (fs *fileStat) Sys() interface{}   { return fs.sys }

func TestScanDirectory(t *testing.T) {
	findLogFiles("/etc")
}

func TestFileFilter(t *testing.T) {
	v := make([]os.FileInfo, 0)
	var i int64
	for i = 0; i < 100; i++ {
		n := BinaryLogNamePrefix + strconv.Itoa(rand.Intn(1000000000)) + BinaryLogFileExt
		v = append(v, &fileStat{n, i + 100, 11, time.Now(), 0})
	}
	d := extractLogFiles(v)
	if len(d) != 100 {
		t.Error("Number of elements doesn't match 100!")
	}

	fval := d[0].Num

	for i := 1; i < len(d); i++ {
		if d[i].Num < fval {
			t.Error("Data is not sorted well!")
			break
		}
		fval = d[i].Num
	}
}
