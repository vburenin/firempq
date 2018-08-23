package linear

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vburenin/firempq/fctx"
	"github.com/vburenin/firempq/log"
)

func TestOpenedFileOps(t *testing.T) {
	filename := "testdata/pay_test.dat"
	if _, err := os.Stat(filename); err == nil {
		os.Remove(filename)
	}
	defer os.Remove(filename)

	val1 := []byte("test1 data to write ---")
	val2 := []byte("test2 data to write ------")
	val3 := []byte("test3 data to write -----------")
	val4 := []byte("anotherdata")

	of, err := NewOpenedFile(filename, false)
	assert.Nil(t, err, "Should be nil")

	p1, err := of.Write(val1) // 23 + 8 bytes
	assert.Nil(t, err, "Should be nil")

	p2, err := of.Write(val2) // 26 + 8 bytes
	assert.Nil(t, err, "Should be nil")

	p3, err := of.Write(val3) // 31 + 8 bytes

	assert.Nil(t, of.Flush())

	assert.Equal(t, int64(0), p1)
	assert.Equal(t, int64(31), p2)
	assert.Equal(t, int64(31+34), p3)

	midata, err := of.RetrieveData(p2)
	assert.Nil(t, err, "Should be nil")
	assert.Equal(t, val2, midata)

	p4, err := of.Write(val4) // 31 + 8 bytes
	assert.Nil(t, of.Flush())
	assert.Nil(t, err, "Should be nil")

	d1, err := of.RetrieveData(p1)
	assert.Nil(t, err, "Should be nil")
	d2, err := of.RetrieveData(p2)
	assert.Nil(t, err, "Should be nil")
	d3, err := of.RetrieveData(p3)
	assert.Nil(t, err, "Should be nil")
	d4, err := of.RetrieveData(p4)
	assert.Nil(t, err, "Should be nil")

	assert.Equal(t, val1, d1)
	assert.Equal(t, val2, d2)
	assert.Equal(t, val3, d3)
	assert.Equal(t, val4, d4)

	of.Close()

}

func removeDbFiles(path string) {
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if f.Name() != "readme.txt" && !f.IsDir() {
			os.Remove(filepath.Join(path, f.Name()))
		}
	}
}

func TestFlatStorageWriteAndRetrieveMetadata(t *testing.T) {
	log.InitLogging()
	a := assert.New(t)

	testDir := "testdata/queue_data"
	removeDbFiles(testDir)
	defer removeDbFiles(testDir)

	v, err := NewFlatStorage(testDir, 300, 1024*10)

	if !a.Nil(err, "init error") {
		return
	}

	records := 10000

	for i := 0; i < records; i++ {
		if !a.Nil(v.AddMetadata([]byte(fmt.Sprintf("test data %d", i)))) {
			return
		}
	}

	a.Nil(v.Close())

	ctx := fctx.Background("unit-test")

	iterator, err := NewIterator(ctx, testDir)
	if !a.Nil(err, "init error") {
		return
	}

	i := 0
	for iterator.Next() != io.EOF {
		if iterator.valid {
			data := iterator.GetData()
			if !a.Equal(fmt.Sprintf("test data %d", i), string(data)) {
				break
			}
			i++
		}

	}
	a.Equal(records, i)
}
