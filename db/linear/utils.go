package linear

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"

	"github.com/vburenin/firempq/ferr"
)

func MakePayloadFileName(num int64) string {
	return fmt.Sprintf("%s-%d.%s", PayloadFilePrefix, num, DBFileExt)
}

func MakeMetaFileName(num int64) string {
	return fmt.Sprintf("%s-%d.%s", MetaFilePrefix, num, DBFileExt)
}

func RetrieveAvailableMetaFileIDs(dbPath string) ([]int64, error) {
	files, err := ioutil.ReadDir(dbPath)
	if err != nil {
		return nil, ferr.Wrapf(err, "could not get database content")
	}
	return sortMetaFileIds(files), nil
}

func findLatestIds(files []os.FileInfo) (metaID, payloadID int64) {
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		data := NameMatchRE.FindAllStringSubmatch(f.Name(), -1)
		if len(data) > 0 && len(data[0]) == 3 {
			db := data[0][1]
			seq, err := strconv.ParseInt(data[0][2], 10, 64)
			if err != nil {
				continue
			}
			if db == PayloadFilePrefix {
				if seq > payloadID {
					payloadID = seq
				}
			} else if db == MetaFilePrefix {
				if seq > metaID {
					metaID = seq
				}
			}
		}

	}
	return metaID, payloadID
}

func sortMetaFileIds(files []os.FileInfo) []int64 {
	var metafileIDs []int64
	for _, f := range files {
		if f.IsDir() || f.Size() == 0 {
			continue
		}

		data := NameMatchRE.FindAllStringSubmatch(f.Name(), -1)
		if len(data) > 0 && len(data[0]) == 3 {
			db := data[0][1]
			seq, err := strconv.ParseInt(data[0][2], 10, 64)
			if err != nil {
				continue
			}
			if db == MetaFilePrefix {
				metafileIDs = append(metafileIDs, seq)
			}
		}
	}
	sort.Slice(metafileIDs, func(i, j int) bool {
		return metafileIDs[i] < metafileIDs[j]
	})
	return metafileIDs
}
