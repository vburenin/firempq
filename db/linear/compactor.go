package linear

import (
	"github.com/vburenin/firempq/fctx"
	"go.uber.org/zap"
	"bufio"
	"os"
	"github.com/vburenin/firempq/ferr"
	"path/filepath"
)

type Compactor struct {
	dbPath string
	writeBuf *bufio.Writer
	f *os.File
}

const tempCompactorName = ".temp_compactor.db"
const compactedDb = "compacted.db"
const compactedPrev = "compacted.db.prev"

func CompactDatabase(ctx *fctx.Context, dbPath string) ( *Compactor, error) {
	ctx.Info("compaction database", zap.String("path", dbPath))

	f, err := os.Create(filepath.Join(dbPath, tempCompactorName))
	if err != nil {
		return nil, ferr.Wrapf(err, "could not initialize file for compaction")
	}
	return &Compactor{
		f: f,
		writeBuf: bufio.NewWriterSize(f, 1024*1024),
		dbPath:dbPath,
	}, nil

}

func (c *Compactor) Write(data []byte) error {
	l := len(data)
	c.writeBuf.WriteByte(byte(l >> 8))
	c.writeBuf.WriteByte(byte(l))
	_, err := c.writeBuf.Write(data)
	return err
}

func (c *Compactor) Complete() error {
	if err := c.writeBuf.Flush(); err != nil {
		return ferr.Wrapf(err, "could not flush compacted database")
	}

	if err := c.f.Close(); err != nil {
		return ferr.Wrapf(err, "failed to close compacted database")
	}

	compacted := filepath.Join(c.dbPath, compactedDb)
	compactedPrev := filepath.Join(c.dbPath, compactedPrev)
	compactedTemp := filepath.Join(c.dbPath, tempCompactorName)

	err := os.Rename(compacted, compactedPrev)

	if err != nil && !os.IsNotExist(err) {
		return ferr.Wrapf(err, "could not move current compacted db")
	}
	err = os.Rename(compactedTemp, compactedDb)
	if err != nil {
		return ferr.Wrapf(err, "could not rename temporary compacted db to the permanent one")
	}
	err = os.Remove(compactedPrev)
	if err != nil {
		return ferr.Wrapf(err, "could not delete compacted prev")
	}

	return nil
}


func RecoverDBState(dbPath string) error {
	compacted := filepath.Join(dbPath, compactedDb)
	compactedPrev := filepath.Join(dbPath, compactedPrev)
	compactedTemp := filepath.Join(dbPath, tempCompactorName)
	err := os.Remove(compactedTemp)

	if err !=nil && !os.IsNotExist(err) {
		return ferr.Wrapf(err, "can't delete temporary compacted db")
	}

	s, err := os.Stat(compacted)
	if err == nil {
		if s.IsDir() {
			return ferr.Errorf("database file is a directory: %s", compacted)
		}
		err = os.Remove(compactedPrev)
		if err != nil && !os.IsNotExist(err) {
			return ferr.Errorf("database previous copy file is a directory: %s", compactedPrev)
		}
		return nil
	}

	s, err = os.Stat(compactedPrev)
	if err == nil {
		if s.IsDir() {
			return ferr.Errorf("database file is a directory: %s", compactedPrev)
		}
		err = os.Rename(compactedPrev, compacted)
		if err != nil {
			return ferr.Wrapf(err, "could not revert db copy: %s", compactedPrev)
		}
		return nil
	}

	if !os.IsNotExist(err) {
		return ferr.Wrapf(err, "could not get info about the copy of compacted db: %s", compactedPrev)
	}

	return nil
}