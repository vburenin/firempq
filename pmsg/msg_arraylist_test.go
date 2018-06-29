package pmsg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMessageArrayList(t *testing.T) {
	l := NewMessageArrayList(3)
	l.Add(&MsgMeta{Serial: 1})
	l.Add(&MsgMeta{Serial: 2})
	l.Add(&MsgMeta{Serial: 3})
	l.Add(&MsgMeta{Serial: 4})
	l.Add(&MsgMeta{Serial: 5})

	assert.Equal(t, 2, len(l.curList))
	assert.Equal(t, 3, len(l.lists[0]))
	assert.Equal(t, uint64(5), l.size)
	assert.Equal(t, 0, l.pos)
	assert.Equal(t, 1, len(l.lists))

	assert.Equal(t, uint64(1), l.Pop().Serial)
	assert.Equal(t, 1, l.pos)
	assert.Equal(t, uint64(2), l.Pop().Serial)
	assert.Equal(t, 2, l.pos)

	assert.Equal(t, uint64(3), l.Pop().Serial)
	assert.Equal(t, 0, len(l.lists))
	assert.Equal(t, 0, l.pos)
	assert.Equal(t, uint64(4), l.Pop().Serial)
	assert.Equal(t, 1, l.pos)
	assert.Equal(t, uint64(5), l.Pop().Serial)
	assert.Equal(t, 2, l.pos)

	assert.Nil(t, l.Pop())
}

func TestNewMessageArrayListAddPopSeq(t *testing.T) {
	l := NewMessageArrayList(3)
	l.Add(&MsgMeta{Serial: 1})
	assert.Equal(t, uint64(1), l.Pop().Serial)

	l.Add(&MsgMeta{Serial: 2})
	assert.Equal(t, uint64(2), l.Pop().Serial)

	l.Add(&MsgMeta{Serial: 3})
	assert.Equal(t, 1, len(l.lists))

	assert.Equal(t, uint64(3), l.Pop().Serial)

	l.Add(&MsgMeta{Serial: 4})
	assert.Equal(t, uint64(4), l.Pop().Serial)

	l.Add(&MsgMeta{Serial: 5})
	assert.Equal(t, uint64(5), l.Pop().Serial)

	assert.Nil(t, l.Pop())

}
