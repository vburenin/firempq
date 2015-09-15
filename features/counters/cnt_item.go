package counters

import (
	"encoding/binary"
	"firempq/common"
	"math"
	"strconv"
)

type Counter interface {
	common.IItemMetaData
	Update()
	GetValueAsString() string
}

type CounterDataType byte

const (
	CNT_INT_COUNTER          CounterDataType = 1
	CNT_FLOAT_COUNTER        CounterDataType = 2
	CNT_STATIC_INT_COUNTER   CounterDataType = 3
	CNT_STATIC_FLOAT_COUNTER CounterDataType = 4
)

type IntLinearCounter struct {
	Id               string
	Value            int64
	MinValue         int64
	MaxValue         int64
	CountRate        float64
	LastUpdateNanoTs int64
	InactiveTTL      int64
	CounterType      CounterDataType
}

func NewIntLinearCounter(
	id string, value int64, minVal int64, maxVal int64,
	cntRate float64, inactiveTTL int64) *IntLinearCounter {

	pqm := IntLinearCounter{
		Id:               id,
		Value:            value,
		MinValue:         minVal,
		MaxValue:         maxVal,
		CountRate:        cntRate,
		LastUpdateNanoTs: common.UnixNanoTs(),
		InactiveTTL:      inactiveTTL,
		CounterType:      CNT_INT_COUNTER,
	}
	return &pqm
}

func (c *IntLinearCounter) GetId() string {
	return c.Id
}

func (c *IntLinearCounter) GetValueAsString() string {
	return strconv.FormatInt(c.Value, 10)
}

func (c *IntLinearCounter) Update() {
	curTs := common.UnixNanoTs()
	tsDelta := float64(curTs-c.LastUpdateNanoTs) / 1000000000.0

	// If clocks moved back, just do nothing. Lets just remember the last time stamp.
	if tsDelta < 0 {
		c.LastUpdateNanoTs = curTs
		return
	}

	valDelta := c.CountRate * tsDelta
	if valDelta >= 1 || valDelta <= -1 {
		c.Value += int64(valDelta)
		leftover := valDelta - math.Trunc(valDelta)
		// Overflow will decrease a current TS to take into account overflow value.
		c.LastUpdateNanoTs = curTs - int64(math.Abs(tsDelta*leftover))
	}
	c.correctLimits()
}

func (c *IntLinearCounter) correctLimits() {
	if c.Value < c.MinValue {
		c.Value = c.MinValue
	} else if c.Value > c.MaxValue {
		c.Value = c.MaxValue
	}
}

func (c *IntLinearCounter) Marshal() ([]byte, error) {
	buf := make([]byte, 6*8+1)
	buf[0] = byte(c.CounterType)

	offset := 1
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.Value))

	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.MinValue))

	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.MaxValue))

	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], math.Float64bits(c.CountRate))

	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.LastUpdateNanoTs))

	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(c.InactiveTTL))

	return buf, nil
}

func IntLinearCountersFromBytes(itemId string, data []byte) *IntLinearCounter {
	offset := 1
	value := int64(binary.BigEndian.Uint64(data[offset:]))

	offset += 8
	minVal := int64(binary.BigEndian.Uint64(data[offset:]))

	offset += 8
	maxVal := int64(binary.BigEndian.Uint64(data[offset:]))

	offset += 8
	cntRate := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))

	offset += 8
	updateTs := int64(binary.BigEndian.Uint64(data[offset:]))

	offset += 8
	inactiveTTL := int64(binary.BigEndian.Uint64(data[offset:]))

	pqm := IntLinearCounter{
		Id:               itemId,
		Value:            value,
		MinValue:         minVal,
		MaxValue:         maxVal,
		CountRate:        cntRate,
		LastUpdateNanoTs: updateTs,
		InactiveTTL:      inactiveTTL,
		CounterType:      CNT_INT_COUNTER,
	}
	return &pqm

}

var _ Counter = &IntLinearCounter{}
