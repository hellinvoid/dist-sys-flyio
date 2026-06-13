package snowflake

import (
	"log"
	"strconv"
	"sync"
	"time"
)

/*
	<no_need>
	[1]			[41]			[10]		[12]
	<sign>		<timestamp>		<node_id>	<counter>
*/

var (
	start = time.Date(2005, time.January, 18, 0, 0, 0, 0, time.Local)

	nodeIdBits  uint64 = 10
	counterBits uint64 = 12

	timestampShiftBits = nodeIdBits + counterBits
)

type IdGenerator struct {
	idCh chan uint64

	currTimestamp uint64
	nodeId        uint64
	counter       uint64

	once sync.Once
}

func NewIdGenerator() *IdGenerator {
	return &IdGenerator{
		idCh: make(chan uint64),
		once: sync.Once{},
	}
}

func (idgen *IdGenerator) generate() {
	var currId uint64
	for {
		currId = 0
		currTimestamp := uint64(time.Since(start))

		if currTimestamp == idgen.currTimestamp {
			idgen.counter++
		} else {
			idgen.currTimestamp = currTimestamp
			idgen.counter = 0
		}

		currId += currTimestamp << timestampShiftBits
		currId += idgen.nodeId << counterBits
		currId += idgen.counter

		idgen.idCh <- currId
	}
}

func (idgen *IdGenerator) startGenerator(nodeIdStr string) {
	idgen.once.Do(func() {
		nodeIdInt, _ := strconv.Atoi(nodeIdStr[1:])
		idgen.nodeId = uint64(nodeIdInt)
		if nodeIdInt > (1<<nodeIdBits)-1 {
			log.Fatalf("Node Id too large")
		}
		go idgen.generate()
	})
}

func (idgen *IdGenerator) GetUniqueId(nodeIdStr string) uint64 {
	idgen.startGenerator(nodeIdStr)
	return <-idgen.idCh
}
