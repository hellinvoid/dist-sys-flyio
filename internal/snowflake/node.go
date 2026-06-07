package snowflake

import (
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
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

type Node struct {
	*maelstrom.Node

	idCh chan uint64

	currTimestamp uint64
	nodeId        uint64
	counter       uint64

	once sync.Once
}

func NewNode() *Node {
	return &Node{
		Node: maelstrom.NewNode(),
		idCh: make(chan uint64),
		once: sync.Once{},
	}
}

func (n *Node) Generate() {
	var currId uint64
	for {
		currId = 0
		currTimestamp := uint64(time.Since(start))

		if currTimestamp == n.currTimestamp {
			n.counter++
		} else {
			n.currTimestamp = currTimestamp
			n.counter = 0
		}

		currId += currTimestamp << timestampShiftBits
		currId += n.nodeId << counterBits
		currId += n.counter

		n.idCh <- currId
	}
}

func (n *Node) SetNodeId(nodeIdStr string) {
	n.once.Do(func() {
		nodeIdInt, _ := strconv.Atoi(nodeIdStr[1:])
		n.nodeId = uint64(nodeIdInt)
		if nodeIdInt > (1<<nodeIdBits)-1 {
			log.Fatalf("Node Id too large")
		}
	})
}

func (n *Node) GetUniqueId() uint64 {
	return <-n.idCh
}
