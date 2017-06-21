package queue

import (
	"fmt"
	"github.com/matrix-org/gomatrixserverlib"
	"sync"
	"time"
)

// OutgoingQueues is a collection of queues for sending transactions to other
// matrix servers
type OutgoingQueues struct {
	mutex  sync.Mutex
	queues map[gomatrixserverlib.ServerName]*outgoingQueue
	origin gomatrixserverlib.ServerName
	client *gomatrixserverlib.FederationClient
}

// SendEvent sends an event to the destinations
func (oqs *OutgoingQueues) SendEvent(
	ev *gomatrixserverlib.Event, destinations []gomatrixserverlib.ServerName,
) {
	oqs.mutex.Lock()
	defer oqs.mutex.Unlock()
	for _, destination := range destinations {
		if destination == oqs.origin {
			continue
		}
		oq := oqs.queues[destination]
		if oq == nil {
			oq = &outgoingQueue{
				origin:      oqs.origin,
				destination: destination,
				client:      oqs.client,
			}
			oqs.queues[destination] = oq
		}
		oq.sendEvent(ev)
	}
}

type outgoingQueue struct {
	mutex              sync.Mutex
	client             *gomatrixserverlib.FederationClient
	origin             gomatrixserverlib.ServerName
	destination        gomatrixserverlib.ServerName
	running            bool
	sentCounter        int
	lastTransactionIDs []gomatrixserverlib.TransactionID
	pendingEvents      []*gomatrixserverlib.Event
}

func (oq *outgoingQueue) sendEvent(ev *gomatrixserverlib.Event) {
	oq.mutex.Lock()
	defer oq.mutex.Unlock()
	oq.pendingEvents = append(oq.pendingEvents, ev)
	if oq.running {
		return
	}
	go oq.backgroundSend()
}

func (oq *outgoingQueue) backgroundSend() {
	for oq.sendOnce() {
	}
	// TODO: Remove this destination from the queue map.
}

func (oq *outgoingQueue) sendOnce() bool {
	oq.mutex.Lock()
	defer oq.mutex.Unlock()
	if len(oq.pendingEvents) == 0 {
		oq.running = false
		return false
	}

	var t gomatrixserverlib.Transaction
	now := gomatrixserverlib.AsTimestamp(time.Now())
	t.TransactionID = gomatrixserverlib.TransactionID(fmt.Sprintf("%d-%d", now, oq.sentCounter))
	t.Origin = oq.origin
	t.Destination = oq.destination
	t.OriginServerTS = now
	t.PreviousIDs = oq.lastTransactionIDs
	if t.PreviousIDs == nil {
		t.PreviousIDs = []gomatrixserverlib.TransactionID{}
	}
	oq.lastTransactionIDs = []gomatrixserverlib.TransactionID{t.TransactionID}
	for _, pdu := range oq.pendingEvents {
		t.PDUs = append(t.PDUs, *pdu)
	}
	oq.pendingEvents = nil

	// TODO: handle retries.
	// TODO: blacklist uncooperative servers.
	_, err := oq.client.SendTransaction(t)
	if err != nil {
		// TODO: log the error
		panic(err)
	}
	// TODO: Check the response for failures.

	oq.sentCounter += len(t.PDUs)

	// Keep going
	return true
}
