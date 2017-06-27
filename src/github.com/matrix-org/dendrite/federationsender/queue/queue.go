// Copyright 2017 Vector Creations Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
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

// NewOutgoingQueues makes a new OutgoingQueues
func NewOutgoingQueues(origin gomatrixserverlib.ServerName, client *gomatrixserverlib.FederationClient) *OutgoingQueues {
	return &OutgoingQueues{
		origin: origin,
		client: client,
		queues: map[gomatrixserverlib.ServerName]*outgoingQueue{},
	}
}

// SendEvent sends an event to the destinations
func (oqs *OutgoingQueues) SendEvent(
	ev *gomatrixserverlib.Event, origin gomatrixserverlib.ServerName,
	destinations []gomatrixserverlib.ServerName,
) error {
	if origin != oqs.origin {
		return fmt.Errorf(
			"sendevent: unexpected server to send as: got %q expected %q",
			origin, oqs.origin,
		)
	}

	// Remove our own server from the list of destinations.
	destinations = filterDestinations(oqs.origin, destinations)

	log.WithFields(log.Fields{
		"destinations": destinations, "event": ev.EventID(),
	}).Info("Sending event")

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
	return nil
}

// filterDestinations removes our own server from the list of destinations.
// Otherwise we could end up trying to talk to ourselves.
func filterDestinations(origin gomatrixserverlib.ServerName, destinations []gomatrixserverlib.ServerName) []gomatrixserverlib.ServerName {
	var result []gomatrixserverlib.ServerName
	for _, destination := range destinations {
		if destination == origin {
			continue
		}
		result = append(result, destination)
	}
	return result
}

// outgoingQueue is a queue of events for a single destination.
// It is responsible for sending the events to the destination and
// ensures that only one request is in flight to a given destination
// at a time.
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

// Send event adds the event to the pending queue for the destination.
// If the queue is empty then it starts a background goroutine to
// start sending events to that destination.
func (oq *outgoingQueue) sendEvent(ev *gomatrixserverlib.Event) {
	oq.mutex.Lock()
	defer oq.mutex.Unlock()
	oq.pendingEvents = append(oq.pendingEvents, ev)
	if !oq.running {
		go oq.backgroundSend()
	}
}

func (oq *outgoingQueue) backgroundSend() {
	for {
		t := oq.next()
		if t == nil {
			// If the queue is empty then stop processing for this destination.
			// TODO: Remove this destination from the queue map.
			return
		}

		// TODO: handle retries.
		// TODO: blacklist uncooperative servers.

		_, err := oq.client.SendTransaction(*t)
		if err != nil {
			log.WithFields(log.Fields{
				"destination": oq.destination,
				log.ErrorKey:  err,
			}).Info("problem sending transaction")
		}
	}
}

// next creates a new transaction from the pending event queue
// and flushes the queue.
// Returns nil if the queue was empty.
func (oq *outgoingQueue) next() *gomatrixserverlib.Transaction {
	oq.mutex.Lock()
	defer oq.mutex.Unlock()
	if len(oq.pendingEvents) == 0 {
		oq.running = false
		return nil
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
	oq.sentCounter += len(t.PDUs)
	return &t
}
