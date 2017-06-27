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

package types

import (
	"fmt"
	"github.com/matrix-org/gomatrixserverlib"
)

// A JoinedHost is a server that is joined to a matrix room.
type JoinedHost struct {
	// THe EventID of a m.room.member event that joins a server to a room.
	EventID string
	// The
	ServerName gomatrixserverlib.ServerName
}

// A EventIDMismatchError indicates that we have got out of sync with the
// rooms erver.
type EventIDMismatchError struct {
	// The event ID we have stored in our local database.
	DatabaseID string
	// The event ID received from the room server.
	RoomServerID string
}

func (l EventIDMismatchError) Error() string {
	return fmt.Sprintf(
		"mismatched last sent event ID: had %q in database got %q from room server",
		l.DatabaseID, l.RoomServerID,
	)
}
