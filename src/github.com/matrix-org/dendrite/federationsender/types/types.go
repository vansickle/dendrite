package types

import (
	"fmt"
	"github.com/matrix-org/gomatrixserverlib"
)

// JoinedHost ...
type JoinedHost struct {
	EventID    string
	ServerName gomatrixserverlib.ServerName
}

// LastSentIDMismatchError ...
type LastSentIDMismatchError struct {
	DatabaseID string
	EventID    string
}

func (l LastSentIDMismatchError) Error() string {
	return fmt.Sprintf(
		"mismatched last sent event ID: had %q in database got %q  in event",
		l.DatabaseID, l.EventID,
	)
}
