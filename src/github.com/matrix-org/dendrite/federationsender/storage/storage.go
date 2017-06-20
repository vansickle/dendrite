package storage

import (
	"database/sql"
	"github.com/matrix-org/dendrite/federationsender/types"
)

// Database ...
type Database struct {
	joinedHostsStatements
	roomStatements
	db *sql.DB
}

func (d *Database) prepare() error {
	var err error

	if err = d.joinedHostsStatements.prepare(d.db); err != nil {
		return err
	}

	if err = d.roomStatements.prepare(d.db); err != nil {
		return err
	}

	return nil
}

// UpdateRoom implements
func (d *Database) UpdateRoom(
	roomID, oldLastSentEventID, newLastSentEventID string,
	addHosts []types.JoinedHost,
	removeHosts []string,
) (joinedHosts []types.JoinedHost, err error) {
	err = runTransaction(d.db, func(txn *sql.Tx) error {
		if err = d.insertRoom(txn, roomID); err != nil {
			return err
		}
		lastSentEventID, err := d.selectRoomForUpdate(txn, roomID)
		if err != nil {
			return err
		}
		if lastSentEventID != oldLastSentEventID {
			return types.LastSentIDMismatchError{lastSentEventID, oldLastSentEventID}
		}
		joinedHosts, err = d.selectJoinedHosts(txn, roomID)
		if err != nil {
			return err
		}
		for _, add := range addHosts {
			err = d.insertJoinedHosts(txn, roomID, add.EventID, add.ServerName)
			if err != nil {
				return err
			}
		}
		if err = d.deleteJoinedHosts(txn, removeHosts); err != nil {
			return err
		}
		return d.updateRoom(txn, roomID, newLastSentEventID)
	})
	return
}

func runTransaction(db *sql.DB, fn func(txn *sql.Tx) error) (err error) {
	txn, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			txn.Rollback()
			panic(r)
		} else if err != nil {
			txn.Rollback()
		} else {
			err = txn.Commit()
		}
	}()
	err = fn(txn)
	return
}
