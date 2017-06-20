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

package storage

import (
	_ "database/sql"
)

const roomSchema = `
CREATE TABLE IF NOT EXISTS room (
	-- The string ID of the room
	room_id TEXT NOT NULL PRIMARY KEY,
	-- The most recent event state by the room server.
	-- We can use this to tell if our view of the room state has become
	-- desynchronised.
	last_sent_event_id TEXT NOT NULL
);`
