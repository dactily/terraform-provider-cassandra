package cassandra

import (
	"fmt"
	"regexp"
)

// boolToAction maps a creation flag to the corresponding CQL action.
var boolToAction = map[bool]string{
	true:  "CREATE",
	false: "ALTER",
}

// validRoleRegex ensures role names meet length requirements and do not contain disallowed characters.
var validRoleRegex = regexp.MustCompile(`^[^"]{1,256}$`)

// validKeyspaceRegex ensures keyspace names meet Cassandra naming rules (non-empty and valid length/characters).
var validKeyspaceRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{1,48}$`)
