// Package migrations embeds the SQL migration files.
//
// They live at the repository root rather than beside the runner so that an
// operator can apply them with an external tool — goose, dbmate, psql — without
// extracting them from a Go binary. A deployment whose database role is not the
// one the dispatcher connects with needs exactly that.
package migrations

import "embed"

// FS holds every migration, named <version>_<name>.sql.
//
// Files are immutable once released. The runner records a checksum of each and
// refuses to continue when one changes: the previous version edited its initial
// migration after the fact, so a fresh install and an upgraded install ended up
// with different schemas and nothing detected it.
//
//go:embed *.sql
var FS embed.FS
