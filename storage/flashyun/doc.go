// Package flashyun coordinates authorized, distributed mutations of logical
// FlashYun files stored by Restic-backed services.
//
// A coordinator lease is scoped only to tenant, workspace, and file. It allows
// independent files to progress in parallel and supplements, never replaces,
// Restic repository locks required for repository-wide maintenance. Callers
// must honor the mutation context because a failed lease refresh cancels it.
package flashyun
