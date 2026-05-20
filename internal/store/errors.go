package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

var ErrConflict = errors.New("store conflict")

func IsNotFound(err error) bool {
	return err == pgx.ErrNoRows
}

func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}
