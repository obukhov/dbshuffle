package service

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrNoBuffer       = errors.New("no buffer databases available")
	ErrAlreadyAssigned = errors.New("database name already assigned")
	ErrUnknownTemplate = errors.New("unknown template")
)
