package ident

import "github.com/google/uuid"

type IDMaker interface {
	NewID() string
}

type UUID4Maker struct {
}

func NewUUID4Maker() *UUID4Maker {
	return &UUID4Maker{}
}

func (U *UUID4Maker) NewID() string {
	return uuid.NewString()
}
