package mcap

import "errors"

// Error used to specify when an unexpected token was found in an MCAP file.
type UnexpectedTokenError struct {
	err error
}

func NewUnexpectedTokenError(err error) UnexpectedTokenError {
	return UnexpectedTokenError{err}
}

func (e UnexpectedTokenError) Error() string {
	return e.err.Error()
}

func (e UnexpectedTokenError) Is(target error) bool {
	err := UnexpectedTokenError{}
	if errors.As(target, &err) {
		return true
	}
	return errors.Is(e.err, target)
}
