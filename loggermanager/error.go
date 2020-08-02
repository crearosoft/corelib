package loggermanager

import "fmt"

// CoreError custom error
type CoreError struct {
	msg string
}

// Wrap error
func Wrap(msg string) *CoreError {
	err := CoreError{}
	err.msg = msg
	return &err
}
func (cerr *CoreError) Error() string {
	return fmt.Sprintf(cerr.msg)
}
