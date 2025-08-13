package errors

import (
	"errors"
	"fmt"
)

type ErrorType int

const (
	ErrorTypeTransient ErrorType = iota
	ErrorTypePermanent
	ErrorTypeConfiguration
	ErrorTypeResource
	ErrorTypeData
)

type ObeliskError struct {
	Type      ErrorType
	Operation string
	Message   string
	Cause     error
	Retryable bool
}

func (e *ObeliskError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s failed: %s (caused by: %v)", e.Operation, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s failed: %s", e.Operation, e.Message)
}

// Transient errors (network issues, temporary disk problems)
func NewTransientError(operation, message string, cause error) *ObeliskError {
	return &ObeliskError{
		Type:      ErrorTypeTransient,
		Operation: operation,
		Message:   message,
		Cause:     cause,
		Retryable: true,
	}
}

// Permanent errors (invalid input, logic errors)
func NewPermanentError(operation, message string, cause error) *ObeliskError {
	return &ObeliskError{
		Type:      ErrorTypePermanent,
		Operation: operation,
		Message:   message,
		Cause:     cause,
		Retryable: false,
	}
}

// Configuration errors (missing files, invalid config)
func NewConfigurationError(operation, message string, cause error) *ObeliskError {
	return &ObeliskError{
		Type:      ErrorTypeConfiguration,
		Operation: operation,
		Message:   message,
		Cause:     cause,
		Retryable: false,
	}
}

// Resource errors (disk full, out of memory)
func NewResourceError(operation, message string, cause error) *ObeliskError {
	return &ObeliskError{
		Type:      ErrorTypeResource,
		Operation: operation,
		Message:   message,
		Cause:     cause,
		Retryable: false,
	}
}

// Data errors (corruption, invalid format)
func NewDataError(operation, message string, cause error) *ObeliskError {
	return &ObeliskError{
		Type:      ErrorTypeData,
		Operation: operation,
		Message:   message,
		Cause:     cause,
		Retryable: false, // Data corruption is not retryable
	}
}

var (
	ErrTopicNotFound        = NewPermanentError("topic_lookup", "topic not found", nil)
	ErrInvalidOffset        = NewPermanentError("offset_validation", "invalid offset", nil)
	ErrFilePoolNotReady     = NewConfigurationError("file_pool_access", "file pool not initialized", nil)
	ErrDiskFull             = NewResourceError("disk_write", "insufficient disk space", nil)
	ErrCorruptedIndex       = NewDataError("index_read", "index file is corrupted", nil)
	ErrInvalidMessageFormat = NewPermanentError("message_parse", "invalid message format", nil)
)

func IsRetryable(err error) bool {
	var obeliskErr *ObeliskError
	if errors.As(err, &obeliskErr) {
		return obeliskErr.Retryable
	}
	return false
}

func GetErrorType(err error) ErrorType {
	var obeliskErr *ObeliskError
	if errors.As(err, &obeliskErr) {
		return obeliskErr.Type
	}
	return ErrorTypePermanent
}
