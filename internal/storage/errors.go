package storage

import (
	"io"
	"os"
	"strings"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

// categorizeFileError determines the error type for file operations
func categorizeFileError(operation string, err error) error {
	if err == nil {
		return nil
	}

	if os.IsNotExist(err) {
		return obeliskErrors.NewPermanentError(operation, "file not found", err)
	}

	if os.IsPermission(err) {
		return obeliskErrors.NewConfigurationError(operation, "permission denied", err)
	}

	errStr := strings.ToLower(err.Error())

	// Disk space issues
	if strings.Contains(errStr, "no space left") || strings.Contains(errStr, "disk full") {
		return obeliskErrors.NewResourceError(operation, "insufficient disk space", err)
	}

	// I/O timeouts and temporary issues
	if strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "resource temporarily unavailable") {
		return obeliskErrors.NewTransientError(operation, "temporary file system error", err)
	}

	// Default to transient for unknown file errors
	return obeliskErrors.NewTransientError(operation, "file system error", err)
}

// categorizeDataError determines the error type for data reading/parsing
func categorizeDataError(operation string, err error) error {
	if err == nil {
		return nil
	}

	if err == io.EOF {
		return nil // Normal end of file, not an error
	}

	errStr := strings.ToLower(err.Error())

	// Data corruption indicators
	if strings.Contains(errStr, "unexpected eof") ||
		strings.Contains(errStr, "invalid") ||
		strings.Contains(errStr, "corrupt") ||
		strings.Contains(errStr, "malformed") {
		return obeliskErrors.NewDataError(operation, "corrupted or invalid data", err)
	}

	// Protocol errors (from your protocol package)
	if strings.Contains(errStr, "message too large") {
		return obeliskErrors.NewPermanentError(operation, "message exceeds size limit", err)
	}

	if strings.Contains(errStr, "zero-length message") {
		return obeliskErrors.NewDataError(operation, "invalid zero-length message", err)
	}

	// Default to transient for unknown data errors
	return obeliskErrors.NewTransientError(operation, "data read error", err)
}

// categorizePoolError determines error type for file pool operations
func categorizePoolError(operation string, err error) error {
	if err == nil {
		return nil
	}

	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, "file pool not initialized") {
		return obeliskErrors.NewConfigurationError(operation, "file pool not ready", err)
	}

	if strings.Contains(errStr, "too many open files") {
		return obeliskErrors.NewResourceError(operation, "file descriptor limit exceeded", err)
	}

	return obeliskErrors.NewTransientError(operation, "file pool error", err)
}
