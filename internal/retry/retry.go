package retry

import (
	"context"
	"math"
	"time"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

type Config struct {
	MaxAttempts   int           // Maximum number of retry attempts
	InitialDelay  time.Duration // Initial delay between retries
	MaxDelay      time.Duration // Maximum delay between retries
	BackoffFactor float64       // Multiplier for exponential backoff
}

func DefaultConfig() Config {
	return Config{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
	}
}

type Operation func() error

func Retry(ctx context.Context, config Config, op Operation) error {
	var lastErr error

	for attempt := 0; attempt < config.MaxAttempts; attempt++ {
		// Execute operation
		err := op()
		if err == nil {
			return nil // Success!
		}

		lastErr = err

		// Check if error is retryable
		if !obeliskErrors.IsRetryable(err) {
			return err // Don't retry permanent errors
		}

		// Don't sleep after the last attempt
		if attempt == config.MaxAttempts-1 {
			break
		}

		// Calculate delay with exponential backoff
		delay := calculateDelay(config, attempt)

		// Sleep with context cancellation support
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return lastErr // Return the last error after all attempts failed
}

func calculateDelay(config Config, attempt int) time.Duration {
	delay := float64(config.InitialDelay) * math.Pow(config.BackoffFactor, float64(attempt))

	// Cap at maximum delay
	if delay > float64(config.MaxDelay) {
		delay = float64(config.MaxDelay)
	}

	return time.Duration(delay)
}

func RetryWithMetrics(
	ctx context.Context,
	config Config,
	op Operation,
	onAttempt func(attempt int, err error),
	onSuccess func(attempts int),
	onFailure func(attempts int, finalErr error),
) error {
	var lastErr error

	for attempt := 0; attempt < config.MaxAttempts; attempt++ {
		err := op()

		// Call metrics callback for each attempt
		if onAttempt != nil {
			onAttempt(attempt+1, err)
		}

		if err == nil {
			// Success callback
			if onSuccess != nil {
				onSuccess(attempt + 1)
			}
			return nil
		}

		lastErr = err

		if !obeliskErrors.IsRetryable(err) {
			break // Don't retry non-retryable errors
		}

		if attempt == config.MaxAttempts-1 {
			break // Last attempt
		}

		delay := calculateDelay(config, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	// Failure callback
	if onFailure != nil {
		onFailure(config.MaxAttempts, lastErr)
	}

	return lastErr
}
