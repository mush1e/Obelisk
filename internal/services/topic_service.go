package services

// // This package provides service layer components for the Obelisk message broker.
// // The service layer abstracts the broker's internal components and provides a
// // clean interface for external consumers like HTTP handlers and administrative tools.
// // It implements the facade pattern to hide complex interactions between batching,
// // buffering, and storage systems.
// //
// // The service layer handles:
// // - Topic statistics aggregation from multiple internal sources
// // - Interface abstraction for testability and modularity
// // - Business logic coordination across broker subsystems
// // - Data transformation between internal and external representations
// // - Error handling and validation for service requests

// // TopicStatsProvider defines the interface for retrieving topic-level statistics.
// // This interface abstracts the underlying statistics collection mechanism,
// // allowing the service layer to work with different providers (batchers, storage
// // systems, etc.) without coupling to specific implementations.
// //
// // The interface enables:
// // - Decoupling of service layer from internal broker components
// // - Easy testing through mock implementations
// // - Flexibility in statistics data sources
// // - Consistent error handling across different providers
// type TopicStatsProvider interface {
// 	// GetTopicStats retrieves current statistics for the specified topic.
// 	// The statistics include both in-memory buffer counts and persistent
// 	// storage counts, providing a complete view of topic activity.
// 	//
// 	// Parameters:
// 	//   - topic: Name of the topic to retrieve statistics for
// 	//
// 	// Returns:
// 	//   - buffered: Number of messages currently in memory buffers
// 	//   - persisted: Number of messages written to persistent storage
// 	//   - err: Any error that occurred during statistics retrieval
// 	GetTopicStats(topic string) (buffered int, persisted int64, err error)
// }

// // TopicService provides high-level operations for topic management and monitoring.
// // This service acts as a facade over the broker's internal components, providing
// // a simplified interface for external consumers while maintaining separation of
// // concerns between the service layer and internal implementation details.
// //
// // The service coordinates with:
// // - Statistics providers for retrieving topic metrics
// // - Internal broker components through abstracted interfaces
// // - HTTP handlers that need topic information for API responses
// // - Administrative tools that require broker state visibility
// type TopicService struct {
// 	provider TopicStatsProvider // Abstracted provider for topic statistics
// }

// // NewTopicService creates a new topic service with the specified statistics provider.
// // The service requires a provider that implements the TopicStatsProvider interface
// // to abstract the underlying statistics collection mechanism. This design enables
// // testing with mock providers and flexibility in data source configuration.
// //
// // The service assumes the provider is properly initialized and ready to serve
// // requests. It does not perform any validation or initialization of the provider,
// // delegating that responsibility to the caller.
// //
// // Parameters:
// //   - provider: Implementation of TopicStatsProvider for retrieving topic statistics
// //
// // Returns:
// //   - *TopicService: Configured service instance ready for use
// func NewTopicService(provider TopicStatsProvider) *TopicService {
// 	return &TopicService{
// 		provider: provider,
// 	}
// }

// // GetStats retrieves comprehensive statistics for the specified topic.
// // This method provides a unified interface for accessing topic metrics,
// // abstracting the complexity of gathering data from multiple internal sources.
// // The statistics are useful for monitoring, capacity planning, and debugging.
// //
// // The method delegates to the configured statistics provider to gather:
// // - Buffered message count: Messages in memory awaiting persistent storage
// // - Persisted message count: Messages durably written to storage
// //
// // Error handling is transparent - any errors from the underlying provider
// // are passed through to the caller without modification, maintaining
// // error context and debugging information.
// //
// // Parameters:
// //   - topic: Name of the topic to retrieve statistics for
// //
// // Returns:
// //   - int: Number of messages currently buffered in memory
// //   - int64: Number of messages persisted to storage
// //   - error: Any error that occurred during statistics retrieval
// func (s *TopicService) GetStats(topic string) (int, int64, error) {
// 	// Delegate to provider for actual statistics retrieval
// 	// This maintains separation of concerns and enables flexible implementations
// 	return s.provider.GetTopicStats(topic)
// }
