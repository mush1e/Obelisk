package services

type TopicStatsProvider interface {
	GetTopicStats(topic string) (buffered int, persisted int64, err error)
}

type TopicService struct {
	provider TopicStatsProvider
}

func NewTopicService(provider TopicStatsProvider) *TopicService {
	return &TopicService{provider: provider}
}

func (s *TopicService) GetStats(topic string) (int, int64, error) {
	return s.provider.GetTopicStats(topic)
}
