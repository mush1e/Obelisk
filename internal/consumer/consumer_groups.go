package consumer

type ConsumerGroup struct {
	ID      string
	Topics  []string
	Members map[string]*Consumer
}
