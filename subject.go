package dgnats

type NatsSubject struct {
	Name  string `json:"name" binding:"required"`
	Queue string `json:"queue"`
}
