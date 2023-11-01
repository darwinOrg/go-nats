package dgnats

type Subject struct {
	Name  string `json:"name" binding:"required"`
	Queue string `json:"queue"`
}
