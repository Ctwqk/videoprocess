package worker

type Config struct {
	WorkerType string
	WorkerID   string
}

func DefaultWorkerType() string {
	return "ffmpeg_go"
}
