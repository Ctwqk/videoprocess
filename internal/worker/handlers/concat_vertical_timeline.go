package handlers

import (
	"context"
	"errors"
	"os"
	"strconv"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ConcatVerticalTimelineHandler struct {
	Runner vpffmpeg.Runner
}

func (h ConcatVerticalTimelineHandler) NodeType() string {
	return "concat_vertical_timeline"
}

func (h ConcatVerticalTimelineHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	firstVideo := inputPaths["video_first"]
	if firstVideo == "" {
		return nil, errors.New("missing input path on video_first port")
	}
	secondVideo := inputPaths["video_second"]
	if secondVideo == "" {
		return nil, errors.New("missing input path on video_second port")
	}
	topImage := inputPaths["image_top"]
	bottomImage := inputPaths["image_bottom"]

	var generatedTop string
	var generatedBottom string
	var err error
	if topImage == "" {
		generatedTop, err = h.extractDefaultFrame(ctx, firstVideo, true)
		if err != nil {
			return nil, err
		}
		topImage = generatedTop
	}
	if bottomImage == "" {
		generatedBottom, err = h.extractDefaultFrame(ctx, secondVideo, false)
		if err != nil {
			safeRemove(generatedTop)
			return nil, err
		}
		bottomImage = generatedBottom
	}

	paneWidth := intValue(config["pane_width"], 640)
	paneHeight := intValue(config["pane_height"], 360)
	backgroundColor := stringValue(config["background_color"], "black")

	firstSegment, err := h.renderSegment(ctx, firstVideo, bottomImage, "top", paneWidth, paneHeight, backgroundColor)
	if err != nil {
		safeRemove(generatedTop)
		safeRemove(generatedBottom)
		return nil, err
	}
	secondSegment, err := h.renderSegment(ctx, secondVideo, topImage, "bottom", paneWidth, paneHeight, backgroundColor)
	if err != nil {
		safeRemove(firstSegment)
		safeRemove(generatedTop)
		safeRemove(generatedBottom)
		return nil, err
	}
	defer safeRemove(firstSegment)
	defer safeRemove(secondSegment)
	defer safeRemove(generatedTop)
	defer safeRemove(generatedBottom)

	args := []string{
		"-i", firstSegment,
		"-i", secondSegment,
		"-filter_complex", "[0:v][0:a][1:v][1:a]concat=n=2:v=1:a=1[v][a]",
		"-map", "[v]",
		"-map", "[a]",
	}
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, "-c:a", "aac", outputPath)
	if err := runFFmpeg(ctx, h.Runner, args); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func (h ConcatVerticalTimelineHandler) renderSegment(ctx context.Context, activeVideo string, staticImage string, activePosition string, paneWidth int, paneHeight int, backgroundColor string) (string, error) {
	probe, err := probePath(ctx, h.Runner, activeVideo)
	if err != nil {
		return "", err
	}
	if probe.Duration <= 0 {
		probe.Duration = 5
	}
	outputFile, err := os.CreateTemp("", "vp_vertical_segment_*.mp4")
	if err != nil {
		return "", err
	}
	outputPath := outputFile.Name()
	if err := outputFile.Close(); err != nil {
		safeRemove(outputPath)
		return "", err
	}

	args := VerticalTimelineSegmentArgs(activeVideo, staticImage, outputPath, activePosition, paneWidth, paneHeight, backgroundColor, probe)
	if err := runFFmpeg(ctx, h.Runner, args); err != nil {
		safeRemove(outputPath)
		return "", err
	}
	return outputPath, nil
}

func VerticalTimelineSegmentArgs(activeVideo string, staticImage string, outputPath string, activePosition string, paneWidth int, paneHeight int, backgroundColor string, probe probeSummary) []string {
	duration := probe.Duration
	if duration <= 0 {
		duration = 5
	}
	durationText := strconv.FormatFloat(duration, 'f', 3, 64)

	topSource := "[0:v]"
	bottomSource := "[1:v]"
	if activePosition != "top" {
		topSource = "[1:v]"
		bottomSource = "[0:v]"
	}

	filterComplex := topSource + scaleFilter(strconv.Itoa(paneWidth), strconv.Itoa(paneHeight), "decrease") +
		",pad=" + strconv.Itoa(paneWidth) + ":" + strconv.Itoa(paneHeight) + ":(ow-iw)/2:(oh-ih)/2:color=" + backgroundColor + ",setsar=1,fps=30[top];" +
		bottomSource + scaleFilter(strconv.Itoa(paneWidth), strconv.Itoa(paneHeight), "decrease") +
		",pad=" + strconv.Itoa(paneWidth) + ":" + strconv.Itoa(paneHeight) + ":(ow-iw)/2:(oh-ih)/2:color=" + backgroundColor + ",setsar=1,fps=30[bottom];" +
		"[top][bottom]vstack=inputs=2,setsar=1[v]"

	args := []string{
		"-i", activeVideo,
		"-loop", "1",
		"-t", durationText,
		"-i", staticImage,
	}
	if !probe.HasAudio {
		args = append(args,
			"-f", "lavfi",
			"-t", durationText,
			"-i", "anullsrc=r=48000:cl=stereo",
		)
	}
	args = append(args,
		"-filter_complex", filterComplex,
		"-map", "[v]",
	)
	if probe.HasAudio {
		args = append(args, "-map", "0:a:0")
	} else {
		args = append(args, "-map", "2:a:0")
	}
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args,
		"-c:a", "aac",
		"-shortest",
		outputPath,
	)
	return args
}

func (h ConcatVerticalTimelineHandler) extractDefaultFrame(ctx context.Context, videoPath string, preferFromEnd bool) (string, error) {
	frameCount, err := h.countVideoFrames(ctx, videoPath)
	if err != nil {
		return "", err
	}
	frameIndex := defaultFrameIndex(frameCount, preferFromEnd)
	outputFile, err := os.CreateTemp("", "vp_vertical_frame_*.png")
	if err != nil {
		return "", err
	}
	outputPath := outputFile.Name()
	if err := outputFile.Close(); err != nil {
		safeRemove(outputPath)
		return "", err
	}
	args := []string{
		"-i", videoPath,
		"-vf", "select=eq(n\\," + strconv.Itoa(frameIndex) + ")",
		"-vsync", "vfr",
		"-frames:v", "1",
		outputPath,
	}
	if err := runFFmpeg(ctx, h.Runner, args); err != nil {
		safeRemove(outputPath)
		return "", err
	}
	return outputPath, nil
}

func (h ConcatVerticalTimelineHandler) countVideoFrames(ctx context.Context, videoPath string) (int, error) {
	runner := h.Runner
	if runner.Binary == "" && runner.ProbeBinary == "" {
		runner = vpffmpeg.NewRunner()
	}
	return runner.CountVideoFrames(ctx, videoPath)
}

func defaultFrameIndex(frameCount int, preferFromEnd bool) int {
	if frameCount <= 0 {
		frameCount = 1
	}
	targetOffset := 15
	if preferFromEnd {
		index := frameCount - targetOffset
		if index < 0 {
			return 0
		}
		return index
	}
	index := targetOffset - 1
	if index > frameCount-1 {
		return frameCount - 1
	}
	return index
}
