package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ExportHandler struct {
	Runner vpffmpeg.Runner
}

func (h ExportHandler) NodeType() string {
	return "export"
}

func (h ExportHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inputPath := inputPaths["input"]
	if inputPath == "" {
		return nil, errors.New("missing input path on input port")
	}

	outputDir := stringValue(config["output_dir"], "/tmp/vp_export")
	filename := stringValue(config["filename"], "")
	if filename == "" {
		filename = filepath.Base(inputPath)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create export directory: %w", err)
	}

	exportPath := filepath.Join(outputDir, filename)
	if err := copyFile(inputPath, exportPath); err != nil {
		return nil, fmt.Errorf("copy export artifact: %w", err)
	}
	if err := copyFile(inputPath, outputPath); err != nil {
		return nil, fmt.Errorf("copy tracked artifact: %w", err)
	}
	return map[string]any{}, nil
}

func copyFile(src string, dst string) error {
	if same, err := sameFilePath(src, dst); err != nil {
		return err
	} else if same {
		return fmt.Errorf("source and destination are the same file: %s", src)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func sameFilePath(src string, dst string) (bool, error) {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false, err
	}
	dstInfo, err := os.Stat(dst)
	if err == nil {
		return os.SameFile(srcInfo, dstInfo), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	absSrc, srcErr := filepath.Abs(src)
	if srcErr != nil {
		return false, srcErr
	}
	absDst, dstErr := filepath.Abs(dst)
	if dstErr != nil {
		return false, dstErr
	}
	return absSrc == absDst, nil
}
