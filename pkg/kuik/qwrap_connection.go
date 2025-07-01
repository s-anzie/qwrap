package kuik

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"qwrap/internal/client/downloader"
	"qwrap/pkg/qwrappb"
)

// qwrapConnection is the concrete implementation of the Connection interface.
// It wraps the qwrap downloader to present it as a standard QUIC connection.
type qwrapConnection struct {
	downloader downloader.Downloader
	cancelFunc context.CancelFunc
	ctx        context.Context
	remoteAddr net.Addr
	config     *Config
}

// OpenStreamSync will begin the download and return a stream that can be used to monitor it.
func (c *qwrapConnection) OpenStreamSync(ctx context.Context) (Stream, error) {
	if c.config == nil {
		return nil, fmt.Errorf("kuik.Config is nil, cannot start download")
	}
	if c.config.QwrapFileID == "" {
		return nil, fmt.Errorf("QwrapFileID not set in kuik.Config")
	}
	if c.config.QwrapDestPath == "" {
		return nil, fmt.Errorf("QwrapDestPath not set in kuik.Config")
	}

	clientReqId := fmt.Sprintf("client-req-%d", time.Now().UnixNano())
	transferReq := &qwrappb.TransferRequest{
		RequestId: clientReqId,
		FilesToTransfer: []*qwrappb.FileMetadata{
			{FileId: c.config.QwrapFileID, TotalSize: c.config.QwrapFileSize},
		},
		Options: &qwrappb.TransferOptions{VerifyChunkChecksums: true},
	}

	destFile, err := os.OpenFile(c.config.QwrapDestPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open destination file %s: %w", c.config.QwrapDestPath, err)
	}

	progressChan, finalErrorChan := c.downloader.Download(c.ctx, transferReq, destFile)

	stream := &qwrapStream{
		progressChan: progressChan,
		errorChan:    finalErrorChan,
		destWriter:   destFile,
	}

	return stream, nil
}

// CloseWithError will terminate the download.
func (c *qwrapConnection) CloseWithError(code ApplicationErrorCode, reason string) error {
	c.cancelFunc()
	shutdownTimeout := 10 * time.Second
	return c.downloader.Shutdown(shutdownTimeout)
}

// LocalAddr returns a dummy local address.
func (c *qwrapConnection) LocalAddr() net.Addr {
	return &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)}
}

// RemoteAddr returns the orchestrator's address.
func (c *qwrapConnection) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// Context returns the connection's context.
func (c *qwrapConnection) Context() context.Context {
	return c.ctx
}
