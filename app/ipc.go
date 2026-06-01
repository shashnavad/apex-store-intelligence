package main

import (
	"bufio"
	"context"
	"log"
	"net"
	"os"
	"strings"
)

func startUnixSocketServer(ctx context.Context, engine *Engine, socketPath string) {
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("IPC listener failed: %v", err)
	}

	go func() {
		<-ctx.Done()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()

	log.Printf("IPC listening on %s", socketPath)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			log.Printf("IPC accept error: %v", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			scanner := bufio.NewScanner(c)
			scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
			for scanner.Scan() {
				engine.HandleIPCLine(scanner.Bytes())
			}
		}(conn)
	}
}
