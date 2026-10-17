package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

var ErrUnknownCommand = errors.New("unknown command")

type Dispatcher struct {
	DispatchedCommits map[string]Runner
	PendingCommits    []string
	Runners           []Runner
}

type Runner struct {
	Host string
	Port int
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		DispatchedCommits: make(map[string]Runner),
	}
}

func main() {
	host := flag.String("host", "localhost", "dispatcher's host, by default it uses localhost")
	port := flag.Int("port", 8888, "dispatcher's port, by default it uses 8888")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := NewDispatcher()

	go d.runnerChecker(ctx)

	go d.redistribute(ctx)

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		fmt.Printf("couldn't start TCP listener: %v", err)
		return
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("couldn't accept TCP connection: %v", err)
			continue
		}

		go d.HandleConn(ctx, conn)
	}
}

func (d *Dispatcher) HandleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	inbound := make(chan string)
	outbound := make(chan string)

	go func() {
		for msg := range outbound {
			_, err := fmt.Fprintf(conn, "%s\n", msg)
			if err != nil {
				fmt.Printf("write error: %v\n", err)
				return
			}

		}
	}()

	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			inbound <- scanner.Text()
		}
		close(inbound)
	}()

	for message := range inbound {
		resp, err := d.handleMessage(ctx, message)
		if err != nil {
			fmt.Printf("an error occured: %v", err)
			outbound <- err.Error()
			continue
		}

		outbound <- resp
	}
}

func (d *Dispatcher) dispatchTests(ctx context.Context, commitID string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping tests dispatch...")
			return
		case <-time.After(3 * time.Second):
			fmt.Println("Trying to dispatch to runners")

			for _, runner := range d.Runners {
				response, err := d.communicate(runner, fmt.Sprintf("runtest:%s", commitID))
				if err != nil {
					fmt.Printf("couldn't reach runner at host: %s and port: %d\n", runner.Host, runner.Port)
					continue
				}

				if response != "+OK" {
					continue
				}

				fmt.Printf("adding id %s\n", commitID)
				d.DispatchedCommits[commitID] = runner
				if slices.Contains(d.PendingCommits, commitID) {
					idx := slices.Index(d.PendingCommits, commitID)
					d.PendingCommits = slices.Delete(d.PendingCommits, idx, idx+1)
				}
				return
			}
		}
	}
}

func (d *Dispatcher) runnerChecker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping runner checker...")
			return
		case <-time.After(1 * time.Second):
			for _, runner := range d.Runners {
				response, err := d.communicate(runner, "ping")
				if err != nil {
					d.manageCommitLists(runner)
					fmt.Printf("couldn't reach runner at host: %s and port: %d\n", runner.Host, runner.Port)
					continue
				}

				if response != "+pong" {
					fmt.Printf("removing runner at host: %s and port %d\n", runner.Host, runner.Port)
					d.manageCommitLists(runner)
				}
			}
		}
	}
}

func (d *Dispatcher) manageCommitLists(runner Runner) {
	for commit, assignedRunner := range d.DispatchedCommits {
		if assignedRunner == runner {
			delete(d.DispatchedCommits, commit)
			d.PendingCommits = append(d.PendingCommits, commit)
			break
		}
	}
	idx := slices.Index(d.Runners, runner)

	d.Runners = slices.Delete(d.Runners, idx, idx+1)
}

func (d *Dispatcher) communicate(runner Runner, message string) (string, error) {
	var dialer net.Dialer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", runner.Host, runner.Port))
	if err != nil {
		return "", fmt.Errorf("couldn't reach dispatcher: %v", err)
	}
	defer conn.Close()

	parsedMessage := fmt.Sprintf("+%s\n", message)

	conn.Write([]byte(parsedMessage))

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	return strings.TrimSuffix(response, "\n"), nil
}

func (d *Dispatcher) redistribute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping redistribution of commits process...")
			return
		case <-time.After(5 * time.Second):
			for _, commit := range d.PendingCommits {
				fmt.Println("running redistribute")
				d.dispatchTests(ctx, commit)
			}
		}
	}
}

func (d *Dispatcher) handleMessage(ctx context.Context, message string) (string, error) {
	switch {
	case message == "+status":
		return "+OK", nil
	case strings.HasPrefix(message, "+register"):
		_, after, found := strings.Cut(message, ":")
		if !found {
			return "", fmt.Errorf("invalid command")
		}

		host, port, err := net.SplitHostPort(after)
		if err != nil {
			return "", err
		}
		parsedPort, err := strconv.Atoi(port)
		if err != nil {
			return "", fmt.Errorf("invalid port number: %v", err)
		}

		d.Runners = append(d.Runners, Runner{Host: host, Port: parsedPort})

		return "+OK", nil
	case strings.HasPrefix(message, "+dispatch"):
		if len(d.Runners) == 0 {
			return "", fmt.Errorf("no runners are registered")
		}

		parts := strings.Split(message, ":")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid dispatch command")
		}

		go d.dispatchTests(ctx, parts[1])
		return "+OK", nil
	case strings.HasPrefix(message, "+results"):
		_, after, found := strings.Cut(message, ":")
		if !found {
			return "", fmt.Errorf("invalid results command")
		}

		parts := strings.Split(after, ":")
		if len(parts) != 3 {
			return "", fmt.Errorf("invalid results command")
		}

		commitID, _, results := parts[0], parts[1], parts[2]
		delete(d.DispatchedCommits, commitID)

		if !dirExists("test_results") {
			err := os.Mkdir("test_results", 0755)
			if err != nil {
				return "", fmt.Errorf("couldn't create results directory")
			}
		}

		target := filepath.Join("test_results", commitID)

		f, err := os.Create(target)
		if err != nil {
			return "", err
		}
		defer f.Close()

		io.Copy(f, strings.NewReader(results))
		return "+OK", nil

	default:
		return "", ErrUnknownCommand

	}
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir()
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}
