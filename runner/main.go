package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrUnknownCommand = errors.New("unknown command")

type Runner struct {
	Wg                               sync.WaitGroup
	DispatcherServerHost, RunnerHost string
	DispatcherServerPort, RunnerPort int
	Repo                             string
	LastCommunication                time.Time
	Busy                             bool
}

func NewRunner(repo, host string, port int) *Runner {
	return &Runner{
		DispatcherServerHost: "localhost",
		RunnerHost:           host,
		DispatcherServerPort: 8888,
		RunnerPort:           port,
		Repo:                 repo,
	}
}

func main() {
	host := flag.String("host", "localhost", "runner's host, by default it uses localhost")
	port := flag.Int("port", 8900, "runner's port, by default it uses 8900")
	repo := flag.String("repo", "", "repo to run the tests against")

	flag.Parse()

	if *repo == "" {
		fmt.Fprintf(os.Stderr, "Provide a repo name\n")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		fmt.Printf("couldn't start TCP listener: %v", err)
		return
	}

	r := NewRunner(*repo, *host, *port)

	go func() {
		<-ctx.Done()
		fmt.Println("\nShutdown signal received. Closing net.Listener...")
		l.Close()
	}()

	r.Wg.Add(1)
	go r.DispatcherChecker(ctx, cancel)

	r.Register(cancel)

	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			fmt.Printf("couldn't accept TCP connection: %v", err)
			continue
		}

		r.Wg.Add(1)
		go r.HandleConn(ctx, conn)
	}

	fmt.Println("Waiting for active connections to finish...")
	r.Wg.Wait()
	fmt.Println("All goroutines exited. Safe to shut down program.")
}

func (r *Runner) DispatcherChecker(ctx context.Context, cancel context.CancelFunc) {
	defer r.Wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping dispatcher checker...")
			return
		case <-time.After(5 * time.Second):
			if time.Since(r.LastCommunication) <= 10 {
				continue
			}

			response, err := r.communicate("status")
			if err != nil {
				fmt.Printf("can't communicate with dispatcher server: %v\n", err)
				fmt.Println("Shutting down...")

				cancel()
				return
			}

			if response != "+OK" {
				fmt.Println("Dispatcher is no longer functional")
				cancel()
				return
			}
		}
	}
}

func (r *Runner) Register(cancel context.CancelFunc) {
	defer r.Wg.Done()
	resp, err := r.communicate(fmt.Sprintf("register:%s:%d", r.RunnerHost, r.RunnerPort))
	if err != nil {
		fmt.Printf("can't reach dispatcher: %v\n", err)
		cancel()
		return
	}

	if resp != "+OK" {
		fmt.Printf("runner was unable to be registered with dispatcher")
		cancel()
		return
	}
}

func (r *Runner) HandleConn(ctx context.Context, conn net.Conn) {
	defer r.Wg.Done()
	defer conn.Close()

	inbound := make(chan string)
	outbound := make(chan string)

	r.Wg.Go(func() {
		for msg := range outbound {
			_, err := fmt.Fprintf(conn, "%s\n", msg)
			if err != nil {
				fmt.Printf("write error: %v\n", err)
				return
			}

		}
	})

	r.Wg.Go(func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			inbound <- scanner.Text()
		}
		close(inbound)
	})

	for message := range inbound {
		resp, err := r.handleMessage(ctx, message)
		if err != nil {
			fmt.Printf("an error occured: %v", err)
			outbound <- err.Error()
			continue
		}

		outbound <- resp
	}
}

func (r *Runner) handleMessage(ctx context.Context, message string) (string, error) {
	switch {
	case message == "+ping":
		r.LastCommunication = time.Now()
		return "+pong", nil
	case strings.HasPrefix(message, "+runtest"):
		fmt.Printf("got runtest command: am I busy? %t\n", r.Busy)
		parts := strings.Split(message, ":")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid runtest command")
		}
		commitID := parts[1]

		if r.Busy {
			return "+BUSY", nil
		}

		r.Wg.Go(func() {
			r.Busy = true
			err := r.RunTests(commitID)
			if err != nil {
				r.Busy = false
				fmt.Printf("test run for commit: %s failed: %s\n", commitID, err.Error())
			}

			r.Busy = false
		})

		return "+OK", nil
	default:
		return "", ErrUnknownCommand
	}
}

func (r *Runner) RunTests(commitID string) error {
	commitID = strings.Trim(commitID, "\"\n\r ")
	cmd := exec.Command("git", "clean", "-d", "-f", "-x")
	cmd.Dir = r.Repo
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not clean repository: %v", err)
	}

	cmd = exec.Command("git", "pull")
	cmd.Dir = r.Repo
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not call git pull: %v", err)
	}

	cmd = exec.Command("git", "reset", "--hard", commitID)
	fmt.Println(commitID, " length: ", len(commitID))
	cmd.Dir = r.Repo
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not update to given commit hash: %v (git output: %s)", err, string(outputBytes))
	}

	cmd = exec.Command("go", "test", "./...")
	cmd.Dir = r.Repo
	outputBytes, err = cmd.CombinedOutput()
	output := string(outputBytes)
	if err != nil {
		fmt.Printf("tests completed with failures/errors: %v\n", err)
	}

	resultPayload := fmt.Sprintf("results:%s:%d:%s", commitID, len(output), output)
	if _, err := r.communicate(resultPayload); err != nil {
		return fmt.Errorf("could not send results to dispatcher: %v", err)
	}

	return nil
}

func (r *Runner) communicate(message string) (string, error) {
	var dialer net.Dialer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", r.DispatcherServerHost, r.DispatcherServerPort))
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
