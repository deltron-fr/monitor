package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	COMMIT_FILE = ".commit_id"
)

type Observer struct {
	DispatcherHost string
	DispatcherPort int
	Repo           string
}

func NewObserver(dispatcherHost, repo string, dispatcherPort int) *Observer {
	return &Observer{
		DispatcherHost: dispatcherHost,
		DispatcherPort: dispatcherPort,
		Repo:           repo,
	}
}

func main() {
	repo := flag.String("repo", "", "path to the repository this will observer")
	dispatcherServer := flag.String("dispatcher-server", "localhost:8888", "dispatcher host:port")

	flag.Parse()

	if *repo == "" {
		log.Fatal("a repository is required\n")
	}

	parts := strings.Split(*dispatcherServer, ":")
	if len(parts) != 2 {
		fmt.Printf("invalid syntax for dispatcher server address")
		return
	}

	dispatcherHost, dispatcherPort := parts[0], parts[1]
	port, err := strconv.Atoi(dispatcherPort)
	if err != nil {
		fmt.Printf("dispatcher port is not a valid integer: %v", err)
		return
	}

	observer := NewObserver(dispatcherHost, *repo, port)
	observer.poll()
}

func (o *Observer) poll() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := o.updateRepo(); err != nil {
			fmt.Printf("error updating repo: %v\n", err)
			continue
		}

		if !fileExists(COMMIT_FILE) {
			continue
		}

		if err := o.handleCommitDispatch(); err != nil {
			fmt.Printf("dispatch error: %v\n", err)
			continue
		}

		fmt.Println("Dispatched!")
	}
}

func (o *Observer) handleCommitDispatch() error {
	fmt.Println("handling dispatch!")
	res, err := o.communicate("status")
	if err != nil {
		return fmt.Errorf("status check failed: %w", err)
	}
	if res != "+OK" {
		return fmt.Errorf("server not ready (+OK expected, got %s)", res)
	}

	commit, err := readFirstLine(COMMIT_FILE)
	if err != nil {
		return fmt.Errorf("reading commit file: %w", err)
	}

	res, err = o.communicate(fmt.Sprintf("dispatch:%s", commit))
	if err != nil {
		return fmt.Errorf("dispatch communication failed: %w", err)
	}

	if res != "+OK" {
		return fmt.Errorf("dispatch rejected by server: %s", res)
	}

	return nil
}

func readFirstLine(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("file is empty")
}

func (o *Observer) communicate(message string) (string, error) {
	var d net.Dialer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", o.DispatcherHost, o.DispatcherPort))
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

func (o *Observer) updateRepo() error {
	err := os.Remove(COMMIT_FILE)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	cmd := exec.Command("git", "reset", "--hard", "HEAD")
	cmd.Dir = o.Repo
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("could not reset git: %v", err)
	}

	var commitID, newCommitID []byte

	cmd = exec.Command("git", "log", "-n1", `--format="%H"`)
	cmd.Dir = o.Repo
	commitID, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not call 'git log' on repository: %v", err)
	}

	fmt.Println(string(commitID))

	cmd = exec.Command("git", "pull")
	cmd.Dir = o.Repo
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("could not pull repo: %v", err)
	}

	cmd = exec.Command("git", "log", "-n1", `--format="%H"`)
	cmd.Dir = o.Repo
	newCommitID, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not call 'git log' on repository: %v", err)
	}

	if string(commitID) != string(newCommitID) {
		fmt.Println("file has changed!")
		f, err := os.Create(COMMIT_FILE)
		if err != nil {
			return err
		}
		defer f.Close()

		f.Write(newCommitID)
	}

	return nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}
