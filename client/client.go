package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "RTVMandatoryActivity3/grpc"

	"math/rand"
)

type Clock struct {
	lamport int32
	mu      sync.Mutex
}

func (c *Clock) sendTick() int32 {
	c.mu.Lock()
	c.lamport++
	returnValue := c.lamport
	log.Println("[TICK] Lamport ticked to: " + strconv.Itoa(int(c.lamport)))
	c.mu.Unlock()

	return returnValue
}

func (c *Clock) mergeAndTick(message *pb.ServerMessage) int32 {
	c.mu.Lock()
	log.Println("[MERGE] Merging between local timestamp: " + strconv.Itoa(int(c.lamport)) + " and message timestamp: " + strconv.Itoa(int(message.Lamport)) + ", and ticking by one.")
	c.lamport = max(c.lamport, message.Lamport) + 1
	returnValue := c.lamport
	log.Println("[TICK] Lamport merged and ticked to: " + strconv.Itoa(int(c.lamport)))
	c.mu.Unlock()

	return returnValue
}

func main() {
	id := rand.Int31()
	logFileName := "logs/client" + strconv.Itoa(int(id)) + ".log"

	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	logPrefix := "[CLIENT " + strconv.Itoa(int(id)) + "] "
	log.SetPrefix(logPrefix)

	defer file.Close()

	clock := Clock{
		lamport: 0,
	}
	join := true
	leave := false
	message := ""

	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("[ERROR] Server not found!")
	}

	client := pb.NewChitChatClient(conn)

	stream, err := client.Chat(context.Background())

	if err != nil {
		log.Fatalf("[ERROR] Chat could not be established!")
	}

	defer conn.Close()

	// construct join message
	joinMessage := pb.ClientMessage{
		Id:      id,
		Lamport: clock.sendTick(),
		Leave:   leave,
		Join:    join,
		Message: message,
	}

	stream.Send(&joinMessage)

	go clock.PrintReceivedMessages(stream)

	join = false

	var input string
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		input = scanner.Text()

		// leave check
		if strings.ToLower(input) == "leave" {
			leave = true
		}

		// checking to see if the input is valid
		if !utf8.ValidString(input) || !(utf8.RuneCountInString(input) <= 128) || input == "" {
			fmt.Println("[INFO] Your message is either too long, or contains invalid characters! Your message has not been sent. Please enter a valid string of 128 characters or less")
			continue
		}

		// construct message
		messageConstruct := pb.ClientMessage{
			Id:      id,
			Lamport: clock.sendTick(),
			Leave:   leave,
			Join:    join,
			Message: input,
		}

		// send message
		stream.Send(&messageConstruct)

		// If leave true, break
		if leave {
			stream.CloseSend()
			break
		}
	}
}

func (c *Clock) PrintReceivedMessages(stream pb.ChitChat_ChatClient) {
	for {
		message, err := stream.Recv()

		if err == io.EOF {
			log.Println("[DISCONNECT] EOF: Stream is closed.")
			return
		}
		if err != nil {
			log.Println("[ERROR] Error in recieved message! Server could be shut down.")
			return
		}

		log.Println(message.Message + " - timestamp: " + strconv.Itoa(int(message.Lamport)))

		c.mergeAndTick(message)

		fmt.Println(message.Message + " - timestamp: " + strconv.Itoa(int(message.Lamport)))
	}
}
