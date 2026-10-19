package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

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
	c.mu.Unlock()

	return returnValue
}

func (c *Clock) mergeAndTick(message *pb.ServerMessage) int32 {
	c.mu.Lock()
	c.lamport = max(c.lamport, message.Lamport) + 1
	returnValue := c.lamport
	c.mu.Unlock()

	return returnValue
}

func main() {
	id := rand.Int31()
	clock := Clock{
		lamport: 0,
	}
	join := true
	leave := false
	message := ""

	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Server not found!")
	}

	client := pb.NewChitChatClient(conn)

	stream, err := client.Chat(context.Background())

	if err != nil {
		log.Fatalf("Chat could not be established!")
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

		if err != nil {
			log.Fatalf("Error in received message! Server could be shut down.")
		}

		c.mergeAndTick(message)

		fmt.Println(message.Message)
	}
}
