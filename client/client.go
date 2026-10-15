package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "RTVMandatoryActivity3/grpc"

	"math/rand"
)

func main() {
	id := rand.Int31()
	var lamport int32 = 0
	join := true
	leave := false
	message := ""

	lamportCh := make(chan int32)

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
		Lamport: lamport,
		Leave:   leave,
		Join:    join,
		Message: message,
	}

	stream.Send(&joinMessage)

	go PrintReceivedMessages(stream, lamportCh)

	join = false

	var input string
	scanner := bufio.NewScanner(os.Stdin)

	for {
		scanner.Scan()
		input = scanner.Text()

		// leave check
		if strings.ToLower(input) == "leave" {
			leave = true
		}

		lamport = max(<-lamportCh, lamport)

		// construct message
		messageConstruct := pb.ClientMessage{
			Id:      id,
			Lamport: lamport + 1,
			Leave:   leave,
			Join:    join,
			Message: input,
		}

		lamport = lamport + 1

		// send message
		stream.Send(&messageConstruct)

		// If leave true, break
		if leave {
			stream.CloseSend()
			break
		}
	}
}

func PrintReceivedMessages(stream pb.ChitChat_ChatClient, lamportCh chan int32) {
	for {
		message, err := stream.Recv()

		if err != nil {
			log.Fatalf("Error in received message! Server could be shut down.")
		}

		lamportCh <- max(message.Lamport, <-lamportCh) + 1

		fmt.Println(message.Message)
	}
}
