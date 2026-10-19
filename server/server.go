package main

import (
	pb "RTVMandatoryActivity3/grpc"
	"bufio"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedChitChatServer
	lamport int32
	clients map[int32]chan *pb.ServerMessage
	mu      sync.Mutex
}

func (s *Server) sendTick() int32 {
	s.mu.Lock()
	s.lamport++
	returnValue := s.lamport
	s.mu.Unlock()

	return returnValue
}

func (s *Server) mergeAndTick(message *pb.ClientMessage) int32 {
	s.mu.Lock()
	s.lamport = max(s.lamport, message.Lamport) + 1
	returnValue := s.lamport
	s.mu.Unlock()

	return returnValue
}

func (s *Server) Chat(stream pb.ChitChat_ChatServer) error {
	firstMessage, err := stream.Recv()
	if err != nil {
		//log.Fatalf("Error upon receiving a message")
		return err
	}

	if !firstMessage.Join {
		//return status.Error("")
	}

	t := s.mergeAndTick(firstMessage)

	outMessageCh := make(chan *pb.ServerMessage, 10)

	go s.SendToClients(stream, outMessageCh)

	s.mu.Lock()
	s.clients[firstMessage.Id] = outMessageCh
	s.mu.Unlock()

	var loggedMessage string = "Participant " + strconv.Itoa(int(firstMessage.Id)) + " joined Chit Chat at logical time: " + strconv.Itoa(int(t))

	log.Println(loggedMessage)

	joinBroadcast := pb.ServerMessage{
		OriginalSenderId: firstMessage.Id,
		Lamport:          s.sendTick(),
		Message:          loggedMessage,
	}

	s.mu.Lock()
	for _, channel := range s.clients {
		channel <- &joinBroadcast
	}
	s.mu.Unlock()

	for {
		clientMessage, err := stream.Recv()

		if err != nil {
			log.Printf("Message from participant %d has caused an error! \n", firstMessage.Id)
			break
		}

		t := s.mergeAndTick(clientMessage)

		if clientMessage.Leave {
			break
		}

		messageLog := "Participant " + strconv.Itoa(int(firstMessage.Id)) + " sent a message at logical time: " + strconv.Itoa(int(t))
		messageContent := "Participant: " + strconv.Itoa(int(firstMessage.Id)) + " " + clientMessage.Message

		log.Println(messageLog)

		serverMessage := pb.ServerMessage{
			OriginalSenderId: firstMessage.Id,
			Lamport:          s.sendTick(),
			Message:          messageContent,
		}

		s.mu.Lock()
		for _, channel := range s.clients {
			channel <- &serverMessage
		}
		s.mu.Unlock()
	}

	s.mu.Lock()
	delete(s.clients, firstMessage.Id)
	close(outMessageCh)
	leaveString := "Participant " + strconv.Itoa(int(firstMessage.Id)) + " left Chit Chat at logical time: " + strconv.Itoa(int(s.lamport))
	s.mu.Unlock()
	log.Println(leaveString)

	leaveMessage := pb.ServerMessage{
		OriginalSenderId: firstMessage.Id,
		Lamport:          s.sendTick(),
		Message:          leaveString,
	}

	s.mu.Lock()
	for _, channel := range s.clients {
		channel <- &leaveMessage
	}
	s.mu.Unlock()

	return nil
}

func (s *Server) SendToClients(stream pb.ChitChat_ChatServer, ch chan *pb.ServerMessage) {
	for msg := range ch {
		err := stream.Send(msg)

		if err != nil {
			log.Println("Could not send message!")
		}
	}
}

func main() {
	server := Server{
		lamport: 0,
		clients: make(map[int32]chan *pb.ServerMessage),
	}

	listener, _ := net.Listen("tcp", "localhost:5050")

	grpcServer := grpc.NewServer()

	pb.RegisterChitChatServer(grpcServer, &server)

	log.Println("Server has started at logical time: 0")

	go grpcServer.Serve(listener)

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		if strings.ToLower(scanner.Text()) == "exit" {
			break
		}
	}

	grpcServer.Stop()

	server.mu.Lock()
	log.Println("Server has shutdown at logical time: " + strconv.Itoa(int(server.lamport)))
	server.mu.Unlock()
}
