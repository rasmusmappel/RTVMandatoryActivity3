package main

import (
	pb "RTVMandatoryActivity3/grpc"
	"log"
	"net"
	"strconv"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedChitChatServer
	lamport int32
	clients map[int32]chan *pb.ServerMessage
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

	s.lamport = max(s.lamport, firstMessage.Lamport) + 1

	outMessageCh := make(chan *pb.ServerMessage, 10)

	s.clients[firstMessage.Id] = outMessageCh

	var loggedMessage string = "Participant " + strconv.Itoa(int(firstMessage.Id)) + " joined Chit Chat at logical time: " + strconv.Itoa(int(s.lamport))

	log.Println(loggedMessage)

	s.lamport++

	joinBroadcast := pb.ServerMessage{
		OriginalSenderId: firstMessage.Id,
		Lamport:          s.lamport,
		Message:          loggedMessage,
	}

	for _, channel := range s.clients {
		channel <- &joinBroadcast
	}

	for {
		message, err := stream.Recv()

		if err != nil {
			log.Fatalf("Message from participant %d has caused an error!", firstMessage.Id)
		}

	}

	return nil
}

func main() {
	server := Server{
		lamport: 0,
		clients: make(map[int32]chan *pb.ServerMessage),
	}

	listener, _ := net.Listen("tcp", "localhost:5050")

	grpcServer := grpc.NewServer()

	pb.RegisterChitChatServer(grpcServer, &server)

	grpcServer.Serve(listener)
}
