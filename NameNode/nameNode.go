package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"time"

	pb "google.golang.org/Tarea2SD/Client/Servicio"
	"google.golang.org/grpc"
)

const (
	port = ":50055"
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedEstructuraCentralizadaServer
}

var ipServer = make(map[int]string)

func serverIsOn(number int) bool {
	log.Printf("Conectar al server %v", number)
	conn, err := grpc.Dial(ipServer[number], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		log.Printf("No se pudo conectar al server %v", number)
		return false
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	re, error1 := c.VerificarEstadoServidor(ctx, &pb.Mensaje{Msg: "Hello"})
	defer cancel()
	if re.GetMsg() != "Hello" || error1 != nil {
		log.Printf("No se pudo conectar al server %v", number)
		return false
	}
	return true

}
func verificarEstadoSv(nserver int) bool {
	if serverIsOn(nserver) {
		return true
	}
	return false
}

func analizarPropuesta(s1 int, s2 int, s3 int) bool {
	if s1 > 0 {
		if !verificarEstadoSv(1) {
			return false
		}
	}

	if s2 > 0 {
		if !verificarEstadoSv(2) {
			return false
		}
	}

	if s3 > 0 {
		if !verificarEstadoSv(3) {
			return false
		}
	}
	return true
}

func chooseRandomServer(server [3]bool) int {
	for {
		random := rand.Intn(3)
		if server[random] {
			return random
		}
	}
}

func updateValor(Server1 int, Server2 int, Server3 int, server [3]bool) (int, int, int) {
	random := chooseRandomServer(server)

	if random == 0 {
		Server1++
	}
	if random == 1 {
		Server2++

	}
	if random == 2 {
		Server3++
	}
	return Server1, Server2, Server3
}

func distribuirRandom(total int, server [3]bool) (int, int, int) {
	var (
		Server1 = 0
		Server2 = 0
		Server3 = 0
	)

	for total > 0 {
		Server1, Server2, Server3 = updateValor(Server1, Server2, Server3, server)
		total = total - 1
	}
	return Server1, Server2, Server3
}

func generarNuevaPropuesta(total int) (int, int, int) {
	//Sv estan online?
	server := [3]bool{false, false, false}

	if verificarEstadoSv(1) {
		server[0] = true
	}
	if verificarEstadoSv(2) {
		server[1] = true
	}
	if verificarEstadoSv(3) {
		server[2] = true
	}
	return distribuirRandom(total, server)

}

func (s *server) EnviarPropuesta(ctx context.Context, in *pb.Propuesta) (*pb.Respuesta, error) {
	var (
		s1    int
		s2    int
		s3    int
		total int
	)
	s1 = int(in.GetChunkSendToServer1())
	s2 = int(in.GetChunkSendToServer2())
	s3 = int(in.GetChunkSendToServer3())
	total = int(in.GetTotalChunks())
	if analizarPropuesta(s1, s2, s3) {
		log.Printf("Propuesta S1: %v S2: %v S3: %v", s1, s2, s3)
		return &pb.Respuesta{Mensaje: "Ok"}, nil
	}
	s1, s2, s3 = generarNuevaPropuesta(total)
	log.Printf("Nueva Propuesta S1: %v S2: %v S3: %v", s1, s2, s3)

	return &pb.Respuesta{Mensaje: "Ok"}, nil
}

func main() {
	ipServer[1] = "localhost:50051"
	ipServer[2] = "localhost:50052"
	ipServer[3] = "localhost:50053"

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterEstructuraCentralizadaServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
