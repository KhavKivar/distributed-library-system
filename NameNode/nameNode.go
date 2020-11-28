package main

import (
	"bufio"
	"context"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
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
var extBookInfo = make(map[string]string)

func serverIsOn(number int) bool {
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
		rand.Seed(time.Now().UnixNano())
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

func writeLogs(name string, s1 int, s2 int, s3 int) {
	f, err := os.OpenFile("BookInfo.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(name + "    " + strconv.Itoa(s1+s2+s3) + "\n"); err != nil {
		log.Println(err)
	}

	total := s1 + s2 + s3

	for i := 0; i < total; i++ {
		if s1 > 0 {
			f.WriteString("parte_" + strconv.Itoa(i) + "    " + ipServer[1] + "\n")
			s1--
			continue
		}
		if s2 > 0 {
			f.WriteString("parte_" + strconv.Itoa(i) + "    " + ipServer[2] + "\n")
			s2--
			continue
		}
		if s3 > 0 {
			f.WriteString("parte_" + strconv.Itoa(i) + "    " + ipServer[3] + "\n")
			s3--
			continue
		}
	}

}
func (s *server) BajarArchivo(ctx context.Context, in *pb.BookToDownload) (*pb.ListChunk, error) {
	//Leer log
	file, err := os.Open("BookInfo.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	bookName := in.GetBook()

	scanner := bufio.NewScanner(file)
	var contador = 0
	var listDirecciones []string

	log.Println("se recibio un pedido a descargar el libro ", bookName)

	breakN := true
	c := false
	for scanner.Scan() && breakN {
		text := scanner.Text()
		listText := strings.Fields(text)
		if c {
			vp := strings.Split(listText[0], "_")[0]
			if vp != "parte" {
				break

			}

			listDirecciones = append(listDirecciones, text)

		}

		if listText[0] == bookName {
			log.Println(listText)

			contador++
			c = true
		}

	}

	return &pb.ListChunk{ChunkList: listDirecciones, Ext: extBookInfo[bookName]}, nil

}
func (s *server) ObtenerLibrosDisponibles(ctx context.Context, in *pb.Mensaje) (*pb.Books, error) {
	var Books []string

	for key := range extBookInfo {
		Books = append(Books, key)
	}

	return &pb.Books{Book: Books}, nil
}

func (s *server) EnviarPropuesta(ctx context.Context, in *pb.Propuesta) (*pb.Respuesta, error) {
	var (
		s1     int
		s2     int
		s3     int
		total  int
		nombre string
	)
	s1 = int(in.GetChunkSendToServer1())
	s2 = int(in.GetChunkSendToServer2())
	s3 = int(in.GetChunkSendToServer3())
	nombre = in.GetBook()
	total = int(in.GetTotalChunks())
	extBookInfo[nombre] = in.GetExt()

	if analizarPropuesta(s1, s2, s3) {
		log.Printf("Propuesta S1: %v S2: %v S3: %v", s1, s2, s3)
		writeLogs(nombre, s1, s2, s3)
		return &pb.Respuesta{Mensaje: "Ok", ChunkSendToServer1: int32(s1), ChunkSendToServer2: int32(s2), ChunkSendToServer3: int32(s3)}, nil
	}
	s1, s2, s3 = generarNuevaPropuesta(total)
	writeLogs(nombre, s1, s2, s3)
	log.Printf("Nueva Propuesta S1: %v S2: %v S3: %v", s1, s2, s3)
	return &pb.Respuesta{Mensaje: "Ok", ChunkSendToServer1: int32(s1), ChunkSendToServer2: int32(s2), ChunkSendToServer3: int32(s3)}, nil

}

func cleanLogs() {
	os.Remove("BookInfo.log")

}

func main() {
	ipServer[1] = "localhost:50051"
	ipServer[2] = "localhost:50052"
	ipServer[3] = "localhost:50053"
	cleanLogs()
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
