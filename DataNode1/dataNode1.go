package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "google.golang.org/Tarea2SD/Client/Servicio"
	"google.golang.org/grpc"
)

const (
	port    = ":50051"
	address = "localhost:50055"
)

var ipServer = make(map[int]string)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedEstructuraCentralizadaServer
}

func crearCarpeta(nombreCarpeta string) bool {
	if _, err := os.Stat(nombreCarpeta); os.IsNotExist(err) {
		err := os.Mkdir(nombreCarpeta, 0755)
		if err != nil {
			log.Fatal(err)
			return false
		}
	}
	return true
}
func crearArchivo(path string) bool {
	filename := path
	_, err1 := os.Create(filename)
	if err1 != nil {
		fmt.Println(err1)
		return false
	}
	return true
}

func escribirChunk(filename string, chunk []byte) {
	ioutil.WriteFile(filename, chunk, os.ModeAppend)
}

var extBookInfo = make(map[string]string)
var totalPartBook = make(map[string]int32)
var queue []string

func verificarSubida(nameBook string) bool {
	totalpart := totalPartBook[nameBook]
	for i := 0; i < int(totalpart); i++ {
		if _, err := os.Stat(nameBook + "/" + strconv.Itoa(int(i))); os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func chooseRandom() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(3)
}

func distribuirInicial(total int) (int, int, int, int) {
	if total == 2 {
		return 1, 1, 0, total - 2
	}
	return 1, 1, 1, total - 3
}

func updateValor(Server1 int, Server2 int, Server3 int) (int, int, int) {
	random := chooseRandom()
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

func distribuirRandom(total int) (int, int, int) {
	Server1, Server2, Server3, total := distribuirInicial(total)
	for total > 0 {
		Server1, Server2, Server3 = updateValor(Server1, Server2, Server3)
		total = total - 1
	}
	return Server1, Server2, Server3
}

func distribuirUnChunk(origen int) (int, int, int) {
	var (
		Server1 int
		Server2 int
		Server3 int
	)
	Server1 = 0
	Server2 = 0
	Server3 = 0
	//Si solo se genero una parte se distribuye al origen
	if origen == 1 {
		Server1 = 1
	}
	if origen == 2 {
		Server2 = 1
	}
	if origen == 3 {
		Server3 = 1
	}
	return Server1, Server2, Server3

}

func generarPropuesta(total int, origen int) (int, int, int) {
	if total == 1 {
		return distribuirUnChunk(origen)
	}
	return distribuirRandom(total)
}

func eliminarChunk(path string) {
	e := os.Remove(path)
	if e != nil {
		log.Fatal(e)
	}
}

func enviarChunk(path string, numeroMaquina int, total int, numeroChunk int32, nombre string) {

	conn, err := grpc.Dial(ipServer[numeroMaquina], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	fi, _ := os.Stat(path)
	file, _ := os.Open(path)

	size := fi.Size()
	partBuffer := make([]byte, size)
	file.Read(partBuffer)

	c.EnviarChunk(ctx, &pb.ChunkSendToServer{Contenido: partBuffer, NumeroChunk: int32(numeroChunk), Nombre: nombre, TotalChunks: int32(total)})

}

func distribuirChunks(s1 int32, s2 int32, s3 int32, nombre string, total int) {
	var i int32
	totalf := s1 + s2 + s3
	path := nombre + "/"
	corte := s1 + s2
	for i = s1; i < totalf; i++ {
		if i < corte && s2 > 0 {
			enviarChunk(path+fmt.Sprint(i), 2, total, i, nombre)
			eliminarChunk(path + fmt.Sprint(i))
		} else if s3 > 0 {
			enviarChunk(path+fmt.Sprint(i), 3, total, i, nombre)
			eliminarChunk(path + fmt.Sprint(i))
		}
	}
}

func enviarPropuesta(s1 int, s2 int, s3 int, nombre string, total int) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	re, _ := c.EnviarPropuesta(ctx,
		&pb.Propuesta{Book: nombre, ChunkSendToServer1: int32(s1), ChunkSendToServer2: int32(s2), ChunkSendToServer3: int32(s3), TotalChunks: int32(total), Ext: extBookInfo[nombre]})
	log.Println("Se envio propuesta al NameNode")
	var (
		server1 int32
		server2 int32
		server3 int32
	)

	server1, server2, server3 = re.GetChunkSendToServer1(), re.GetChunkSendToServer2(), re.GetChunkSendToServer3()
	log.Print("Se recibio propuesta " + strconv.Itoa(int(server1)) + " " + strconv.Itoa(int(server2)) + " " + strconv.Itoa(int(server3)) + " para distribuir el libro " + nombre)

	//Escribir en el logs

	re1, _ := c.PedirRecurso(ctx, &pb.Mensaje{Msg: "BookInfo.log"})
	if re1.GetMsg() == "No se puedo asignar el recurso en el tiempo maximo acordado" {

		return
	}
	numeroProceso := re1.GetMsg()
	c.WriteLogs(ctx, &pb.Propuesta{Book: nombre, ChunkSendToServer1: server1, ChunkSendToServer2: server2, ChunkSendToServer3: server3})
	c.LiberarRecurso(ctx, &pb.Mensaje{Msg: numeroProceso})

	distribuirChunks(server1, server2, server3, nombre, total)

}

func manejarPropuesta(total int, origen int, nombre string) {
	s1, s2, s3 := generarPropuesta(total, origen)
	enviarPropuesta(s1, s2, s3, nombre, total)
}

func (s *server) BajarChunk(ctx context.Context, in *pb.ChunkDes) (*pb.ChunkBook, error) {
	var (
		name  string
		parte string
	)
	name = in.GetBook()
	parte = in.GetPart()
	path := name + "/" + parte

	fi, _ := os.Stat(path)
	file, _ := os.Open(path)

	size := fi.Size()
	partBuffer := make([]byte, size)
	file.Read(partBuffer)

	return &pb.ChunkBook{Contenido: partBuffer}, nil

}

func (s *server) EnviarChunk(ctx context.Context, in *pb.ChunkSendToServer) (*pb.Mensaje, error) {

	var (
		name  string
		chunk []byte
		part  int32
	)
	name = in.GetNombre()
	chunk = in.GetContenido()
	part = in.GetNumeroChunk()
	path := name + "/" + strconv.Itoa(int(part))

	crearCarpeta(name)
	crearArchivo(path)
	escribirChunk(path, chunk)

	return &pb.Mensaje{Msg: "ok"}, nil

}

func (s *server) VerificarEstadoServidor(ctx context.Context, in *pb.Mensaje) (*pb.Mensaje, error) {

	return &pb.Mensaje{Msg: "Hello"}, nil

}

func (s *server) Subir(ctx context.Context, in *pb.Chunk) (*pb.UploadStatus, error) {
	var (
		chunk   []byte
		total   int32
		part    int32
		nombre  string
		ext     string
		exito   bool
		mensaje string
	)
	chunk = in.GetContenido()
	total = in.GetTotalChunks()
	part = in.GetNumeroChunk()
	nombre = in.GetNombre()
	ext = in.GetExt()
	exito = false

	path := nombre + "/" + strconv.Itoa(int(part))

	crearCarpeta(nombre)
	crearArchivo(path)
	escribirChunk(path, chunk)

	if part == (total - 1) {

		extBookInfo[nombre] = ext
		totalPartBook[nombre] = total

		queue = append(queue, nombre)

		if verificarSubida(nombre) {
			exito = true
			go manejarPropuesta(int(total), 1, nombre)
		}
	}

	mensaje = "ChunkRecibido"
	if exito {
		mensaje = "Recibido"
	}
	return &pb.UploadStatus{Mensaje: mensaje, Code: pb.UploadStatusCode_Ok}, nil
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {

		if strings.Compare(name, "dataNode1.go") != 0 && strings.Compare(name, "Makefile") != 0 {
			err = os.RemoveAll(filepath.Join(dir, name))
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func main() {
	ipServer[1] = "localhost:50051"
	ipServer[2] = "localhost:50052"
	ipServer[3] = "localhost:50053"

	removeContents("../DataNode1")
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
