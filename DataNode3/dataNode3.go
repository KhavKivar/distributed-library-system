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
	"sync"
	"time"

	pb "google.golang.org/Tarea2SD/Client/Servicio"
	"google.golang.org/grpc"
)

const (
	port = ":50053"
)

var ipServer = make(map[int]string)
var address string
var extBookInfo = make(map[string]string)
var totalPartBook = make(map[string]int32)
var queue []string

func (s *server) EnviarPropuesta(ctx context.Context, in *pb.Propuesta) (*pb.Respuesta, error) {

	if random99() {
		return &pb.Respuesta{Mensaje: "Propuesta Aceptada"}, nil
	}
	return &pb.Respuesta{Mensaje: "Propuesta Rechazada"}, nil
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
		if strings.Compare(name, "dataNode3.go") != 0 && strings.Compare(name, "Makefile") != 0 && strings.Compare(name, "makefile.win") != 0 {
			err = os.RemoveAll(filepath.Join(dir, name))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

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

	corte := s1
	corte2 := s1 + s2
	for i = 0; i < totalf; i++ {
		if i >= corte && s2 > 0 && i < corte2 {
			enviarChunk(path+fmt.Sprint(i), 2, total, i, nombre)
			eliminarChunk(path + fmt.Sprint(i))
		} else if i < corte && s1 > 0 {
			enviarChunk(path+fmt.Sprint(i), 1, total, i, nombre)
			eliminarChunk(path + fmt.Sprint(i))
		}

	}
}

func generarPropuesta(total int, origen int) (int, int, int) {
	if total == 1 {
		return distribuirUnChunk(origen)
	}
	return distribuirRandom(total)
}

func pedirRecurso(intentos int, libro string) int {

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if intentos == 0 {
		return -1

	}
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	re, _ := c.PedirRecurso(ctx, &pb.Mensaje{Msg: "BookInfo.log"})
	if re.GetMsg() == "No se puedo asignar el recurso en el tiempo maximo acordado" || re.GetMsg() == "" {
		log.Printf("No se puedo pedir el recurso en el intento %v por el libro %v", intentos, libro)
		time.Sleep(500 * time.Millisecond)
		return pedirRecurso(intentos-1, libro)
	}
	msg, _ := strconv.Atoi(re.GetMsg())
	return msg
}

func writeLogspp(nombre string, s1 int32, s2 int32, s3 int32, numeroProceso int) bool {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	re, _ := c.WriteLogs(ctx, &pb.Propuesta{Book: nombre, ChunkSendToServer1: s1, ChunkSendToServer2: s2, ChunkSendToServer3: s3, TotalChunks: int32(numeroProceso)})

	if re.GetMsg() == "" {
		return false
	}
	return true
}

func liberarRecurso(numeroProceso string) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.LiberarRecurso(ctx, &pb.Mensaje{Msg: numeroProceso})

}

func writeLogsProceso(nombre string, s1 int32, s2 int32, s3 int32, intentos int) bool {
	time.Sleep(500 * time.Millisecond)
	if intentos == 0 {
		return false
	}

	//se pide el recurso
	estado := pedirRecurso(4, nombre)
	if estado == -1 {
		return false
	}

	log.Printf("Proceso a escribir %v", estado)
	resultado := writeLogspp(nombre, s1, s2, s3, estado)
	if !resultado {
		writeLogsProceso(nombre, s1, s2, s3, intentos-1)
	}

	liberarRecurso(strconv.Itoa(estado))
	return true
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

	writeLogsProceso(nombre, server1, server2, server3, 4)

	distribuirChunks(server1, server2, server3, nombre, total)

}

func manejarPropuesta(total int, origen int, nombre string) {
	s1, s2, s3 := generarPropuesta(total, origen)
	enviarPropuesta(s1, s2, s3, nombre, total)
}

func (s *server) VerificarEstadoServidor(ctx context.Context, in *pb.Mensaje) (*pb.Mensaje, error) {

	return &pb.Mensaje{Msg: "Hello"}, nil

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
			go manejarPropuesta(int(total), 3, nombre)
		}
	}

	mensaje = "ChunkRecibido"
	if exito {
		mensaje = "Recibido"
	}
	return &pb.UploadStatus{Mensaje: mensaje, Code: pb.UploadStatusCode_Ok}, nil
}

func random99() bool {
	rand.Seed(time.Now().UnixNano())
	numeroRandom := rand.Intn(100)
	if numeroRandom < 99 {
		return true
	}
	return false

}

/*FUNCIONES DISTRIBUIDAS
//
//
//
//













*/
var estadoRecurso string
var queueSolicitudes []int
var mutex = &sync.Mutex{}
var lamportClock int
var numeroMaquinaid int = 3
var receivedAllreplies bool
var contadorReplies int
var waitingForSend bool

type waitingSafe struct {
	sync.RWMutex
	waitingForSend bool
}

var waitingForSendStruct = &waitingSafe{}

func (waitingForSendStruct *waitingSafe) Get() bool {
	waitingForSendStruct.RLock()
	waitingForSendStruct.RUnlock()
	return waitingForSendStruct.waitingForSend
}
func (waitingForSendStruct *waitingSafe) set(x bool) {
	waitingForSendStruct.Lock()
	waitingForSendStruct.waitingForSend = x
	waitingForSendStruct.Unlock()
}

func (s *server) DarPermiso(ctx context.Context, in *pb.Solicitud) (*pb.Mensaje, error) {
	for waitingForSend {
	}

	relojComing := int(in.GetRelojLamport())
	procesoNumber := int(in.GetMaquina())

	log.Printf("T1: %v T2: %v Maquina  %v", lamportClock, relojComing, in.GetMaquina())

	if estadoRecurso == "HELD" || (estadoRecurso == "WANTED" && (lamportClock < relojComing || lamportClock == relojComing)) {
		if lamportClock == relojComing && estadoRecurso == "WANTED" {
			if numeroMaquinaid < procesoNumber {
				mutex.Lock()
				lamportClock = maxValue(lamportClock, relojComing) + 1
				mutex.Unlock()
				log.Printf("OK -> Servidor %v ", procesoNumber)
				go aceptarSolicitudAltiro(procesoNumber)
				return &pb.Mensaje{Msg: "OK"}, nil
			}
		}
	} else {
		mutex.Lock()
		lamportClock = maxValue(lamportClock, relojComing) + 1
		mutex.Unlock()
		log.Printf("OK -> Servidor %v", procesoNumber)
		go aceptarSolicitudAltiro(procesoNumber)
		return &pb.Mensaje{Msg: "OK"}, nil
	}
	//Lo encolo
	log.Printf("Lo encolo")
	lamportClock = maxValue(lamportClock, relojComing) + 1

	queueSolicitudes = append(queueSolicitudes, procesoNumber)
	return &pb.Mensaje{Msg: "NO"}, nil
}

func enviarMensajeDeAutorizacion(destino int) string {

	conn, err := grpc.Dial(ipServer[destino], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	log.Printf("Enviando mensajes para autorizar Maquina:%v reloj : %v", numeroMaquinaid, lamportClock)
	re, _ := c.DarPermiso(ctx, &pb.Solicitud{Maquina: int32(numeroMaquinaid), RelojLamport: int32(lamportClock)})
	return re.GetMsg()
}

func replyQueue() {

	for i := 0; i < len(queueSolicitudes); i++ {
		//send reply
		log.Printf("ALL REPLY OK-> Servidor %v", queueSolicitudes[i])

		aceptarSolicitudAltiro(queueSolicitudes[i])
	}
	queueSolicitudes = make([]int, 0)
}

func writeLogsDistribuido(s1 int, s2 int, s3 int, nombre string, total int) {
	/*
		mutex.Lock()
		estado := estadoRecurso
		mutex.Unlock()

			if estado == "WANTED" || estado == "HELD" {
				//esperamos a que lo libere
				for true {
					if estadoRecurso == "RELEASED" {
						break
					}
				}
			}
	*/
	mutex.Lock()
	waitingForSend = true
	estadoRecurso = "WANTED"
	lamportClock = lamportClock + 1
	mutex.Unlock()

	contadorReplies = 2

	//Difusion de los mensajes recordar el aplazamiento
	log.Printf("Libro :%v", nombre)
	enviarMensajeDeAutorizacion(1)
	enviarMensajeDeAutorizacion(2)
	waitingForSend = false

	for !receivedAllreplies {
	}
	mutex.Lock()
	estadoRecurso = "HELD"
	mutex.Unlock()
	//Escribir en elogs
	//Liberar

	log.Printf("Writting bookInfo.log proposal: %v %v %v  Book:  %v", s1, s2, s3, nombre)

	writeLogspp(nombre, int32(s1), int32(s2), int32(s3), 3)

	mutex.Lock()
	estadoRecurso = "RELEASED"
	mutex.Unlock()
	receivedAllreplies = false
	replyQueue()

}

func aceptarSolicitudAltiro(destino int) string {

	conn, err := grpc.Dial(ipServer[destino], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	re, _ := c.AceptarSolicitud(ctx, &pb.Solicitud{RelojLamport: int32(lamportClock), Maquina: int32(numeroMaquinaid)})

	return re.GetMsg()
}

func (s *server) AceptarSolicitud(ctx context.Context, in *pb.Solicitud) (*pb.Mensaje, error) {

	relojComing := int(in.GetRelojLamport())
	lamportClock = maxValue(lamportClock, relojComing) + 1
	log.Printf("one reply llego del servidor %v", in.GetMaquina())

	if estadoRecurso == "WANTED" {
		contadorReplies = contadorReplies - 1
		if contadorReplies == 0 {
			receivedAllreplies = true
		}
	}
	return &pb.Mensaje{Msg: "KK"}, nil
}

func maxValue(x int, y int) int {
	if x > y {
		return x
	}
	return y

}
func enviarPropuestaFor(s1 int, s2 int, s3 int, nombre string, total int) bool {
	c1 := enviarPropuestaAlservidorX(s1, s2, s3, nombre, total, 1)
	c2 := enviarPropuestaAlservidorX(s1, s2, s3, nombre, total, 2)
	if c1 && c2 {
		//Ambos nodos aceptaron la propuesta
		return true
	}
	return false
}

func enviarPropuestaAlservidorX(s1 int, s2 int, s3 int, nombre string, total int, nservidor int) bool {
	conn, err := grpc.Dial(ipServer[nservidor], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	re, _ := c.EnviarPropuesta(ctx,
		&pb.Propuesta{Book: nombre, ChunkSendToServer1: int32(s1), ChunkSendToServer2: int32(s2), ChunkSendToServer3: int32(s3), TotalChunks: int32(total), Ext: extBookInfo[nombre]})
	if re.GetMensaje() == "Propuesta Aceptada" {
		return true
	}
	return false
}

func enviarPropuestaDistribuida(s1 int, s2 int, s3 int, nombre string, total int) bool {
	//Lo intentamos tres veces si no gg..

	res := enviarPropuestaFor(s1, s2, s3, nombre, total)

	if res {
		log.Printf("Propuesta aceptada por todos los nodos en el intento 0 Propuesta: %v %v %v", s1, s2, s3)
		return true
	}
	//intentamos nuevamente

	s1, s2, s3 = generarPropuesta(total, 1)

	res = enviarPropuestaFor(s1, s2, s3, nombre, total)
	if res {
		log.Printf("Propuesta aceptada por todos los nodos en el intento 1 Propuesta: %v %v %v", s1, s2, s3)
		return true
	}
	//intentamos nuevamente
	s1, s2, s3 = generarPropuesta(total, 1)
	res = enviarPropuestaFor(s1, s2, s3, nombre, total)
	if res {
		log.Printf("Propuesta aceptada por todos los nodos en el intento 2  Propuesta: %v %v %v", s1, s2, s3)
		return true
	}
	return false
}

func manejarPropuestaDistribuida(total int, origen int, nombre string) {
	s1, s2, s3 := generarPropuesta(total, origen)
	if enviarPropuestaDistribuida(s1, s2, s3, nombre, total) {
		//Ricart y Agrawala
		writeLogsDistribuido(s1, s2, s3, nombre, total)
	} else {
		log.Printf("Los servidores no aceptaron la propuesta tres veces seguidas")
	}
}

func (s *server) SubirDistribuida(ctx context.Context, in *pb.Chunk) (*pb.UploadStatus, error) {
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
			go manejarPropuestaDistribuida(int(total), 3, nombre)
		}
	}

	mensaje = "ChunkRecibido"
	if exito {
		mensaje = "Recibido"
	}
	return &pb.UploadStatus{Mensaje: mensaje, Code: pb.UploadStatusCode_Ok}, nil
}

func main() {

	lamportClock = 0
	estadoRecurso = "RELEASED"
	contadorReplies = 2
	receivedAllreplies = false
	waitingForSend = false
	waitingForSendStruct.set(false)

	argsWithoutProg := os.Args[1:]
	address = argsWithoutProg[3]
	ipServer[1] = argsWithoutProg[0]
	ipServer[2] = argsWithoutProg[1]
	ipServer[3] = argsWithoutProg[2]
	waitingForSendStruct.set(false)
	removeContents("../DataNode3")

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterEstructuraCentralizadaServer(s, &server{})
	log.Printf("Server Iniciado")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
