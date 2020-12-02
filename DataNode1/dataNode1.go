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
	port = ":50051"
)

var address string
var ipServer = make(map[int]string)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedEstructuraCentralizadaServer
}

var extBookInfo = make(map[string]string)
var totalPartBook = make(map[string]int32)
var queue []string

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

func manejarPropuesta(total int, origen int, nombre string) {
	s1, s2, s3 := generarPropuesta(total, origen)
	enviarPropuesta(s1, s2, s3, nombre, total)
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

		if strings.Compare(name, "dataNode1.go") != 0 && strings.Compare(name, "Makefile") != 0 && strings.Compare(name, "makefile.win") != 0 {
			err = os.RemoveAll(filepath.Join(dir, name))
			if err != nil {
				return err
			}
		}

	}
	return nil
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
	re, _ := c.WriteLogs(ctx, &pb.Propuesta{Ext: extBookInfo[nombre], Book: nombre, ChunkSendToServer1: s1, ChunkSendToServer2: s2, ChunkSendToServer3: s3, TotalChunks: int32(numeroProceso)})
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
	conn, err := grpc.Dial(address, grpc.WithInsecure())
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

	//Escribir en el logs

	writeLogsProceso(nombre, server1, server2, server3, 4)

	distribuirChunks(server1, server2, server3, nombre, total)

}

//FUNCIONES DISTRIBUIDAS
//
//
//
//

type book struct {
	name  string
	total int
	ext   string
}
type propuesta struct {
	s1 int
	s2 int
	s3 int
}

var queueBook []book
var w sync.WaitGroup

var estadoRecurso string = "RELEASED"

var numeroMaquinaid int = 1
var receivedAllreplies bool = false
var lamportClock int = 0
var queueSolicitudes []int
var contadorReplies int = 2
var maquinaAdyacenteUno int = 2
var maquinaAdyacenteDos int = 3

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

func envioPropuestaAlosServidores(s1 int, s2 int, s3 int, nombre string, total int) bool {
	c1 := enviarPropuestaAlservidorX(s1, s2, s3, nombre, total, maquinaAdyacenteUno)
	c2 := enviarPropuestaAlservidorX(s1, s2, s3, nombre, total, maquinaAdyacenteDos)
	if c1 && c2 {
		//Ambos nodos aceptaron la propuesta
		return true
	}
	return false
}

func enviarPropuestaDistribuida(s1 int, s2 int, s3 int, nombre string, total int, intentos int) (int, int, int) {
	if intentos == 0 {
		return -1, -1, -1
	}
	if intentos < 3 {
		s1, s2, s3 = generarPropuesta(total, numeroMaquinaid)
	}
	res := envioPropuestaAlosServidores(s1, s2, s3, nombre, total)
	if res {
		log.Printf("Propuesta aceptada por todos los nodos en el intento %v Propuesta: %v %v %v", intentos, s1, s2, s3)
		return s1, s2, s3
	}
	return enviarPropuestaDistribuida(s1, s2, s3, nombre, total, intentos-1)
}

func socilitarPermisosAServidores(destino int, reloj int) string {
	conn, err := grpc.Dial(ipServer[destino], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Printf("Enviando mensajes para autorizar Maquina:%v reloj : %v", numeroMaquinaid, reloj)
	re, _ := c.DarPermiso(ctx, &pb.Solicitud{Maquina: int32(numeroMaquinaid), RelojLamport: int32(reloj)})
	return re.GetMsg()
}
func maxValue(x int, y int) int {
	if x > y {
		return x
	}
	return y

}

func responderSolicitud(destino int) string {
	conn, err := grpc.Dial(ipServer[destino], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	re, _ := c.AceptarSolicitud(ctx, &pb.Solicitud{RelojLamport: int32(lamportClock), Maquina: int32(numeroMaquinaid)})
	return re.GetMsg()
}

func (s *server) AceptarSolicitud(ctx context.Context, in *pb.Solicitud) (*pb.Mensaje, error) {
	//Actualizamos el reloj

	log.Printf("one reply llego del servidor %v", in.GetMaquina())
	relojComing := int(in.GetRelojLamport())
	lamportClock = maxValue(lamportClock, relojComing) + 1

	if estadoRecurso == "WANTED" {
		contadorReplies = contadorReplies - 1
		if contadorReplies == 0 {
			receivedAllreplies = true
		}
	}

	return &pb.Mensaje{Msg: "KK"}, nil

}

func (s *server) DarPermiso(ctx context.Context, in *pb.Solicitud) (*pb.Mensaje, error) {

	relojComing := int(in.GetRelojLamport())
	procesoNumber := int(in.GetMaquina())
	log.Printf("T1: %v T2: %v Maquina  %v", lamportClock, relojComing, in.GetMaquina())
	if estadoRecurso == "HELD" || (estadoRecurso == "WANTED" && (lamportClock < relojComing || lamportClock == relojComing)) {
		if lamportClock == relojComing && estadoRecurso == "WANTED" {
			if numeroMaquinaid < procesoNumber {
				lamportClock = maxValue(lamportClock, relojComing) + 1
				log.Printf("OK -> Servidor %v ", procesoNumber)
				go responderSolicitud(procesoNumber)
				return &pb.Mensaje{Msg: "OK"}, nil
			}
		}
	} else {
		lamportClock = maxValue(lamportClock, relojComing) + 1
		log.Printf("OK -> Servidor %v", procesoNumber)
		go responderSolicitud(procesoNumber)
		return &pb.Mensaje{Msg: "OK"}, nil
	}
	//Lo encolo
	log.Printf("Lo encolo")
	queueSolicitudes = append(queueSolicitudes, procesoNumber)
	lamportClock = maxValue(lamportClock, relojComing) + 1
	return &pb.Mensaje{Msg: "NO"}, nil
}

func solicitarAcceso(reloj int) {
	r1 := socilitarPermisosAServidores(maquinaAdyacenteUno, reloj)
	r2 := socilitarPermisosAServidores(maquinaAdyacenteDos, reloj)
	log.Printf("Acceso respuesta M1: %v M2: %v", r1, r2)
}

func entrarZonaCritica(t book, p propuesta) {
	estadoRecurso = "HELD"
	log.Printf("Writting bookInfo.log proposal: %v %v %v  Book:  %v", p.s1, p.s2, p.s3, t.name)
	writeLogspp(t.name, int32(p.s1), int32(p.s2), int32(p.s3), numeroMaquinaid)

}

func liberarZonaCritica() {
	time.Sleep(100 * time.Millisecond)
	estadoRecurso = "RELEASED"
	replyQueue()
}

func replyQueue() {
	for i := 0; i < len(queueSolicitudes); i++ {
		//send reply
		log.Printf("ALL REPLY OK-> Servidor %v", queueSolicitudes[i])
		responderSolicitud(queueSolicitudes[i])
	}
	queueSolicitudes = make([]int, 0)
}

func ricartAgrawala(t book, relojLamport int, p propuesta) {
	estadoRecurso = "WANTED"
	contadorReplies = 2
	receivedAllreplies = false

	solicitarAcceso(relojLamport)
	for !receivedAllreplies {
	}
	log.Printf("Entrando a la zona critica")
	entrarZonaCritica(t, p)

	liberarZonaCritica()
}

func manejarPropuestaDistribuida(t book) (int, int, int) {
	s1, s2, s3 := generarPropuesta(t.total, numeroMaquinaid)
	//Enviamos la propuesta
	s1, s2, s3 = enviarPropuestaDistribuida(s1, s2, s3, t.name, t.total, 3)
	if s1 != -1 {
		return s1, s2, s3
	}
	return -1, -1, -1

}

func random99() bool {
	rand.Seed(time.Now().UnixNano())
	numeroRandom := rand.Intn(100)
	if numeroRandom < 99 {
		return true
	}
	return false

}

func (s *server) EnviarPropuesta(ctx context.Context, in *pb.Propuesta) (*pb.Respuesta, error) {
	r99 := random99()
	if r99 {
		return &pb.Respuesta{Mensaje: "Propuesta Aceptada"}, nil
	}
	return &pb.Respuesta{Mensaje: "Propuesta Rechazada"}, nil

}

func procesarCola(wg *sync.WaitGroup) {
	if len(queueBook) > 0 {
		//Obtenemos el primer valor

		book1 := queueBook[0]
		s1, s2, s3 := manejarPropuestaDistribuida(book1)

		//Enviar la Propuesta al nodo madre
		writeLogsProceso(book1.name, int32(s1), int32(s2), int32(s3), 4)
		suma := s1 + s2 + s3

		distribuirChunks(int32(s1), int32(s2), int32(s3), book1.name, suma)

		if len(queueBook) == 0 {
			queueBook = make([]book, 0)
		} else {
			queueBook = queueBook[1:]
		}

		//El algoritsmo distribuidos no funcionan en las maquinas virtuales, pero si localmente .
		/*if s1 != -1 {
			prop := propuesta{s1: s1, s2: s2, s3: s3}
			lamportClock = lamportClock + 1
			ricartAgrawala(book1, lamportClock, prop)

		} else {
			log.Printf("Hubo un error en el envio de la propuesta")
		}

		*/

	}
	wg.Done()
}

func dispatcher(wg *sync.WaitGroup) {
	r1 := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-r1.C:
			wg.Add(1)
			procesarCola(wg)
			wg.Wait()
		}
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

		if verificarSubida(nombre) {
			exito = true
			b := book{name: nombre, total: int(total), ext: ext}
			queueBook = append(queueBook, b)
		}
	}

	mensaje = "ChunkRecibido"
	if exito {
		mensaje = "Recibido"
	}
	return &pb.UploadStatus{Mensaje: mensaje, Code: pb.UploadStatusCode_Ok}, nil
}

func main() {

	argsWithoutProg := os.Args[1:]
	address = argsWithoutProg[3]
	ipServer[1] = argsWithoutProg[0]
	ipServer[2] = argsWithoutProg[1]
	ipServer[3] = argsWithoutProg[2]
	go dispatcher(&w)

	removeContents("../DataNode1")
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
