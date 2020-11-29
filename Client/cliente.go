package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "google.golang.org/Tarea2SD/Client/Servicio"

	"google.golang.org/grpc"
)

func uploadFile(ctx context.Context, c pb.EstructuraCentralizadaClient, f string, name string, etx string) (stats pb.UploadStatus, err error) {
	fileToBeChunked := f
	file, err := os.Open(fileToBeChunked)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	const fileChunk = 250000 //250KB
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	enviado := false

	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)
		file.Read(partBuffer)
		re, _ := c.Subir(ctx, &pb.Chunk{Contenido: partBuffer, TotalChunks: int32(totalPartsNum), NumeroChunk: int32(i), Nombre: name, Ext: etx})
		mensaje := re.GetMensaje()

		if mensaje == "Recibido" {
			log.Printf("Libro enviado correctamente")
			enviado = true
		}

	}
	if !enviado {
		log.Printf("No se puedo enviar el libro al Servidor")
	}
	return

}

func bajarChunk(dir string, name string, parte string) []byte {
	conn, err := grpc.Dial(dir, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	re, _ := c.BajarChunk(ctx, &pb.ChunkDes{Book: name, Part: parte})
	return re.GetContenido()

}

func bajarArchivo(name string) {
	conn, err := grpc.Dial(address4, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	re, _ := c.BajarArchivo(ctx, &pb.BookToDownload{Book: name})

	listaDirreciones := re.GetChunkList()
	ext := re.GetExt()
	_, err = os.Create(name + "." + ext)
	file, err := os.OpenFile(name+"."+ext, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for i := 0; i < len(listaDirreciones); i++ {
		arrayString := strings.Fields(listaDirreciones[i])

		parte := strings.Split(arrayString[0], "_")[1]

		dirrecion := arrayString[1]
		datos := bajarChunk(dirrecion, name, parte)
		file.Write(datos)
		file.Sync()

	}

	log.Printf("Archivo bajado con exito")
}
func verLibrosDisponibles() []string {
	conn, err := grpc.Dial(address4, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	re, _ := c.ObtenerLibrosDisponibles(ctx, &pb.Mensaje{Msg: "Hey"})
	return re.GetBook()

}

func uploadFileRandom(f string, name string, etx string) {
	conexiones := [3]string{address1, address2, address3}

	rand.Seed(time.Now().UnixNano())
	numeroRandom := rand.Intn(3)
	elegido := conexiones[0]
	nServidor := numeroRandom + 1
	fmt.Println("Enviando el libro "+name+" al Servidor", nServidor)
	conn, err := grpc.Dial(elegido, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEstructuraCentralizadaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	uploadFile(ctx, c, f, name, etx)
}

func rutinaUploadFiles(folder string, dontInclude string) {
	var files []string
	root := folder
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		bookWithExt := strings.Split(file, "/")[1]
		if bookWithExt != dontInclude {

			ext := strings.Split(bookWithExt, ".")[1]
			bookName := strings.Split(bookWithExt, ".")[0]

			path := folder + "/" + bookWithExt
			go uploadFileRandom(path, bookName, ext)
		}
	}

}

var address1 string
var address2 string
var address3 string
var address4 string

func main() {

	argsWithoutProg := os.Args[1:]

	address1 = argsWithoutProg[0]
	address2 = argsWithoutProg[1]
	address3 = argsWithoutProg[2]
	address4 = argsWithoutProg[3]

	for true {

		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Bienvenido a la simulacion de la Biblioteca \n")
		fmt.Print("Ingrese 1 o 2 para realizar las siguientes tareas\n")
		fmt.Print("1) Enviar libros automaticamente\n")
		fmt.Print("2) Descargar un libro \n")
		text, _ := reader.ReadString('\n')

		if text == "1\n" {
			rutinaUploadFiles("./Book", "Book")
		}

		if text == "2\n" {
			log.Println("Elija un libro a descargar en la siguiente lista: ")
			libros := verLibrosDisponibles()

			for i := 0; i < len(libros); i++ {
				fmt.Println(strconv.Itoa(i) + "." + libros[i])

			}
			reader := bufio.NewReader(os.Stdin)
			text, _ := reader.ReadString('\n')
			text = text[:len(text)-1]

			indice, _ := strconv.Atoi(text)

			fmt.Println(indice)
			libroElegido := libros[indice]
			fmt.Println("Libro elegido ", libroElegido)
			bajarArchivo(libroElegido)

		}

		//bajarArchivo("Frankenstein-Mary_Shelley")
	}

}
