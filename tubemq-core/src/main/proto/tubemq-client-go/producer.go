package main

import (
	"bytes"
	"log"
	"net"
	"time"
	"tubemq-client-go/MasterService"

	"google.golang.org/protobuf/proto"
)

func main() {
	Connect()
}

func Connect() {
	var sec = 30 * time.Second
	cnx, err := net.DialTimeout("tcp", "192.168.1.4:8000", sec)
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	buf.Write([]byte{'h'})

	// RegisterRequestP2M

	var checkSum int64 = -1
	var clientId string = "192.168.1.4-lan"
	var hostName string = "192.168.1.4"

	var message = &MasterService.RegisterRequestP2M{
		ClientId:       &clientId,
		BrokerCheckSum: &checkSum,
		HostName:       &hostName,
	}

	// serialized, err := proto.Marshal(*message)
	// if err != nil {
	// 	panic(err)
	// }
	serialized := marshalA(*message)

	i, err2 := cnx.Write(serialized)
	if err2 != nil {
		panic(err2)
	}
	log.Println("i:", i)

	defer cnx.Close()
	log.Println(cnx == nil)
	// defer close(cnx)
}

func marshalA(message proto.Message) []byte {
	serialized, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return serialized
}
