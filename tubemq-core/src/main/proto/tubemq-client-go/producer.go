package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	i := -8391426
	log.Println("i:", i)
	// os.Exit(-1)

	var sec = 30 * time.Second
	cnx, err := net.DialTimeout("tcp", "192.168.1.5:8000", sec)
	if err != nil {
		panic(err)
	}

	go func() {
		data := make([]byte, 1024)
		for {
			dataLen, errTmp := cnx.Read(data)
			if errTmp != nil {
				log.Println("errTmp:", errTmp)
			}
			if dataLen > 0 {
				log.Println("have.data:", dataLen)
			}
		}

	}()

	// RegisterRequestP2M

	// var checkSum int64 = -1
	// var clientId string = "192.168.1.4-lan"
	// var hostName string = "192.168.1.4"
	// var message = &MasterService.RegisterRequestP2M{
	// 	ClientId:       &clientId,
	// 	BrokerCheckSum: &checkSum,
	// 	HostName:       &hostName,
	// }

	log.Println("BaseCommand():", BaseCommand())
	serialized, err := proto.Marshal(BaseCommand())
	if err != nil {
		panic(err)
	}

	var cbuf bytes.Buffer

	// -2
	// -12
	// 127
	// -1

	s1 := make([]byte, 4)
	s1[0] = 0xff
	s1[1] = 0x7f
	s1[2] = 0xf4
	s1[3] = 0xfe

	// buf := bytes.NewBuffer(s1)
	// // 数字转 []byte, 网络字节序为大端字节序
	// binary.Write(buf, binary.BigEndian, i)

	cbuf.Write(s1)

	serialNo := make([]byte, 4)
	// serialNo = append(serialNo, []byte{uint8(257 >> 8)})
	serialNo[0] = 0
	serialNo[1] = 0
	serialNo[2] = 0
	serialNo[3] = 1

	cbuf.Write(serialNo)

	listSize := make([]byte, 4)
	listSize[0] = 0
	listSize[1] = 0
	listSize[2] = 0
	listSize[3] = 1

	cbuf.Write(listSize)

	// log.Println(len(serialized))

	dataLenTmp := len(serialized)

	s2 := make([]byte, 4)
	buf := bytes.NewBuffer(s2)
	// 数字转 []byte, 网络字节序为大端字节序
	binary.Write(buf, binary.BigEndian, dataLenTmp)
	fmt.Println(buf.Bytes())

	log.Println("dataLenTmp: %n , s2:%n ", dataLenTmp, len(s2))

	cbuf.Write(s2)
	cbuf.Write(serialized)

	// var buf bytes.Buffer
	// buf.WriteByte()
	// b.ResizeIfNeeded(4)
	// binary.BigEndian.PutUint32(b.WritableSlice(), n)
	// b.writerIdx += 4

	// buf.Write(bufdata, binary.BigEndian, i)

	i, err2 := cnx.Write(cbuf.Bytes())
	if err2 != nil {
		panic(err2)
	}
	log.Println("i:", i)

	defer cnx.Close()
	log.Println(cnx == nil)
	select {}
	// defer close(cnx)
}

// func main() {
// 	serialized, err := proto.Marshal(baseCommand(pb.BaseCommand_CONNECT))
// 	if err != nil {
// 		log.WithError(err).Fatal("Protobuf serialization error")
// 	}
// 	log.Println("serialized:{}", serialized)
// }

func BaseCommand() *MasterService.RegisterRequestP2M {
	var checkSum int64 = -1
	var clientId string = "192.168.1.11-lan"
	var hostName string = "192.168.1.11"

	message := &MasterService.RegisterRequestP2M{
		ClientId:       &clientId,
		BrokerCheckSum: &checkSum,
		HostName:       &hostName,
		TopicList:      []string{},
	}
	return message
}

func marshalA(message proto.Message) []byte {
	serialized, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return serialized
}
