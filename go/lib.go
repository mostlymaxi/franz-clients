package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

// TODO: This library is unfinished! D:

// TODO: maybe find a way to close the original socket in a nice way :D
type Producer struct {
	raw *bufio.ReadWriter
}

func NewProducer(addr string, topic string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		return nil, err
	}

	buf_conn_wtr := bufio.NewWriter(conn)
	buf_conn_rdr := bufio.NewReader(conn)
	buf_conn := bufio.NewReadWriter(buf_conn_rdr, buf_conn_wtr)

	header := fmt.Sprintf("version=1,api=produce,topic=%s", topic)
	header_len := uint32(len(header))

	err = binary.Write(buf_conn.Writer, binary.BigEndian, header_len)
	_, err = buf_conn.Writer.Write([]byte(header))

	if err != nil {
		return nil, err
	}

	err = buf_conn.Flush()

	if err != nil {
		return nil, err
	}

	return &Producer{raw: buf_conn}, nil

}

func (p *Producer) Send(msg string) error {
	_, err := p.raw.Write([]byte(msg))
	_, err = p.raw.Write([]byte("\n"))

	if err != nil {
		return err
	}

	return err
}

func (p *Producer) SendUnbuffered(msg string) error {
	err := p.Send(msg)
	if err != nil {
		return err
	}

	err = p.raw.Flush()

	return err
}

type Consumer struct {
	raw *bufio.ReadWriter
}

func NewConsumer(addr string, topic string) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		return nil, err
	}

	buf_conn_wtr := bufio.NewWriter(conn)
	buf_conn_rdr := bufio.NewReader(conn)
	buf_conn := bufio.NewReadWriter(buf_conn_rdr, buf_conn_wtr)

	header := fmt.Sprintf("version=1,api=consume,topic=%s", topic)
	header_len := uint32(len(header))

	err = binary.Write(buf_conn.Writer, binary.BigEndian, header_len)
	_, err = buf_conn.Writer.Write([]byte(header))

	if err != nil {
		return nil, err
	}

	err = buf_conn.Flush()

	if err != nil {
		return nil, err
	}

	return &Consumer{raw: buf_conn}, nil

}

func (c *Consumer) Recv() (string, error) {
	s, err := c.raw.ReadString('\n')
	s = strings.TrimSuffix(s, "\n")

	return s, err
}

func main() {
	p, _ := NewProducer("127.0.0.1:8085", "test")

	p.SendUnbuffered("a")
	p.SendUnbuffered("b")
	p.SendUnbuffered("cdef")

	c, _ := NewConsumer("127.0.0.1:8085", "test")

	s, _ := c.Recv()
	fmt.Printf("%s", s)
	s, _ = c.Recv()
	fmt.Printf("%s", s)
	s, _ = c.Recv()
	fmt.Printf("%s", s)

}
