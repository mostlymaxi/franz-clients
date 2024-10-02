package main

import (
	"bufio"
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

	_, err = buf_conn.Write([]byte("0\n"))
	_, err = buf_conn.Write([]byte(topic))
	_, err = buf_conn.Write([]byte("\n"))

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
	fmt.Printf("%s", msg)
	_, err := p.raw.Write([]byte(msg))
	_, err = p.raw.Write([]byte("\n"))

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

	_, err = buf_conn.Write([]byte("1\n"))
	_, err = buf_conn.Write([]byte(topic))
	_, err = buf_conn.Write([]byte("\n"))

	if err != nil {
		return nil, err
	}

	err = buf_conn.Flush()

	if err != nil {
		return nil, err
	}

	return &Consumer{raw: buf_conn}, nil

}

// TODO: add polling
func (c *Consumer) Recv() (string, error) {
	s, err := c.raw.ReadString('\n')
	s = strings.TrimSuffix(s, "\n")

	return s, err
}

func main() {
	p, err := NewProducer("127.0.0.1:8085", "test")
	if err != nil {
		fmt.Println("%s", err)
	}
	p.Send("a")
	p.Send("b")
	p.Send("cdef")

	c, _ := NewConsumer("127.0.0.1:8085", "test")
	s, _ := c.Recv()
	fmt.Printf("%s", s)
	s, _ = c.Recv()
	fmt.Printf("%s", s)
	s, _ = c.Recv()
	fmt.Printf("%s", s)

}
