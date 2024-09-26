package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

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
	_, err := p.raw.WriteString(msg)
	if err != nil {
		return err
	}

	err = p.raw.WriteByte('\n')

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
	fmt.Printf("ping")
	s, err := c.raw.ReadString('\n')
	s = strings.TrimSuffix(s, "\n")

	return s, err
}

func main() {
	p, _ := NewProducer("127.0.0.1:8085", "test")
	err := p.Send("a")
	if err != nil {
		fmt.Printf("%%", err)

	}
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
